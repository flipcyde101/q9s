import { describe, expect, it } from "vitest";
import {
  buildPgBossJobsSql,
  buildPgBossQueueStatsSql,
  buildPgBossRetryJobSql,
  buildRiverJobsSql,
  buildRiverQueueStatsSql,
  buildRiverRetryFailedBulkSql,
  parseBackendDetection,
  parsePgBossJobs,
  parsePgBossQueueStats,
  parsePostgresUrl,
  parseRiverJobs,
  parseRiverQueueStats,
  PostgresQueueBackend,
  PSQL_FIELD_SEPARATOR,
  validateIdentifier,
  validateJobId,
  validateQueueName,
} from "../src/backends/postgres.js";
import { ReadOnlyError } from "../src/types.js";

const SEP = PSQL_FIELD_SEPARATOR;

describe("Postgres URL parsing and input validation", () => {
  it("strips passwords from sanitizedUrl and isolates them for PGPASSWORD", () => {
    const parsed = parsePostgresUrl("postgres://appuser:top%23secret@db.internal:5433/prod");
    expect(parsed.password).toBe("top#secret");
    expect(parsed.username).toBe("appuser");
    expect(parsed.host).toBe("db.internal");
    expect(parsed.port).toBe(5433);
    expect(parsed.database).toBe("prod");
    expect(parsed.sanitizedUrl).not.toContain("top#secret");
    expect(parsed.sanitizedUrl).not.toContain("top%23secret");
  });

  it("rejects SQL injection attempts in schema, queue name, and job IDs", () => {
    expect(() => validateIdentifier("public; DROP TABLE river_job--")).toThrow(/Invalid SQL/);
    expect(() => validateQueueName("emails' OR '1'='1")).toThrow(/Invalid queue name/);
    expect(() => validateJobId("123 OR 1=1", "river")).toThrow(/Invalid River job ID/);
    expect(() => validateJobId("not-a-uuid", "pg-boss")).toThrow(/Invalid pg-boss job ID/);

    expect(validateJobId("90412", "river")).toBe("90412");
    expect(validateJobId("550e8400-e29b-41d4-a716-446655440000", "pg-boss")).toBe(
      "550e8400-e29b-41d4-a716-446655440000",
    );
  });
});

describe("Postgres backend auto-detection and pure SQL parsers", () => {
  it("detects River vs pg-boss tables accurately", () => {
    expect(parseBackendDetection(`public${SEP}river_job\n`)).toEqual({
      kind: "river",
      schema: "public",
    });
    expect(parseBackendDetection(`pgboss${SEP}job\n`)).toEqual({
      kind: "pg-boss",
      schema: "pgboss",
    });
    expect(() => parseBackendDetection("")).toThrow(/Could not auto-detect/);
  });

  it("parses River queue stats and failed job rows", () => {
    const statsOut = [
      ["default", "14", "2", "5", "410", "3"].join(SEP),
      ["webhooks", "8", "1", "0", "220", "11"].join(SEP),
    ].join("\n");

    const stats = parseRiverQueueStats(statsOut);
    expect(stats).toEqual([
      { name: "default", backend: "river", waiting: 14, active: 2, delayed: 5, completed: 410, failed: 3 },
      { name: "webhooks", backend: "river", waiting: 8, active: 1, delayed: 0, completed: 220, failed: 11 },
    ]);

    const errorsJson = JSON.stringify([
      { attempt: 1, error: "first timeout", trace: "line1\nline2" },
      { attempt: 2, error: "dial tcp: i/o timeout", trace: "goroutine 42\nworker.go:88" },
    ]);
    const jobsOut = [
      "901",
      "WebhookDeliverArgs",
      "discarded",
      JSON.stringify({ url: "https://partner.example/hook" }),
      errorsJson,
      "2",
      "5",
      "1795200000000",
      "1795200005000",
      "1795200010000",
    ].join(SEP);

    const jobs = parseRiverJobs(jobsOut, "webhooks");
    expect(jobs).toHaveLength(1);
    expect(jobs[0]).toMatchObject({
      id: "901",
      queue: "webhooks",
      name: "WebhookDeliverArgs",
      state: "failed",
      data: { url: "https://partner.example/hook" },
      failedReason: "dial tcp: i/o timeout",
      stacktrace: ["goroutine 42", "worker.go:88"],
      attemptsMade: 2,
      maxAttempts: 5,
    });

    expect(buildRiverQueueStatsSql("app")).toContain('"app"."river_job"');
    expect(buildRiverJobsSql("public", "webhooks", "failed", 10)).toContain("state IN ('discarded', 'cancelled')");
    expect(buildRiverRetryFailedBulkSql("public", "webhooks", 25)).toContain("FOR UPDATE SKIP LOCKED");
  });

  it("parses pg-boss queue stats and failed job rows", () => {
    const statsOut = ["billing-sync", "3", "1", "2", "800", "4"].join(SEP);
    expect(parsePgBossQueueStats(statsOut)).toEqual([
      { name: "billing-sync", backend: "pg-boss", waiting: 3, active: 1, delayed: 2, completed: 800, failed: 4 },
    ]);

    const outputJson = JSON.stringify({
      message: "HTTP 503 Service Unavailable",
      stack: "Error: HTTP 503\n    at syncBilling (worker.js:41:11)",
    });
    const jobId = "550e8400-e29b-41d4-a716-446655440000";
    const jobsOut = [
      jobId,
      "billing-sync",
      "failed",
      JSON.stringify({ invoiceId: "inv_99" }),
      outputJson,
      "3",
      "3",
      "1795200000000",
      "1795200002000",
      "1795200004000",
    ].join(SEP);

    const jobs = parsePgBossJobs(jobsOut, "billing-sync");
    expect(jobs[0]).toMatchObject({
      id: jobId,
      queue: "billing-sync",
      name: "billing-sync",
      state: "failed",
      data: { invoiceId: "inv_99" },
      failedReason: "HTTP 503 Service Unavailable",
      stacktrace: ["Error: HTTP 503", "    at syncBilling (worker.js:41:11)"],
      attemptsMade: 3,
      maxAttempts: 3,
    });

    expect(buildPgBossQueueStatsSql("pgboss")).toContain('"pgboss"."job"');
    expect(buildPgBossJobsSql("pgboss", "billing-sync", "failed", 5)).toContain("state IN ('failed', 'cancelled')");
    expect(buildPgBossRetryJobSql("pgboss", "billing-sync", jobId)).toContain(`id = '${jobId}'`);
  });
});

describe("PostgresQueueBackend orchestration and read-only safety", () => {
  it("auto-detects River, executes read queries, and blocks writes when readOnly is true", async () => {
    const executed: Array<{ sql: string; readOnly: boolean }> = [];
    const runner = (sql: string, readOnly: boolean): string => {
      executed.push({ sql, readOnly });
      if (sql.includes("information_schema.tables")) {
        return `public${SEP}river_job\n`;
      }
      if (sql.includes("GROUP BY queue")) {
        return `emails${SEP}2${SEP}1${SEP}0${SEP}90${SEP}1\n`;
      }
      if (sql.startsWith("UPDATE")) {
        return "42\n";
      }
      return "";
    };

    const roBackend = PostgresQueueBackend.connect("postgres://app:pw@localhost:5432/db", {
      readOnly: true,
      sqlRunner: runner,
    });

    expect(roBackend.kind).toBe("river");
    const queues = await roBackend.listQueues();
    expect(queues[0]?.name).toBe("emails");
    expect(executed.every((q) => q.readOnly)).toBe(true);

    await expect(roBackend.retryJob("emails", "42")).rejects.toBeInstanceOf(ReadOnlyError);
    await expect(roBackend.retryFailedBulk("emails", 10)).rejects.toBeInstanceOf(ReadOnlyError);

    const rwBackend = PostgresQueueBackend.connect("postgres://app:pw@localhost:5432/db", {
      readOnly: false,
      sqlRunner: runner,
    });
    const retryRes = await rwBackend.retryJob("emails", "42");
    expect(retryRes).toEqual({ queue: "emails", retried: 1, jobIds: ["42"] });
  });
});
