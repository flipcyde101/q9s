import net from "node:net";
import { describe, expect, it } from "vitest";
import { BullMqBackend, extractQueueNameFromKey } from "../src/backends/bullmq.js";
import {
  buildPgBossJobsSql,
  buildPgBossQueueStatsSql,
  buildPgBossRetryFailedBulkSql,
  buildPgBossRetryJobSql,
  buildRiverRetryFailedBulkSql,
  buildRiverRetryJobSql,
  parsePgBossJobs,
  parsePostgresUrl,
  PostgresQueueBackend,
  PSQL_FIELD_SEPARATOR,
} from "../src/backends/postgres.js";
import { parseCliArgs, runCli } from "../src/cli.js";
import { parseRespFrame, parseRedisUrl, RespClient, RespError, RespParser } from "../src/clients/resp.js";
import { parseSimpleYamlContexts, redactUrl } from "../src/config/contexts.js";
import { startMockRedisServer } from "../src/demo/fixture.js";
import { renderJobInspector, renderQueueTable } from "../src/render/dashboard.js";

describe("q9s whole-branch review regression fixes (C1-C5, I1-I10, M1-M7)", () => {
  it("C1 & C4 & I7 & M2 & M4: pg-boss v10 SQL omits invalid 'expired' enum, River bumps max_attempts, Postgres clears output/started_on & preserves retry_limit=0 & strips ?password=/?options=", () => {
    const statsSql = buildPgBossQueueStatsSql("pgboss");
    expect(statsSql).not.toContain("'expired'");
    expect(statsSql).toContain("state IN ('failed', 'cancelled')");

    const jobsSql = buildPgBossJobsSql("pgboss", "emails", "failed", Number.NaN);
    expect(jobsSql).not.toContain("'expired'");
    expect(jobsSql).not.toContain("NaN");

    const retryPgBossSql = buildPgBossRetryJobSql(
      "pgboss",
      "emails",
      "11111111-1111-1111-1111-111111111111",
    );
    expect(retryPgBossSql).not.toContain("'expired'");
    expect(retryPgBossSql).toContain("started_on = NULL");
    expect(retryPgBossSql).toContain("output = NULL");

    const bulkPgBossSql = buildPgBossRetryFailedBulkSql("pgboss", "emails", 50);
    expect(bulkPgBossSql).not.toContain("'expired'");
    expect(bulkPgBossSql).toContain("AND j.name = 'emails'");
    expect(bulkPgBossSql).toContain("started_on = NULL");

    const riverRetry = buildRiverRetryJobSql("public", "webhooks", "42");
    expect(riverRetry).toContain("max_attempts = CASE WHEN max_attempts <= attempt");

    const riverBulk = buildRiverRetryFailedBulkSql("public", "webhooks", Number.NaN);
    expect(riverBulk).toContain("max_attempts = CASE WHEN max_attempts <= attempt");
    expect(riverBulk).not.toContain("NaN");

    const parsedPg = parsePostgresUrl(
      "postgres://app@db.internal:5432/prod?password=supersecret&options=-c%20default_transaction_read_only=off&sslmode=require",
    );
    expect(parsedPg.password).toBe("supersecret");
    expect(parsedPg.sanitizedUrl).not.toContain("supersecret");
    expect(parsedPg.sanitizedUrl).not.toContain("options=");
    expect(parsedPg.sanitizedUrl).toContain("sslmode=require");

    const pgJobs = parsePgBossJobs(
      [
        "11111111-1111-1111-1111-111111111111",
        "emails",
        "failed",
        "{}",
        '{"message":"boom"}',
        "0",
        "0",
        "1700000000000",
        "",
        "",
      ].join(PSQL_FIELD_SEPARATOR),
      "emails",
    );
    expect(pgJobs[0]?.maxAttempts).toBe(0);
  });

  it("C2: RESP2 command timeout destroys socket and fails pending queue so late reply never corrupts subsequent commands", async () => {
    const server = net.createServer((socket) => {
      socket.on("data", () => {
        // Delay reply longer than client timeout
        setTimeout(() => {
          if (!socket.destroyed) {
            socket.write("+LATE_REPLY\r\n");
          }
        }, 120);
      });
    });

    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const addr = server.address() as net.AddressInfo;

    try {
      const client = await RespClient.connect(`redis://127.0.0.1:${addr.port}`, {
        timeoutMs: 40,
      });
      await expect(client.command("PING")).rejects.toThrow(/timed out/i);
      // Connection must be closed after timeout, not left alive with desynced pipeline
      await expect(client.command("PING")).rejects.toThrow(/closed/i);
      await client.close();
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it("C3 & I3 & I10 & M1: BullMQ preserves RespError in arrays, respects paused queue on retry, ignores :metrics phantom queues, includes paused/prioritized in waiting, and fails closed when readOnly omitted", async () => {
    // RespError preserved inside arrays
    const arrBuf = Buffer.from("*2\r\n:1\r\n-ERR boom\r\n", "utf8");
    const frame = parseRespFrame(arrBuf, 0);
    expect(Array.isArray(frame?.value)).toBe(true);
    expect((frame?.value as unknown[])[1]).toBeInstanceOf(RespError);

    // Phantom metrics queue ignored
    expect(extractQueueNameFromKey("bull:webhooks:metrics:failed", "bull", "failed")).toBeNull();
    expect(extractQueueNameFromKey("bull:webhooks:failed", "bull", "failed")).toBe("webhooks");

    const demo = await startMockRedisServer();
    try {
      const client = await RespClient.connect(demo.url);
      // Pause webhooks queue and add a paused job
      await client.command("HSET", "bull:webhooks:meta", "paused", "1");
      await client.command("RPUSH", "bull:webhooks:paused", "wh-paused-1");
      await client.command("HSET", "bull:webhooks:wh-paused-1", "name", "paused-job", "data", '{"p":1}');

      // Fail closed if readOnly omitted at runtime (JS caller)
      const defaultClosed = BullMqBackend.fromClient(client, {} as unknown as { readOnly: boolean });
      expect(defaultClosed.readOnly).toBe(true);

      const writable = BullMqBackend.fromClient(client, { readOnly: false });
      const waiting = await writable.listJobs("webhooks", "waiting", 50);
      expect(waiting.some((j) => j.id === "wh-paused-1")).toBe(true);

      // Retrying on a paused queue must push to :paused, NOT :wait
      const failedBefore = await writable.listJobs("webhooks", "failed", 1);
      expect(failedBefore.length).toBe(1);
      const jobId = failedBefore[0]!.id;
      const waitBefore = await client.lrange("bull:webhooks:wait", 0, -1);
      const retryRes = await writable.retryJob("webhooks", jobId);
      expect(retryRes.retried).toBe(1);
      const pausedAfter = await client.lrange("bull:webhooks:paused", 0, -1);
      const waitAfter = await client.lrange("bull:webhooks:wait", 0, -1);
      expect(pausedAfter).toContain(jobId);
      expect(waitAfter.length).toBe(waitBefore.length);

      await client.close();
    } finally {
      await demo.close();
    }
  });

  it("C5 & M5: YAML inline comments preserve readOnly: true, invalid booleans throw, and top-level keys after contexts reset block", () => {
    const yaml = [
      "contexts:",
      "  prod-bull:",
      "    url: rediss://default:secret@redis.internal:6379/0 # primary",
      "    backend: bullmq # queue",
      "    readOnly: true # production safety",
      "other_section:",
      "  ignored: true",
    ].join("\n");

    const entries = parseSimpleYamlContexts(yaml);
    expect(entries).toHaveLength(1);
    expect(entries[0]?.readOnly).toBe(true);
    expect(entries[0]?.backend).toBe("bullmq");
    expect(entries[0]?.url).toBe("rediss://default:secret@redis.internal:6379/0");

    expect(() =>
      parseSimpleYamlContexts("contexts:\n  bad:\n    url: redis://localhost:6379\n    readOnly: maybe\n"),
    ).toThrow(/readOnly/i);
  });

  it("I1 & I2 & I5: strict RESP integer/bounds/IPv6 parsing and full URL credential redaction on errors", () => {
    expect(() => parseRespFrame(Buffer.from(":12abc\r\n"))).toThrow();
    expect(() => parseRespFrame(Buffer.from("$5xyz\r\nhello\r\n"))).toThrow();
    expect(() => parseRespFrame(Buffer.from("*1\r\n".repeat(40)))).toThrow(/depth/i);
    const parser = new RespParser();
    expect(() => parser.push(Buffer.from("$999999999\r\n"))).toThrow();

    const ipv6 = parseRedisUrl("rediss://[::1]:6380/2");
    expect(ipv6.host).toBe("::1");
    expect(ipv6.servername).toBeUndefined();
    expect(ipv6.db).toBe(2);

    expect(() => parseRedisUrl("redis://localhost:6379/2foo")).toThrow();

    // Unparseable URL containing password with @ or ?password= must not leak secret in error
    expect(() => parseRedisUrl("redis://user:p@ss word@host:badport")).not.toThrow(/p@ss/);
    const redacted = redactUrl("postgres://u:top@secret@bad host:9999/db?password=leak2");
    expect(redacted).not.toContain("top@secret");
    expect(redacted).not.toContain("leak2");
  });

  it("I6: strips ANSI/OSC terminal escape sequences from untrusted queue names and job payloads", () => {
    const evilTable = renderQueueTable([
      {
        name: "jobs\x1b]52;c;ZXZpbA==\x07\x1b[31mFAKE",
        backend: "bullmq",
        waiting: 1,
        active: 0,
        delayed: 0,
        completed: 0,
        failed: 1,
      },
    ]);
    expect(evilTable).not.toContain("\x1b");
    expect(evilTable).not.toContain("\x07");

    const evilInspector = renderJobInspector(
      [
        {
          id: "1\x1b[2J",
          queue: "q",
          name: "job\x1b[31m",
          state: "failed",
          data: { msg: "ok\x1b]0;pwn\x07" },
          failedReason: "fail\x1b[32m[READ-ONLY]",
          stacktrace: ["Error\x1b[1m at line 1"],
          attemptsMade: 1,
        },
      ],
      "q",
      "failed",
    );
    expect(evilInspector).not.toContain("\x1b");
    expect(evilInspector).not.toContain("\x07");
  });

  it("I9: CLI rejects malformed --limit/--timeout, conflicting target selectors, and missing --queue before connecting", async () => {
    expect(() => parseCliArgs(["--demo", "--limit", "10abc"])).toThrow();
    expect(() => parseCliArgs(["--demo", "--timeout", "500ms"])).toThrow();
    expect(() => parseCliArgs(["--demo", "--limit", "-5"])).toThrow();

    const errs: string[] = [];
    const io = { stdout: () => {}, stderr: (s: string) => errs.push(s), isTty: false };

    const codeConflict = await runCli(["--demo", "redis://localhost:6379"], io);
    expect(codeConflict).toBe(2);

    const codeMissingQueue = await runCli(
      ["redis://127.0.0.1:1", "--retry", "job-1", "--allow-write"],
      io,
    );
    expect(codeMissingQueue).toBe(2);

    // Fail-closed PostgresBackend if readOnly omitted at runtime
    const pgClosed = PostgresQueueBackend.connect("postgres://localhost:5432/db", {
      kind: "river",
      schema: "public",
      sqlRunner: () => "",
    } as unknown as { readOnly: boolean; kind: "river"; schema: string; sqlRunner: () => string });
    expect(pgClosed.readOnly).toBe(true);
  });
});
