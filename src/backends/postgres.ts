import { execFileSync } from "node:child_process";
import {
  type BackendOptions,
  type JobRecord,
  type JobState,
  type QueueBackend,
  type QueueStats,
  ReadOnlyError,
  type RetryResult,
} from "../types.js";

export type PostgresQueueKind = "river" | "pg-boss";

export const PSQL_FIELD_SEPARATOR = "\x1f";

export interface ParsedPostgresUrl {
  sanitizedUrl: string;
  host: string;
  port: number;
  database: string;
  username?: string;
  password?: string;
}

export type SqlRunner = (sql: string, readOnly: boolean) => string;

export interface PostgresBackendOptions extends BackendOptions {
  kind?: PostgresQueueKind;
  psqlPath?: string;
  sqlRunner?: SqlRunner;
}

const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/;
const QUEUE_NAME_RE = /^[a-zA-Z0-9_.:/@-]{1,128}$/;
const RIVER_ID_RE = /^[0-9]{1,20}$/;
const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

export function parsePostgresUrl(rawUrl: string): ParsedPostgresUrl {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error(`Invalid Postgres URL: ${rawUrl}`);
  }

  if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
    throw new Error(
      `Unsupported Postgres protocol "${parsed.protocol}". Expected postgres:// or postgresql://`,
    );
  }

  const host = parsed.hostname || "127.0.0.1";
  const port = parsed.port ? Number.parseInt(parsed.port, 10) : 5432;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`Invalid Postgres port: ${parsed.port}`);
  }

  const username = parsed.username ? decodeURIComponent(parsed.username) : undefined;
  const password = parsed.password ? decodeURIComponent(parsed.password) : undefined;
  const database = parsed.pathname.replace(/^\/+/, "") || "postgres";

  const sanitized = new URL(parsed.toString());
  sanitized.password = "";

  return {
    sanitizedUrl: sanitized.toString(),
    host,
    port,
    database,
    username,
    password,
  };
}

export function validateIdentifier(identifier: string, label = "identifier"): string {
  if (!IDENTIFIER_RE.test(identifier)) {
    throw new Error(`Invalid SQL ${label}: "${identifier}"`);
  }
  return identifier;
}

export function validateQueueName(queue: string): string {
  if (!QUEUE_NAME_RE.test(queue)) {
    throw new Error(`Invalid queue name: "${queue}"`);
  }
  return queue;
}

export function validateJobId(jobId: string, kind: PostgresQueueKind): string {
  if (kind === "river") {
    if (!RIVER_ID_RE.test(jobId)) {
      throw new Error(`Invalid River job ID (expected numeric bigint): "${jobId}"`);
    }
    return jobId;
  }
  if (!UUID_RE.test(jobId)) {
    throw new Error(`Invalid pg-boss job ID (expected UUID): "${jobId}"`);
  }
  return jobId;
}

function sqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function safeJsonParse(raw: string | undefined): unknown {
  if (!raw || raw.trim() === "") {
    return {};
  }
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function parseIntField(raw: string | undefined): number {
  if (!raw) {
    return 0;
  }
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? n : 0;
}

function parseTimestampMs(raw: string | undefined): number | undefined {
  if (!raw || raw.trim() === "") {
    return undefined;
  }
  const asNum = Number(raw);
  if (Number.isFinite(asNum) && asNum > 0) {
    return Math.round(asNum);
  }
  const ms = Date.parse(raw);
  return Number.isFinite(ms) ? ms : undefined;
}

export function buildDetectPostgresBackendSql(schema?: string): string {
  if (schema) {
    const safeSchema = validateIdentifier(schema, "schema");
    return [
      "SELECT table_schema, table_name",
      "FROM information_schema.tables",
      `WHERE table_schema = ${sqlLiteral(safeSchema)}`,
      "  AND table_name IN ('river_job', 'job')",
      "ORDER BY table_name ASC;",
    ].join(" ");
  }

  return [
    "SELECT table_schema, table_name",
    "FROM information_schema.tables",
    "WHERE (table_name = 'river_job' AND table_schema NOT IN ('pg_catalog', 'information_schema'))",
    "   OR (table_schema = 'pgboss' AND table_name = 'job')",
    "ORDER BY CASE WHEN table_name = 'river_job' THEN 0 ELSE 1 END, table_schema ASC;",
  ].join(" ");
}

export function parseBackendDetection(output: string): { kind: PostgresQueueKind; schema: string } {
  const lines = output
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  for (const line of lines) {
    const [schema, table] = line.split(PSQL_FIELD_SEPARATOR);
    if (schema && table === "river_job") {
      return { kind: "river", schema: validateIdentifier(schema, "schema") };
    }
    if (schema && table === "job") {
      return { kind: "pg-boss", schema: validateIdentifier(schema, "schema") };
    }
  }

  throw new Error(
    "Could not auto-detect River (river_job) or pg-boss (pgboss.job) tables in the target Postgres database.",
  );
}

export function buildRiverQueueStatsSql(schema = "public"): string {
  const s = validateIdentifier(schema, "schema");
  return [
    "SELECT",
    "  queue,",
    "  COUNT(*) FILTER (WHERE state IN ('available', 'pending')) AS waiting,",
    "  COUNT(*) FILTER (WHERE state = 'running') AS active,",
    "  COUNT(*) FILTER (WHERE state IN ('scheduled', 'retryable')) AS delayed,",
    "  COUNT(*) FILTER (WHERE state = 'completed') AS completed,",
    "  COUNT(*) FILTER (WHERE state IN ('discarded', 'cancelled')) AS failed",
    `FROM "${s}"."river_job"`,
    "GROUP BY queue",
    "ORDER BY queue ASC;",
  ].join(" ");
}

export function parseRiverQueueStats(output: string): QueueStats[] {
  const lines = output
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  return lines.map((line) => {
    const cols = line.split(PSQL_FIELD_SEPARATOR);
    return {
      name: cols[0] ?? "default",
      backend: "river",
      waiting: parseIntField(cols[1]),
      active: parseIntField(cols[2]),
      delayed: parseIntField(cols[3]),
      completed: parseIntField(cols[4]),
      failed: parseIntField(cols[5]),
    };
  });
}

function riverStatesFor(state: JobState): string[] {
  switch (state) {
    case "waiting":
      return ["available", "pending"];
    case "active":
      return ["running"];
    case "delayed":
      return ["scheduled", "retryable"];
    case "completed":
      return ["completed"];
    case "failed":
      return ["discarded", "cancelled"];
  }
}

function mapRiverStateToJobState(rawState: string): JobState {
  switch (rawState) {
    case "running":
      return "active";
    case "scheduled":
    case "retryable":
      return "delayed";
    case "completed":
      return "completed";
    case "discarded":
    case "cancelled":
      return "failed";
    default:
      return "waiting";
  }
}

export function buildRiverJobsSql(
  schema: string,
  queue: string,
  state: JobState,
  limit = 25,
): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const safeLimit = Math.max(1, Math.min(Math.trunc(limit), 500));
  const statesList = riverStatesFor(state).map(sqlLiteral).join(", ");

  return [
    "SELECT",
    "  id::text,",
    "  kind,",
    "  state::text,",
    "  COALESCE(args::jsonb::text, '{}'),",
    "  COALESCE(array_to_json(errors)::jsonb::text, '[]'),",
    "  attempt::text,",
    "  max_attempts::text,",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM created_at) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM attempted_at) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM finalized_at) * 1000)::bigint::text, '')",
    `FROM "${s}"."river_job"`,
    `WHERE queue = ${sqlLiteral(q)} AND state IN (${statesList})`,
    "ORDER BY id DESC",
    `LIMIT ${safeLimit};`,
  ].join(" ");
}

export function buildRiverGetJobSql(schema: string, queue: string, jobId: string): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const id = validateJobId(jobId, "river");

  return [
    "SELECT",
    "  id::text,",
    "  kind,",
    "  state::text,",
    "  COALESCE(args::jsonb::text, '{}'),",
    "  COALESCE(array_to_json(errors)::jsonb::text, '[]'),",
    "  attempt::text,",
    "  max_attempts::text,",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM created_at) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM attempted_at) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM finalized_at) * 1000)::bigint::text, '')",
    `FROM "${s}"."river_job"`,
    `WHERE queue = ${sqlLiteral(q)} AND id = ${id}`,
    "LIMIT 1;",
  ].join(" ");
}

export function parseRiverJobs(output: string, queue: string): JobRecord[] {
  const lines = output
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  return lines.map((line) => {
    const cols = line.split(PSQL_FIELD_SEPARATOR);
    const id = cols[0] ?? "";
    const kind = cols[1] ?? "unknown";
    const rawState = cols[2] ?? "available";
    const data = safeJsonParse(cols[3]);
    const parsedErrors = safeJsonParse(cols[4]);

    let failedReason: string | undefined;
    let stacktrace: string[] | undefined;

    if (Array.isArray(parsedErrors) && parsedErrors.length > 0) {
      const latest = parsedErrors[parsedErrors.length - 1] as Record<string, unknown> | undefined;
      if (latest && typeof latest === "object") {
        if (typeof latest.error === "string" && latest.error.length > 0) {
          failedReason = latest.error;
        }
        if (typeof latest.trace === "string" && latest.trace.trim().length > 0) {
          stacktrace = latest.trace.split(/\r?\n/).filter((l) => l.trim().length > 0);
        }
      }
    }

    return {
      id,
      queue,
      name: kind,
      state: mapRiverStateToJobState(rawState),
      data,
      failedReason,
      stacktrace,
      attemptsMade: parseIntField(cols[5]),
      maxAttempts: parseIntField(cols[6]) || undefined,
      timestamp: parseTimestampMs(cols[7]),
      processedOn: parseTimestampMs(cols[8]),
      finishedOn: parseTimestampMs(cols[9]),
    };
  });
}

export function buildRiverRetryJobSql(schema: string, queue: string, jobId: string): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const id = validateJobId(jobId, "river");

  return [
    `UPDATE "${s}"."river_job"`,
    "SET state = 'available', scheduled_at = NOW(), finalized_at = NULL",
    `WHERE queue = ${sqlLiteral(q)} AND id = ${id} AND state IN ('discarded', 'cancelled')`,
    "RETURNING id::text;",
  ].join(" ");
}

export function buildRiverRetryFailedBulkSql(schema: string, queue: string, limit = 100): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const safeLimit = Math.max(1, Math.min(Math.trunc(limit), 1000));

  return [
    "WITH target AS (",
    `  SELECT id FROM "${s}"."river_job"`,
    `  WHERE queue = ${sqlLiteral(q)} AND state IN ('discarded', 'cancelled')`,
    "  ORDER BY id ASC",
    `  LIMIT ${safeLimit}`,
    "  FOR UPDATE SKIP LOCKED",
    ")",
    `UPDATE "${s}"."river_job" r`,
    "SET state = 'available', scheduled_at = NOW(), finalized_at = NULL",
    "FROM target",
    "WHERE r.id = target.id",
    "RETURNING r.id::text;",
  ].join(" ");
}

export function buildPgBossQueueStatsSql(schema = "pgboss"): string {
  const s = validateIdentifier(schema, "schema");
  return [
    "SELECT",
    "  name,",
    "  COUNT(*) FILTER (WHERE state = 'created' AND start_after <= NOW()) AS waiting,",
    "  COUNT(*) FILTER (WHERE state = 'active') AS active,",
    "  COUNT(*) FILTER (WHERE (state = 'created' AND start_after > NOW()) OR state = 'retry') AS delayed,",
    "  COUNT(*) FILTER (WHERE state = 'completed') AS completed,",
    "  COUNT(*) FILTER (WHERE state IN ('failed', 'expired', 'cancelled')) AS failed",
    `FROM "${s}"."job"`,
    "GROUP BY name",
    "ORDER BY name ASC;",
  ].join(" ");
}

export function parsePgBossQueueStats(output: string): QueueStats[] {
  const lines = output
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  return lines.map((line) => {
    const cols = line.split(PSQL_FIELD_SEPARATOR);
    return {
      name: cols[0] ?? "default",
      backend: "pg-boss",
      waiting: parseIntField(cols[1]),
      active: parseIntField(cols[2]),
      delayed: parseIntField(cols[3]),
      completed: parseIntField(cols[4]),
      failed: parseIntField(cols[5]),
    };
  });
}

function pgBossWhereForState(state: JobState): string {
  switch (state) {
    case "waiting":
      return "state = 'created' AND start_after <= NOW()";
    case "active":
      return "state = 'active'";
    case "delayed":
      return "((state = 'created' AND start_after > NOW()) OR state = 'retry')";
    case "completed":
      return "state = 'completed'";
    case "failed":
      return "state IN ('failed', 'expired', 'cancelled')";
  }
}

function mapPgBossStateToJobState(rawState: string): JobState {
  switch (rawState) {
    case "active":
      return "active";
    case "retry":
      return "delayed";
    case "completed":
      return "completed";
    case "failed":
    case "expired":
    case "cancelled":
      return "failed";
    default:
      return "waiting";
  }
}

export function buildPgBossJobsSql(
  schema: string,
  queue: string,
  state: JobState,
  limit = 25,
): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const safeLimit = Math.max(1, Math.min(Math.trunc(limit), 500));

  return [
    "SELECT",
    "  id::text,",
    "  name,",
    "  state::text,",
    "  COALESCE(data::jsonb::text, '{}'),",
    "  COALESCE(output::jsonb::text, 'null'),",
    "  retry_count::text,",
    "  retry_limit::text,",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM created_on) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM started_on) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM completed_on) * 1000)::bigint::text, '')",
    `FROM "${s}"."job"`,
    `WHERE name = ${sqlLiteral(q)} AND ${pgBossWhereForState(state)}`,
    "ORDER BY created_on DESC",
    `LIMIT ${safeLimit};`,
  ].join(" ");
}

export function buildPgBossGetJobSql(schema: string, queue: string, jobId: string): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const id = validateJobId(jobId, "pg-boss");

  return [
    "SELECT",
    "  id::text,",
    "  name,",
    "  state::text,",
    "  COALESCE(data::jsonb::text, '{}'),",
    "  COALESCE(output::jsonb::text, 'null'),",
    "  retry_count::text,",
    "  retry_limit::text,",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM created_on) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM started_on) * 1000)::bigint::text, ''),",
    "  COALESCE(FLOOR(EXTRACT(EPOCH FROM completed_on) * 1000)::bigint::text, '')",
    `FROM "${s}"."job"`,
    `WHERE name = ${sqlLiteral(q)} AND id = ${sqlLiteral(id)}`,
    "LIMIT 1;",
  ].join(" ");
}

export function parsePgBossJobs(output: string, queue: string): JobRecord[] {
  const lines = output
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  return lines.map((line) => {
    const cols = line.split(PSQL_FIELD_SEPARATOR);
    const id = cols[0] ?? "";
    const name = cols[1] ?? queue;
    const rawState = cols[2] ?? "created";
    const data = safeJsonParse(cols[3]);
    const parsedOutput = safeJsonParse(cols[4]);

    let failedReason: string | undefined;
    let stacktrace: string[] | undefined;

    if (typeof parsedOutput === "string" && parsedOutput.length > 0) {
      failedReason = parsedOutput;
    } else if (parsedOutput && typeof parsedOutput === "object") {
      const obj = parsedOutput as Record<string, unknown>;
      if (typeof obj.message === "string" && obj.message.length > 0) {
        failedReason = obj.message;
      } else if (typeof obj.error === "string" && obj.error.length > 0) {
        failedReason = obj.error;
      }
      if (typeof obj.stack === "string" && obj.stack.trim().length > 0) {
        stacktrace = obj.stack.split(/\r?\n/).filter((l) => l.trim().length > 0);
      }
    }

    return {
      id,
      queue,
      name,
      state: mapPgBossStateToJobState(rawState),
      data,
      failedReason,
      stacktrace,
      attemptsMade: parseIntField(cols[5]),
      maxAttempts: parseIntField(cols[6]) || undefined,
      timestamp: parseTimestampMs(cols[7]),
      processedOn: parseTimestampMs(cols[8]),
      finishedOn: parseTimestampMs(cols[9]),
    };
  });
}

export function buildPgBossRetryJobSql(schema: string, queue: string, jobId: string): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const id = validateJobId(jobId, "pg-boss");

  return [
    `UPDATE "${s}"."job"`,
    "SET state = 'created', retry_count = 0, completed_on = NULL, start_after = NOW()",
    `WHERE name = ${sqlLiteral(q)} AND id = ${sqlLiteral(id)} AND state IN ('failed', 'expired', 'cancelled')`,
    "RETURNING id::text;",
  ].join(" ");
}

export function buildPgBossRetryFailedBulkSql(schema: string, queue: string, limit = 100): string {
  const s = validateIdentifier(schema, "schema");
  const q = validateQueueName(queue);
  const safeLimit = Math.max(1, Math.min(Math.trunc(limit), 1000));

  return [
    "WITH target AS (",
    `  SELECT id FROM "${s}"."job"`,
    `  WHERE name = ${sqlLiteral(q)} AND state IN ('failed', 'expired', 'cancelled')`,
    "  ORDER BY created_on ASC",
    `  LIMIT ${safeLimit}`,
    "  FOR UPDATE SKIP LOCKED",
    ")",
    `UPDATE "${s}"."job" j`,
    "SET state = 'created', retry_count = 0, completed_on = NULL, start_after = NOW()",
    "FROM target",
    "WHERE j.id = target.id",
    "RETURNING j.id::text;",
  ].join(" ");
}

export function parseReturnedJobIds(output: string): string[] {
  return output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

export class PostgresQueueBackend implements QueueBackend {
  readonly kind: PostgresQueueKind;
  readonly readOnly: boolean;

  private readonly schema: string;
  private readonly runner: SqlRunner;

  private constructor(
    kind: PostgresQueueKind,
    schema: string,
    readOnly: boolean,
    runner: SqlRunner,
  ) {
    this.kind = kind;
    this.schema = validateIdentifier(schema, "schema");
    this.readOnly = readOnly;
    this.runner = runner;
  }

  static connect(url: string, options: PostgresBackendOptions): PostgresQueueBackend {
    const parsed = parsePostgresUrl(url);
    const psqlPath = options.psqlPath ?? "psql";
    const timeoutMs = options.timeoutMs ?? 10000;

    const runner: SqlRunner =
      options.sqlRunner ??
      ((sql: string, readOnly: boolean): string => {
        const env: NodeJS.ProcessEnv = { ...process.env };
        if (parsed.password !== undefined) {
          env.PGPASSWORD = parsed.password;
        }
        if (readOnly) {
          const existing = env.PGOPTIONS ? `${env.PGOPTIONS} ` : "";
          env.PGOPTIONS = `${existing}-c default_transaction_read_only=on`;
        }

        const args = [
          "-X",
          "-A",
          "-t",
          "-F",
          PSQL_FIELD_SEPARATOR,
          "-v",
          "ON_ERROR_STOP=1",
          "-d",
          parsed.sanitizedUrl,
          "-c",
          sql,
        ];

        return execFileSync(psqlPath, args, {
          env,
          encoding: "utf8",
          timeout: timeoutMs,
          stdio: ["ignore", "pipe", "pipe"],
        });
      });

    let kind = options.kind;
    let schema = options.schema;

    if (!kind) {
      const detectSql = buildDetectPostgresBackendSql(schema);
      const detectOut = runner(detectSql, true);
      const detected = parseBackendDetection(detectOut);
      kind = detected.kind;
      schema = schema ?? detected.schema;
    }

    const resolvedSchema = schema ?? (kind === "river" ? "public" : "pgboss");
    return new PostgresQueueBackend(kind, resolvedSchema, options.readOnly, runner);
  }

  async listQueues(): Promise<QueueStats[]> {
    if (this.kind === "river") {
      const out = this.runner(buildRiverQueueStatsSql(this.schema), true);
      return parseRiverQueueStats(out);
    }
    const out = this.runner(buildPgBossQueueStatsSql(this.schema), true);
    return parsePgBossQueueStats(out);
  }

  async listJobs(queue: string, state: JobState, limit = 25): Promise<JobRecord[]> {
    if (this.kind === "river") {
      const out = this.runner(buildRiverJobsSql(this.schema, queue, state, limit), true);
      return parseRiverJobs(out, queue);
    }
    const out = this.runner(buildPgBossJobsSql(this.schema, queue, state, limit), true);
    return parsePgBossJobs(out, queue);
  }

  async getJob(queue: string, jobId: string): Promise<JobRecord | null> {
    if (this.kind === "river") {
      const out = this.runner(buildRiverGetJobSql(this.schema, queue, jobId), true);
      return parseRiverJobs(out, queue)[0] ?? null;
    }
    const out = this.runner(buildPgBossGetJobSql(this.schema, queue, jobId), true);
    return parsePgBossJobs(out, queue)[0] ?? null;
  }

  async retryJob(queue: string, jobId: string): Promise<RetryResult> {
    if (this.readOnly) {
      throw new ReadOnlyError(`retry job ${jobId} in queue ${queue}`);
    }

    const sql =
      this.kind === "river"
        ? buildRiverRetryJobSql(this.schema, queue, jobId)
        : buildPgBossRetryJobSql(this.schema, queue, jobId);

    const out = this.runner(sql, false);
    const jobIds = parseReturnedJobIds(out);
    return { queue, retried: jobIds.length, jobIds };
  }

  async retryFailedBulk(queue: string, limit = 100): Promise<RetryResult> {
    if (this.readOnly) {
      throw new ReadOnlyError(`bulk retry failed jobs in queue ${queue}`);
    }

    const sql =
      this.kind === "river"
        ? buildRiverRetryFailedBulkSql(this.schema, queue, limit)
        : buildPgBossRetryFailedBulkSql(this.schema, queue, limit);

    const out = this.runner(sql, false);
    const jobIds = parseReturnedJobIds(out);
    return { queue, retried: jobIds.length, jobIds };
  }

  async close(): Promise<void> {
    // Each psql call is short-lived and closes on exit.
  }
}
