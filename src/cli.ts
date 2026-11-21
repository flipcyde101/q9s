import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BullMqBackend } from "./backends/bullmq.js";
import { PostgresQueueBackend, type PostgresQueueKind } from "./backends/postgres.js";
import {
  loadContexts,
  redactContext,
  redactUrl,
  resolveContext,
} from "./config/contexts.js";
import { startMockRedisServer } from "./demo/fixture.js";
import { renderDashboard, renderJson } from "./render/dashboard.js";
import {
  type DashboardSnapshot,
  type JobState,
  type QueueBackend,
  type QueueBackendKind,
  ReadOnlyError,
} from "./types.js";

export const Q9S_VERSION = "0.1.0";

const VALID_STATES = new Set<JobState>([
  "waiting",
  "active",
  "delayed",
  "completed",
  "failed",
]);

const VALID_BACKENDS = new Set<QueueBackendKind>(["bullmq", "river", "pg-boss"]);
const STRICT_UINT_RE = /^\d+$/;

const VALUE_FLAGS = new Set([
  "--ctx",
  "--config",
  "--backend",
  "--prefix",
  "--schema",
  "--inspect",
  "--queue",
  "--state",
  "--limit",
  "--timeout",
  "--retry",
  "--retry-failed",
]);

export interface ParsedCliArgs {
  url?: string;
  ctx?: string;
  config?: string;
  listContexts: boolean;
  demo: boolean;
  backend?: QueueBackendKind;
  prefix?: string;
  schema?: string;
  inspect?: string;
  queue?: string;
  state: JobState;
  limit?: number;
  readOnlyFlag: boolean;
  allowWrite: boolean;
  retryJobId?: string;
  retryFailedQueue?: string;
  yes: boolean;
  json: boolean;
  noColor: boolean;
  timeoutMs: number;
  help: boolean;
  version: boolean;
}

export interface CliIo {
  stdout: (line: string) => void;
  stderr: (line: string) => void;
  isTty?: boolean;
}

export function formatHelpText(): string {
  return [
    `q9s v${Q9S_VERSION} - k9s for background job queues (BullMQ, River, pg-boss)`,
    "",
    "Usage:",
    "  q9s <redis://...|rediss://...|postgres://...> [options]",
    "  q9s --ctx <name> [options]",
    "  q9s --demo [--json] [--inspect <queue>]",
    "  q9s --contexts [--config <path>] [--json]",
    "",
    "Target Options:",
    "  --demo                    Start live in-process mock BullMQ Redis TCP server",
    "  --ctx <name>              Load a named context from ~/.config/q9s/contexts.json|.yaml",
    "  --contexts                List configured contexts with redacted credentials",
    "  --config <path>           Override contexts config file path",
    "  --backend <kind>          Force backend: bullmq | river | pg-boss",
    "  --prefix <prefix>         BullMQ Redis key prefix (default: bull)",
    "  --schema <schema>         Postgres schema override (default: auto-detected)",
    "  --timeout <ms>            Connection/query timeout in ms (default: 5000)",
    "",
    "Inspection Options:",
    "  --inspect <queue>         Inspect jobs in a specific queue (default: highest failed)",
    "  --state <state>           Job state: waiting | active | delayed | completed | failed",
    "  --limit <n>               Max jobs to inspect (default: 3) or bulk-retry (default: 100)",
    "  --json                    Emit machine-readable JSON output",
    "  --no-color                Disable ANSI terminal colors",
    "",
    "Safety & Write Actions (read-only by default):",
    "  --read-only               Enforce read-only mode (default: true)",
    "  --allow-write             Explicitly enable write actions (--retry, --retry-failed)",
    "  --retry <jobId>           Retry a single failed job (use with --inspect/--queue + --allow-write)",
    "  --queue <queue>           Target queue for --retry",
    "  --retry-failed <queue>    Bulk retry failed jobs in <queue> (requires --allow-write --yes)",
    "  -y, --yes                 Confirm bulk write operation",
    "",
    "Other:",
    "  -h, --help                Show this help message",
    "  -v, --version             Print q9s version",
    "",
    "Examples:",
    "  q9s --demo",
    "  q9s --demo --inspect webhooks --json",
    "  q9s redis://localhost:6379",
    "  q9s postgres://app:secret@localhost:5432/prod --inspect emails",
    "  q9s --ctx prod-bull --retry-failed webhooks --allow-write --yes",
  ].join("\n");
}

function expandEqualsFlags(argv: ReadonlyArray<string>): string[] {
  const expanded: string[] = [];
  for (const arg of argv) {
    if (arg.startsWith("--") && arg.includes("=")) {
      const eqIdx = arg.indexOf("=");
      const flag = arg.slice(0, eqIdx);
      const val = arg.slice(eqIdx + 1);
      if (VALUE_FLAGS.has(flag)) {
        expanded.push(flag, val);
        continue;
      }
    }
    expanded.push(arg);
  }
  return expanded;
}

function readNextValue(
  argv: ReadonlyArray<string>,
  index: number,
  flag: string,
  allowLeadingDash = false,
): string {
  const val = argv[index + 1];
  if (val === undefined || val === "" || (!allowLeadingDash && val.startsWith("-"))) {
    throw new Error(`Option ${flag} requires a value.`);
  }
  return val;
}

export function parseCliArgs(rawArgv: ReadonlyArray<string>): ParsedCliArgs {
  const argv = expandEqualsFlags(rawArgv);
  const parsed: ParsedCliArgs = {
    listContexts: false,
    demo: false,
    state: "failed",
    readOnlyFlag: false,
    allowWrite: false,
    yes: false,
    json: false,
    noColor: false,
    timeoutMs: 5000,
    help: false,
    version: false,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;

    switch (arg) {
      case "-h":
      case "--help":
        parsed.help = true;
        break;
      case "-v":
      case "--version":
        parsed.version = true;
        break;
      case "--demo":
        parsed.demo = true;
        break;
      case "--contexts":
        parsed.listContexts = true;
        break;
      case "--read-only":
        parsed.readOnlyFlag = true;
        break;
      case "--allow-write":
        parsed.allowWrite = true;
        break;
      case "--yes":
      case "-y":
        parsed.yes = true;
        break;
      case "--json":
        parsed.json = true;
        break;
      case "--no-color":
        parsed.noColor = true;
        break;
      case "--ctx":
        parsed.ctx = readNextValue(argv, i, arg);
        i++;
        break;
      case "--config":
        parsed.config = readNextValue(argv, i, arg);
        i++;
        break;
      case "--backend": {
        const val = readNextValue(argv, i, arg).toLowerCase() as QueueBackendKind;
        if (!VALID_BACKENDS.has(val)) {
          throw new Error(`Invalid --backend "${val}". Expected bullmq, river, or pg-boss.`);
        }
        parsed.backend = val;
        i++;
        break;
      }
      case "--prefix":
        parsed.prefix = readNextValue(argv, i, arg);
        i++;
        break;
      case "--schema":
        parsed.schema = readNextValue(argv, i, arg);
        i++;
        break;
      case "--inspect":
        parsed.inspect = readNextValue(argv, i, arg);
        i++;
        break;
      case "--queue":
        parsed.queue = readNextValue(argv, i, arg);
        i++;
        break;
      case "--state": {
        const val = readNextValue(argv, i, arg).toLowerCase() as JobState;
        if (!VALID_STATES.has(val)) {
          throw new Error(
            `Invalid --state "${val}". Expected waiting, active, delayed, completed, or failed.`,
          );
        }
        parsed.state = val;
        i++;
        break;
      }
      case "--limit": {
        const raw = readNextValue(argv, i, arg, true);
        if (!STRICT_UINT_RE.test(raw)) {
          throw new Error(`Invalid --limit "${raw}". Expected a positive integer.`);
        }
        const n = Number(raw);
        if (!Number.isSafeInteger(n) || n < 1) {
          throw new Error(`Invalid --limit "${raw}". Expected a positive integer.`);
        }
        parsed.limit = n;
        i++;
        break;
      }
      case "--timeout": {
        const raw = readNextValue(argv, i, arg, true);
        if (!STRICT_UINT_RE.test(raw)) {
          throw new Error(`Invalid --timeout "${raw}". Expected milliseconds >= 100.`);
        }
        const n = Number(raw);
        if (!Number.isSafeInteger(n) || n < 100) {
          throw new Error(`Invalid --timeout "${raw}". Expected milliseconds >= 100.`);
        }
        parsed.timeoutMs = n;
        i++;
        break;
      }
      case "--retry":
        parsed.retryJobId = readNextValue(argv, i, arg, true);
        i++;
        break;
      case "--retry-failed":
        parsed.retryFailedQueue = readNextValue(argv, i, arg);
        i++;
        break;
      default:
        if (arg.startsWith("-")) {
          throw new Error(`Unknown option: ${arg}`);
        }
        if (parsed.url !== undefined) {
          throw new Error(`Unexpected extra positional argument: ${arg}`);
        }
        parsed.url = arg;
        break;
    }
  }

  return parsed;
}

function detectProtocolKind(rawUrl: string): "redis" | "postgres" {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error(`Invalid target URL: ${redactUrl(rawUrl)}`);
  }
  if (parsed.protocol === "redis:" || parsed.protocol === "rediss:") {
    return "redis";
  }
  if (parsed.protocol === "postgres:" || parsed.protocol === "postgresql:") {
    return "postgres";
  }
  throw new Error(
    `Unsupported URL protocol "${parsed.protocol}". Expected redis://, rediss://, postgres://, or postgresql://`,
  );
}

function writeError(io: CliIo, json: boolean, message: string, code = "Q9S_ERROR"): void {
  if (json) {
    io.stdout(renderJson({ error: message, code }));
  } else {
    io.stderr(`Error: ${message}`);
  }
}

export async function runCli(
  argv: ReadonlyArray<string>,
  io: CliIo = {
    stdout: (line) => process.stdout.write(`${line}\n`),
    stderr: (line) => process.stderr.write(`${line}\n`),
    isTty: Boolean(process.stdout.isTTY),
  },
): Promise<number> {
  let args: ParsedCliArgs;
  try {
    args = parseCliArgs(argv);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const wantsJson = argv.includes("--json");
    writeError(io, wantsJson, msg, "Q9S_USAGE_ERROR");
    return 2;
  }

  if (args.help) {
    io.stdout(formatHelpText());
    return 0;
  }

  if (args.version) {
    io.stdout(Q9S_VERSION);
    return 0;
  }

  if (args.listContexts) {
    try {
      const contexts = loadContexts(args.config).map(redactContext);
      if (args.json) {
        io.stdout(renderJson({ contexts }));
      } else if (contexts.length === 0) {
        io.stdout("No contexts configured. Create ~/.config/q9s/contexts.json or contexts.yaml.");
      } else {
        for (const ctx of contexts) {
          const mode = ctx.readOnly === false ? "allow-write" : "read-only";
          io.stdout(`${ctx.name}  ${ctx.backend ?? "auto"}  [${mode}]  ${ctx.url}`);
        }
      }
      return 0;
    } catch (err) {
      writeError(io, args.json, err instanceof Error ? err.message : String(err));
      return 1;
    }
  }

  if (args.readOnlyFlag && args.allowWrite) {
    writeError(
      io,
      args.json,
      "Cannot pass both --read-only and --allow-write together.",
      "Q9S_USAGE_ERROR",
    );
    return 2;
  }

  const targetSelectorCount =
    (args.demo ? 1 : 0) + (args.ctx ? 1 : 0) + (args.url ? 1 : 0);
  if (targetSelectorCount > 1) {
    writeError(
      io,
      args.json,
      "Conflicting target selectors: specify only one of <url>, --ctx <name>, or --demo.",
      "Q9S_USAGE_ERROR",
    );
    return 2;
  }

  let targetUrl = args.url;
  let contextName: string | undefined;
  let backendHint = args.backend;
  let prefix = args.prefix;
  let schema = args.schema;
  let readOnly = true;

  if (args.ctx) {
    try {
      const ctx = resolveContext(args.ctx, args.config);
      contextName = ctx.name;
      targetUrl = ctx.url;
      backendHint = backendHint ?? ctx.backend;
      prefix = prefix ?? ctx.prefix;
      schema = schema ?? ctx.schema;
      if (ctx.readOnly === true && args.allowWrite) {
        writeError(
          io,
          args.json,
          `Context "${ctx.name}" is pinned to readOnly: true in config. Remove readOnly or target another context.`,
          "Q9S_READ_ONLY",
        );
        return 2;
      }
    } catch (err) {
      writeError(io, args.json, err instanceof Error ? err.message : String(err));
      return 2;
    }
  }

  if (args.allowWrite && !args.readOnlyFlag) {
    readOnly = false;
  }

  if (!args.demo && !targetUrl) {
    writeError(
      io,
      args.json,
      "Missing target URL. Provide a redis:// or postgres:// URL, --ctx <name>, or --demo.",
      "Q9S_USAGE_ERROR",
    );
    return 2;
  }

  const wantsRetrySingle = args.retryJobId !== undefined;
  const wantsRetryBulk = args.retryFailedQueue !== undefined;

  if (wantsRetrySingle && wantsRetryBulk) {
    writeError(
      io,
      args.json,
      "Cannot combine --retry and --retry-failed in a single invocation.",
      "Q9S_USAGE_ERROR",
    );
    return 2;
  }

  if ((wantsRetrySingle || wantsRetryBulk) && readOnly) {
    const action = wantsRetryBulk
      ? `bulk retry failed jobs in queue "${args.retryFailedQueue}"`
      : `retry job "${args.retryJobId}"`;
    const err = new ReadOnlyError(action);
    writeError(io, args.json, err.message, err.code);
    return 2;
  }

  if (wantsRetrySingle && !(args.queue ?? args.inspect)) {
    writeError(
      io,
      args.json,
      "--retry requires a target queue via --queue <queue> or --inspect <queue>.",
      "Q9S_USAGE_ERROR",
    );
    return 2;
  }

  if (wantsRetryBulk && !args.yes) {
    writeError(
      io,
      args.json,
      `Bulk retry of failed jobs in "${args.retryFailedQueue}" requires explicit confirmation with --yes.`,
      "Q9S_CONFIRMATION_REQUIRED",
    );
    return 2;
  }

  let demoServer: Awaited<ReturnType<typeof startMockRedisServer>> | null = null;
  let backend: QueueBackend | null = null;

  try {
    demoServer = args.demo ? await startMockRedisServer({ prefix: prefix ?? "bull" }) : null;
    const effectiveUrl = demoServer ? demoServer.url : targetUrl!;
    const protocolKind = detectProtocolKind(effectiveUrl);

    if (protocolKind === "redis") {
      if (backendHint && backendHint !== "bullmq") {
        throw new Error(`Redis URLs only support the "bullmq" backend (got "${backendHint}").`);
      }
      backend = await BullMqBackend.connect(effectiveUrl, {
        readOnly,
        prefix: prefix ?? "bull",
        timeoutMs: args.timeoutMs,
      });
    } else {
      if (backendHint === "bullmq") {
        throw new Error('Postgres URLs support "river" or "pg-boss", not "bullmq".');
      }
      backend = PostgresQueueBackend.connect(effectiveUrl, {
        readOnly,
        kind: backendHint as PostgresQueueKind | undefined,
        schema,
        timeoutMs: args.timeoutMs,
      });
    }

    const displayTarget = demoServer
      ? `${redactUrl(effectiveUrl)} (demo)`
      : redactUrl(effectiveUrl);

    if (wantsRetrySingle) {
      const targetQueue = (args.queue ?? args.inspect)!;
      const result = await backend.retryJob(targetQueue, args.retryJobId!);
      if (args.json) {
        io.stdout(
          renderJson({
            action: "retry",
            target: displayTarget,
            backend: backend.kind,
            readOnly: backend.readOnly,
            ...result,
          }),
        );
      } else if (result.retried === 0) {
        io.stdout(
          `[WRITE ENABLED] Job "${args.retryJobId}" was not found in failed state for queue "${targetQueue}".`,
        );
      } else {
        io.stdout(
          `[WRITE ENABLED] Retried ${result.retried} job in "${result.queue}": ${result.jobIds.join(", ")}`,
        );
      }
      return 0;
    }

    if (wantsRetryBulk) {
      const result = await backend.retryFailedBulk(args.retryFailedQueue!, args.limit ?? 100);
      if (args.json) {
        io.stdout(
          renderJson({
            action: "retry-failed",
            target: displayTarget,
            backend: backend.kind,
            readOnly: backend.readOnly,
            ...result,
          }),
        );
      } else {
        io.stdout(
          `[WRITE ENABLED] Bulk-retried ${result.retried} failed ${result.retried === 1 ? "job" : "jobs"} in "${result.queue}".`,
        );
      }
      return 0;
    }

    const queues = await backend.listQueues();

    let inspectedQueue = args.inspect;
    if (!inspectedQueue && queues.length > 0) {
      const sortedByFailed = [...queues].sort((a, b) => b.failed - a.failed);
      if ((sortedByFailed[0]?.failed ?? 0) > 0) {
        inspectedQueue = sortedByFailed[0]!.name;
      }
    }

    const inspectLimit = args.limit ?? 3;
    const inspectedJobs = inspectedQueue
      ? await backend.listJobs(inspectedQueue, args.state, inspectLimit)
      : [];

    const snapshot: DashboardSnapshot = {
      version: Q9S_VERSION,
      target: displayTarget,
      contextName,
      backend: backend.kind,
      readOnly: backend.readOnly,
      capturedAt: new Date().toISOString(),
      queues,
      inspectedQueue,
      inspectedState: inspectedQueue ? args.state : undefined,
      inspectedJobs,
    };

    if (args.json) {
      io.stdout(renderJson(snapshot));
    } else {
      const useColor =
        !args.noColor &&
        !process.env.NO_COLOR &&
        Boolean(io.isTty);
      io.stdout(renderDashboard(snapshot, { color: useColor }));
    }
    return 0;
  } catch (err) {
    const code = err instanceof ReadOnlyError ? err.code : "Q9S_ERROR";
    writeError(io, args.json, err instanceof Error ? err.message : String(err), code);
    return err instanceof ReadOnlyError ? 2 : 1;
  } finally {
    if (backend) {
      await backend.close().catch(() => undefined);
    }
    if (demoServer) {
      await demoServer.close().catch(() => undefined);
    }
  }
}

function isExecutedDirectly(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  const normalize = (p: string) => (process.platform === "win32" ? p.toLowerCase() : p);
  try {
    const currentFile = fs.realpathSync.native(fileURLToPath(import.meta.url));
    const entryFile = fs.realpathSync.native(path.resolve(entry));
    return normalize(currentFile) === normalize(entryFile);
  } catch {
    return normalize(path.resolve(entry)) === normalize(fileURLToPath(import.meta.url));
  }
}

if (isExecutedDirectly()) {
  runCli(process.argv.slice(2))
    .then((code) => {
      process.exitCode = code;
    })
    .catch((err) => {
      process.stderr.write(`Fatal: ${err instanceof Error ? err.message : String(err)}\n`);
      process.exitCode = 1;
    });
}
