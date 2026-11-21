import net from "node:net";
import { RespParser, type RespValue } from "../clients/resp.js";

export interface DemoServerHandle {
  host: string;
  port: number;
  url: string;
  close(): Promise<void>;
}

export interface MockRedisOptions {
  password?: string;
  seedBullMqDemo?: boolean;
  prefix?: string;
}

interface RedisStore {
  hashes: Map<string, Map<string, string>>;
  lists: Map<string, string[]>;
  zsets: Map<string, Map<string, number>>;
  strings: Map<string, string>;
}

function createEmptyStore(): RedisStore {
  return {
    hashes: new Map(),
    lists: new Map(),
    zsets: new Map(),
    strings: new Map(),
  };
}

function sanitizeRespLine(text: string): string {
  return text.replace(/[\r\n]+/g, " ");
}

function encodeRespReply(value: RespValue | { error: string } | { simple: string }): Buffer {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    if ("error" in value) {
      return Buffer.from(`-${sanitizeRespLine(value.error)}\r\n`, "utf8");
    }
    if ("simple" in value) {
      return Buffer.from(`+${sanitizeRespLine(value.simple)}\r\n`, "utf8");
    }
  }

  if (value === null) {
    return Buffer.from("$-1\r\n", "utf8");
  }
  if (typeof value === "number") {
    return Buffer.from(`:${Math.trunc(value)}\r\n`, "utf8");
  }
  if (typeof value === "string") {
    const bytes = Buffer.from(value, "utf8");
    return Buffer.concat([Buffer.from(`$${bytes.length}\r\n`, "utf8"), bytes, Buffer.from("\r\n", "utf8")]);
  }
  if (Array.isArray(value)) {
    const parts: Buffer[] = [Buffer.from(`*${value.length}\r\n`, "utf8")];
    for (const item of value) {
      parts.push(encodeRespReply(item));
    }
    return Buffer.concat(parts);
  }
  return Buffer.from("$-1\r\n", "utf8");
}

function globToRegExp(pattern: string): RegExp {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  const regexStr = `^${escaped.replace(/\*/g, ".*").replace(/\?/g, ".")}$`;
  return new RegExp(regexStr);
}

function getAllKeys(store: RedisStore): string[] {
  const keys = new Set<string>();
  for (const k of store.hashes.keys()) keys.add(k);
  for (const k of store.lists.keys()) keys.add(k);
  for (const k of store.zsets.keys()) keys.add(k);
  for (const k of store.strings.keys()) keys.add(k);
  return Array.from(keys).sort();
}

function normalizeSlice(length: number, start: number, stop: number): [number, number] {
  const s = start < 0 ? Math.max(0, length + start) : start;
  const e = stop < 0 ? length + stop : Math.min(length - 1, stop);
  return [s, e];
}

function sortedZsetMembers(zset: Map<string, number>, reverse = false): string[] {
  const entries = Array.from(zset.entries());
  entries.sort((a, b) => {
    if (a[1] !== b[1]) {
      return reverse ? b[1] - a[1] : a[1] - b[1];
    }
    return reverse ? b[0].localeCompare(a[0]) : a[0].localeCompare(b[0]);
  });
  return entries.map(([member]) => member);
}

function seedQueueCounters(
  store: RedisStore,
  prefix: string,
  queue: string,
  counts: {
    waiting: number;
    active: number;
    delayed: number;
    completed: number;
    paused?: boolean;
    metrics: number[];
  },
): void {
  const meta = new Map<string, string>([
    ["opts.maxLen.count", "10000"],
    ["paused", counts.paused ? "1" : "0"],
  ]);
  store.hashes.set(`${prefix}:${queue}:meta`, meta);
  store.strings.set(`${prefix}:${queue}:id`, String(counts.completed + counts.waiting + 100));

  const waitList: string[] = [];
  for (let i = 1; i <= counts.waiting; i++) {
    waitList.push(`${queue}-wait-${i}`);
  }
  store.lists.set(`${prefix}:${queue}:wait`, waitList);

  const activeList: string[] = [];
  for (let i = 1; i <= counts.active; i++) {
    activeList.push(`${queue}-active-${i}`);
  }
  store.lists.set(`${prefix}:${queue}:active`, activeList);

  const delayedSet = new Map<string, number>();
  const baseNow = Date.UTC(2026, 10, 21, 19, 0, 0);
  for (let i = 1; i <= counts.delayed; i++) {
    delayedSet.set(`${queue}-delayed-${i}`, baseNow + i * 60_000);
  }
  store.zsets.set(`${prefix}:${queue}:delayed`, delayedSet);

  const completedSet = new Map<string, number>();
  for (let i = 1; i <= counts.completed; i++) {
    completedSet.set(`${queue}-done-${i}`, baseNow - i * 1_000);
  }
  store.zsets.set(`${prefix}:${queue}:completed`, completedSet);

  // BullMQ stores newest metric buckets at head of list
  store.lists.set(
    `${prefix}:${queue}:metrics:completed:data`,
    [...counts.metrics].reverse().map(String),
  );
}

function addFailedJob(
  store: RedisStore,
  prefix: string,
  queue: string,
  job: {
    id: string;
    name: string;
    data: Record<string, unknown>;
    failedReason: string;
    stacktrace: string[];
    attemptsMade: number;
    maxAttempts: number;
    timestamp: number;
    processedOn: number;
    finishedOn: number;
  },
): void {
  const failedKey = `${prefix}:${queue}:failed`;
  let zset = store.zsets.get(failedKey);
  if (!zset) {
    zset = new Map<string, number>();
    store.zsets.set(failedKey, zset);
  }
  zset.set(job.id, job.finishedOn);

  const hash = new Map<string, string>([
    ["name", job.name],
    ["data", JSON.stringify(job.data)],
    ["opts", JSON.stringify({ attempts: job.maxAttempts, backoff: { type: "exponential", delay: 2000 } })],
    ["failedReason", job.failedReason],
    ["stacktrace", JSON.stringify(job.stacktrace)],
    ["attemptsMade", String(job.attemptsMade)],
    ["timestamp", String(job.timestamp)],
    ["processedOn", String(job.processedOn)],
    ["finishedOn", String(job.finishedOn)],
  ]);
  store.hashes.set(`${prefix}:${queue}:${job.id}`, hash);
}

export function seedBullMqDemoData(store: RedisStore, prefix = "bull"): void {
  const baseTs = Date.UTC(2026, 10, 21, 18, 45, 0);

  // 1. emails queue (healthy)
  seedQueueCounters(store, prefix, "emails", {
    waiting: 12,
    active: 4,
    delayed: 8,
    completed: 1482,
    metrics: [42, 46, 51, 49, 54, 58, 55, 61, 59, 64, 62, 66],
  });
  addFailedJob(store, prefix, "emails", {
    id: "email-9041",
    name: "send-receipt",
    data: {
      to: "finance@acme-example.test",
      template: "invoice-paid-v3",
      invoiceId: "inv_01J9E3K8",
    },
    failedReason: "SMTP 451 4.3.0 Temporary local problem, please try later",
    stacktrace: [
      "Error: SMTP 451 4.3.0 Temporary local problem, please try later",
      "    at SmtpTransport.send (/app/src/mail/smtp.ts:118:17)",
      "    at processEmailJob (/app/src/workers/emails.ts:44:9)",
    ],
    attemptsMade: 3,
    maxAttempts: 3,
    timestamp: baseTs - 320_000,
    processedOn: baseTs - 310_000,
    finishedOn: baseTs - 305_000,
  });

  // 2. webhooks queue (spiking DLQ with 14 failed jobs)
  seedQueueCounters(store, prefix, "webhooks", {
    waiting: 37,
    active: 6,
    delayed: 19,
    completed: 942,
    metrics: [74, 71, 66, 59, 48, 37, 26, 18, 14, 11, 9, 7],
  });

  const webhookEndpoints = [
    "https://hooks.partner-a.example/v2/events",
    "https://api.merchant-sync.example/webhooks/orders",
    "https://ingest.northwind.example/stripe-relay",
  ];
  const webhookEvents = [
    "order.fulfilled",
    "subscription.renewed",
    "payment.captured",
    "invoice.finalized",
  ];

  for (let i = 1; i <= 14; i++) {
    const jobNum = 4800 + i;
    const endpoint = webhookEndpoints[(i - 1) % webhookEndpoints.length] ?? webhookEndpoints[0]!;
    const webhookEvent = webhookEvents[(i - 1) % webhookEvents.length] ?? webhookEvents[0]!;
    const isConnReset = i % 3 !== 0;
    const reason = isConnReset
      ? `read ECONNRESET while posting ${webhookEvent} to ${endpoint}`
      : `Upstream returned HTTP 502 Bad Gateway from ${endpoint}`;

    const finishedOn = baseTs + i * 12_000;
    addFailedJob(store, prefix, "webhooks", {
      id: `wh-${jobNum}`,
      name: "deliver-webhook",
      data: {
        eventId: `evt_2026_11_${jobNum}`,
        webhookEvent,
        endpoint,
        customerId: `cus_${100 + i}`,
        attemptDurationMs: isConnReset ? 5012 : 1840,
        httpStatus: isConnReset ? null : 502,
      },
      failedReason: reason,
      stacktrace: isConnReset
        ? [
            `Error: ${reason}`,
            "    at TLSWrap.onStreamRead (node:internal/stream_base_commons:216:20)",
            "    at WebhookDispatcher.post (/app/src/webhooks/dispatcher.ts:89:15)",
            "    at Worker.processJob (/app/src/workers/webhooks.ts:52:11)",
          ]
        : [
            `Error: ${reason}`,
            "    at assertOkResponse (/app/src/webhooks/http.ts:61:11)",
            "    at WebhookDispatcher.post (/app/src/webhooks/dispatcher.ts:94:7)",
            "    at Worker.processJob (/app/src/workers/webhooks.ts:52:11)",
          ],
      attemptsMade: 5,
      maxAttempts: 5,
      timestamp: finishedOn - 25_000,
      processedOn: finishedOn - 5_000,
      finishedOn,
    });
  }

  // 3. billing-sync queue (steady with 2 failed jobs)
  seedQueueCounters(store, prefix, "billing-sync", {
    waiting: 5,
    active: 2,
    delayed: 3,
    completed: 618,
    metrics: [19, 22, 21, 24, 23, 25, 22, 26, 24, 25, 23, 27],
  });

  addFailedJob(store, prefix, "billing-sync", {
    id: "bill-7701",
    name: "reconcile-invoice",
    data: {
      accountId: "acct_8841",
      invoiceId: "in_1Qx92A",
      currency: "USD",
      amountCents: 24900,
    },
    failedReason: "StripeRateLimitError: 429 Too Many Requests on /v1/invoices/in_1Qx92A",
    stacktrace: [
      "StripeRateLimitError: 429 Too Many Requests on /v1/invoices/in_1Qx92A",
      "    at BillingClient.fetchInvoice (/app/src/billing/client.ts:74:13)",
      "    at reconcileInvoice (/app/src/workers/billing-sync.ts:39:18)",
    ],
    attemptsMade: 4,
    maxAttempts: 4,
    timestamp: baseTs - 90_000,
    processedOn: baseTs - 84_000,
    finishedOn: baseTs - 82_000,
  });

  addFailedJob(store, prefix, "billing-sync", {
    id: "bill-7702",
    name: "sync-usage-meter",
    data: {
      accountId: "acct_9912",
      meterId: "mtr_api_calls",
      quantity: 184200,
      period: "2026-11",
    },
    failedReason: "ConflictError: usage ledger lock timeout after 3000ms",
    stacktrace: [
      "ConflictError: usage ledger lock timeout after 3000ms",
      "    at LedgerStore.commitUsage (/app/src/billing/ledger.ts:103:11)",
      "    at syncUsageMeter (/app/src/workers/billing-sync.ts:77:9)",
    ],
    attemptsMade: 3,
    maxAttempts: 3,
    timestamp: baseTs - 40_000,
    processedOn: baseTs - 35_000,
    finishedOn: baseTs - 32_000,
  });
}

function executeCommand(
  store: RedisStore,
  args: string[],
  authPassword: string | undefined,
  session: { authenticated: boolean; db: number },
): RespValue | { error: string } | { simple: string } {
  const cmd = (args[0] ?? "").toUpperCase();

  if (cmd === "AUTH") {
    const provided = args.length >= 3 ? args[2] : args[1];
    if (authPassword === undefined) {
      return { simple: "OK" };
    }
    if (provided === authPassword) {
      session.authenticated = true;
      return { simple: "OK" };
    }
    return { error: "WRONGPASS invalid username-password pair or user is disabled." };
  }

  if (authPassword !== undefined && !session.authenticated && cmd !== "QUIT") {
    return { error: "NOAUTH Authentication required." };
  }

  switch (cmd) {
    case "PING":
      return args[1] !== undefined ? args[1] : { simple: "PONG" };

    case "QUIT":
      return { simple: "OK" };

    case "SELECT": {
      const db = Number.parseInt(args[1] ?? "0", 10);
      if (!Number.isInteger(db) || db < 0) {
        return { error: "ERR invalid DB index" };
      }
      session.db = db;
      return { simple: "OK" };
    }

    case "SCAN": {
      const cursor = Number.parseInt(args[1] ?? "0", 10) || 0;
      let matchPattern = "*";
      let count = 100;
      for (let i = 2; i < args.length; i++) {
        const token = (args[i] ?? "").toUpperCase();
        if (token === "MATCH" && args[i + 1] !== undefined) {
          matchPattern = args[i + 1]!;
          i++;
        } else if (token === "COUNT" && args[i + 1] !== undefined) {
          count = Math.max(1, Number.parseInt(args[i + 1]!, 10) || 100);
          i++;
        }
      }

      const re = globToRegExp(matchPattern);
      const allKeys = getAllKeys(store);
      const end = Math.min(allKeys.length, cursor + count);
      const slice = allKeys.slice(cursor, end).filter((k) => re.test(k));
      const nextCursor = end >= allKeys.length ? "0" : String(end);
      return [nextCursor, slice];
    }

    case "KEYS": {
      const re = globToRegExp(args[1] ?? "*");
      return getAllKeys(store).filter((k) => re.test(k));
    }

    case "GET": {
      const key = args[1] ?? "";
      return store.strings.get(key) ?? null;
    }

    case "SET": {
      const key = args[1] ?? "";
      const val = args[2] ?? "";
      store.strings.set(key, val);
      return { simple: "OK" };
    }

    case "EXISTS": {
      let found = 0;
      for (let i = 1; i < args.length; i++) {
        const k = args[i] ?? "";
        if (
          store.hashes.has(k) ||
          store.lists.has(k) ||
          store.zsets.has(k) ||
          store.strings.has(k)
        ) {
          found++;
        }
      }
      return found;
    }

    case "LLEN": {
      const list = store.lists.get(args[1] ?? "");
      return list ? list.length : 0;
    }

    case "LRANGE": {
      const list = store.lists.get(args[1] ?? "");
      if (!list || list.length === 0) {
        return [];
      }
      const start = Number.parseInt(args[2] ?? "0", 10);
      const stop = Number.parseInt(args[3] ?? "-1", 10);
      const [s, e] = normalizeSlice(list.length, start, stop);
      if (s > e || s >= list.length) {
        return [];
      }
      return list.slice(s, e + 1);
    }

    case "LPUSH": {
      const key = args[1] ?? "";
      let list = store.lists.get(key);
      if (!list) {
        list = [];
        store.lists.set(key, list);
      }
      for (let i = 2; i < args.length; i++) {
        list.unshift(args[i] ?? "");
      }
      return list.length;
    }

    case "RPUSH": {
      const key = args[1] ?? "";
      let list = store.lists.get(key);
      if (!list) {
        list = [];
        store.lists.set(key, list);
      }
      for (let i = 2; i < args.length; i++) {
        list.push(args[i] ?? "");
      }
      return list.length;
    }

    case "ZCARD": {
      const zset = store.zsets.get(args[1] ?? "");
      return zset ? zset.size : 0;
    }

    case "ZADD": {
      const key = args[1] ?? "";
      let zset = store.zsets.get(key);
      if (!zset) {
        zset = new Map();
        store.zsets.set(key, zset);
      }
      let added = 0;
      for (let i = 2; i + 1 < args.length; i += 2) {
        const score = Number.parseFloat(args[i] ?? "0");
        const member = args[i + 1] ?? "";
        if (!zset.has(member)) {
          added++;
        }
        zset.set(member, score);
      }
      return added;
    }

    case "ZSCORE": {
      const zset = store.zsets.get(args[1] ?? "");
      const score = zset?.get(args[2] ?? "");
      return score === undefined ? null : String(score);
    }

    case "ZREM": {
      const zset = store.zsets.get(args[1] ?? "");
      if (!zset) {
        return 0;
      }
      let removed = 0;
      for (let i = 2; i < args.length; i++) {
        if (zset.delete(args[i] ?? "")) {
          removed++;
        }
      }
      return removed;
    }

    case "ZRANGE":
    case "ZREVRANGE": {
      const zset = store.zsets.get(args[1] ?? "");
      if (!zset || zset.size === 0) {
        return [];
      }
      const ordered = sortedZsetMembers(zset, cmd === "ZREVRANGE");
      const start = Number.parseInt(args[2] ?? "0", 10);
      const stop = Number.parseInt(args[3] ?? "-1", 10);
      const [s, e] = normalizeSlice(ordered.length, start, stop);
      if (s > e || s >= ordered.length) {
        return [];
      }
      return ordered.slice(s, e + 1);
    }

    case "HGET": {
      const hash = store.hashes.get(args[1] ?? "");
      return hash?.get(args[2] ?? "") ?? null;
    }

    case "HGETALL": {
      const hash = store.hashes.get(args[1] ?? "");
      if (!hash) {
        return [];
      }
      const out: string[] = [];
      for (const [k, v] of hash.entries()) {
        out.push(k, v);
      }
      return out;
    }

    case "HSET": {
      const key = args[1] ?? "";
      let hash = store.hashes.get(key);
      if (!hash) {
        hash = new Map();
        store.hashes.set(key, hash);
      }
      let added = 0;
      for (let i = 2; i + 1 < args.length; i += 2) {
        const field = args[i] ?? "";
        const val = args[i + 1] ?? "";
        if (!hash.has(field)) {
          added++;
        }
        hash.set(field, val);
      }
      return added;
    }

    case "HDEL": {
      const hash = store.hashes.get(args[1] ?? "");
      if (!hash) {
        return 0;
      }
      let removed = 0;
      for (let i = 2; i < args.length; i++) {
        if (hash.delete(args[i] ?? "")) {
          removed++;
        }
      }
      return removed;
    }

    case "WATCH":
    case "UNWATCH":
      return { simple: "OK" };

    default:
      return { error: `ERR unknown command '${cmd}'` };
  }
}

export async function startMockRedisServer(options: MockRedisOptions = {}): Promise<DemoServerHandle> {
  const store = createEmptyStore();
  if (options.seedBullMqDemo !== false) {
    seedBullMqDemoData(store, options.prefix ?? "bull");
  }

  const sockets = new Set<net.Socket>();

  const server = net.createServer((socket) => {
    sockets.add(socket);
    const parser = new RespParser();
    const session = {
      authenticated: options.password === undefined,
      db: 0,
      inMulti: false,
      txQueue: [] as string[][],
    };

    socket.on("close", () => {
      sockets.delete(socket);
    });

    socket.on("data", (chunk: Buffer) => {
      let frames: ReturnType<RespParser["push"]>;
      try {
        frames = parser.push(chunk);
      } catch (err) {
        socket.write(encodeRespReply({ error: `ERR ${err instanceof Error ? err.message : String(err)}` }));
        return;
      }

      for (const frame of frames) {
        if (!Array.isArray(frame)) {
          socket.write(encodeRespReply({ error: "ERR expected array command" }));
          continue;
        }
        const args = frame.map((item) => String(item ?? ""));
        const cmd = (args[0] ?? "").toUpperCase();

        if (cmd === "MULTI") {
          session.inMulti = true;
          session.txQueue = [];
          socket.write(encodeRespReply({ simple: "OK" }));
          continue;
        }

        if (cmd === "DISCARD") {
          session.inMulti = false;
          session.txQueue = [];
          socket.write(encodeRespReply({ simple: "OK" }));
          continue;
        }

        if (cmd === "EXEC") {
          if (!session.inMulti) {
            socket.write(encodeRespReply({ error: "ERR EXEC without MULTI" }));
            continue;
          }
          session.inMulti = false;
          const queued = session.txQueue;
          session.txQueue = [];
          const replies: RespValue[] = [];
          for (const queuedArgs of queued) {
            const reply = executeCommand(store, queuedArgs, options.password, session);
            if (reply !== null && typeof reply === "object" && !Array.isArray(reply)) {
              if ("simple" in reply) {
                replies.push(reply.simple);
              } else if ("error" in reply) {
                replies.push(reply.error);
              }
            } else {
              replies.push(reply);
            }
          }
          socket.write(encodeRespReply(replies));
          continue;
        }

        if (session.inMulti) {
          session.txQueue.push(args);
          socket.write(encodeRespReply({ simple: "QUEUED" }));
          continue;
        }

        const reply = executeCommand(store, args, options.password, session);
        socket.write(encodeRespReply(reply));

        if (cmd === "QUIT") {
          socket.end();
        }
      }
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const addr = server.address();
  if (!addr || typeof addr === "string") {
    throw new Error("Failed to bind mock Redis TCP server.");
  }

  const host = "127.0.0.1";
  const port = addr.port;
  const authPrefix = options.password ? `:${encodeURIComponent(options.password)}@` : "";
  const url = `redis://${authPrefix}${host}:${port}`;

  return {
    host,
    port,
    url,
    close: async () => {
      for (const sock of sockets) {
        sock.destroy();
      }
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    },
  };
}
