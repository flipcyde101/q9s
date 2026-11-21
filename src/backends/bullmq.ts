import { RespClient } from "../clients/resp.js";
import {
  type BackendOptions,
  type JobRecord,
  type JobState,
  type QueueBackend,
  type QueueStats,
  ReadOnlyError,
  type RetryResult,
} from "../types.js";

export interface BullMqBackendOptions extends BackendOptions {
  client?: RespClient;
  rejectUnauthorized?: boolean;
}

function safeJsonParse(raw: string | undefined): unknown {
  if (raw === undefined || raw === "") {
    return {};
  }
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function parseOptionalInt(raw: string | undefined): number | undefined {
  if (raw === undefined || raw === "") {
    return undefined;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseStacktrace(raw: string | undefined): string[] | undefined {
  if (!raw) {
    return undefined;
  }
  const parsed = safeJsonParse(raw);
  if (Array.isArray(parsed)) {
    const lines = parsed
      .map((entry) => String(entry ?? "").trim())
      .filter((entry) => entry.length > 0);
    return lines.length > 0 ? lines : undefined;
  }
  if (typeof parsed === "string" && parsed.trim().length > 0) {
    return [parsed.trim()];
  }
  return undefined;
}

export function extractQueueNameFromKey(key: string, prefix: string, suffix: string): string | null {
  const head = `${prefix}:`;
  const tail = `:${suffix}`;
  if (!key.startsWith(head) || !key.endsWith(tail)) {
    return null;
  }
  const inner = key.slice(head.length, key.length - tail.length);
  return inner.length > 0 ? inner : null;
}

export function parseBullMqJobHash(
  queue: string,
  jobId: string,
  state: JobState,
  hash: Record<string, string>,
): JobRecord {
  const opts = safeJsonParse(hash.opts);
  const maxAttempts =
    typeof opts === "object" &&
    opts !== null &&
    "attempts" in opts &&
    typeof (opts as { attempts?: unknown }).attempts === "number"
      ? (opts as { attempts: number }).attempts
      : undefined;

  const attemptsMade =
    parseOptionalInt(hash.attemptsMade) ?? parseOptionalInt(hash.atm) ?? 0;

  return {
    id: jobId,
    queue,
    name: hash.name || "default",
    state,
    data: safeJsonParse(hash.data),
    failedReason: hash.failedReason || undefined,
    stacktrace: parseStacktrace(hash.stacktrace),
    attemptsMade,
    maxAttempts,
    timestamp: parseOptionalInt(hash.timestamp),
    processedOn: parseOptionalInt(hash.processedOn),
    finishedOn: parseOptionalInt(hash.finishedOn),
    delay: parseOptionalInt(hash.delay),
  };
}

export class BullMqBackend implements QueueBackend {
  readonly kind = "bullmq" as const;
  readonly readOnly: boolean;

  private readonly client: RespClient;
  private readonly ownsClient: boolean;
  private readonly prefix: string;

  private constructor(client: RespClient, ownsClient: boolean, options: BullMqBackendOptions) {
    this.client = client;
    this.ownsClient = ownsClient;
    this.readOnly = options.readOnly;
    this.prefix = (options.prefix ?? "bull").replace(/:+$/, "");
  }

  static async connect(url: string, options: BullMqBackendOptions): Promise<BullMqBackend> {
    if (options.client) {
      return new BullMqBackend(options.client, false, options);
    }
    const client = await RespClient.connect(url, {
      timeoutMs: options.timeoutMs ?? 5000,
      rejectUnauthorized: options.rejectUnauthorized,
    });
    return new BullMqBackend(client, true, options);
  }

  static fromClient(client: RespClient, options: BullMqBackendOptions): BullMqBackend {
    return new BullMqBackend(client, false, options);
  }

  private key(queue: string, suffix: string): string {
    return `${this.prefix}:${queue}:${suffix}`;
  }

  async discoverQueues(): Promise<string[]> {
    const queueNames = new Set<string>();
    const suffixes = ["meta", "id", "failed"] as const;

    for (const suffix of suffixes) {
      const keys = await this.client.scanAll(`${this.prefix}:*:${suffix}`);
      for (const fullKey of keys) {
        const name = extractQueueNameFromKey(fullKey, this.prefix, suffix);
        if (name) {
          queueNames.add(name);
        }
      }
    }

    return Array.from(queueNames).sort((a, b) => a.localeCompare(b));
  }

  async listQueues(): Promise<QueueStats[]> {
    const names = await this.discoverQueues();
    const results: QueueStats[] = [];

    for (const name of names) {
      const [
        waitCount,
        pausedCount,
        prioritizedCount,
        active,
        delayed,
        completed,
        failed,
        pausedMeta,
        metricsRaw,
      ] = await Promise.all([
        this.client.llen(this.key(name, "wait")),
        this.client.llen(this.key(name, "paused")),
        this.client.zcard(this.key(name, "prioritized")),
        this.client.llen(this.key(name, "active")),
        this.client.zcard(this.key(name, "delayed")),
        this.client.zcard(this.key(name, "completed")),
        this.client.zcard(this.key(name, "failed")),
        this.client.hget(this.key(name, "meta"), "paused"),
        this.client.lrange(this.key(name, "metrics:completed:data"), 0, 15),
      ]);

      const parsedMetrics = metricsRaw
        .map((v) => Number.parseFloat(v))
        .filter((n) => Number.isFinite(n))
        .reverse();

      results.push({
        name,
        backend: "bullmq",
        waiting: waitCount + pausedCount + prioritizedCount,
        active,
        delayed,
        completed,
        failed,
        paused: pausedMeta === "1",
        throughputHistory: parsedMetrics.length > 0 ? parsedMetrics : undefined,
      });
    }

    return results;
  }

  async listJobs(queue: string, state: JobState, limit = 25): Promise<JobRecord[]> {
    const safeLimit = Math.max(1, Math.min(limit, 500));
    const stop = safeLimit - 1;

    let ids: string[];
    switch (state) {
      case "failed":
        ids = await this.client.zrevrange(this.key(queue, "failed"), 0, stop);
        break;
      case "completed":
        ids = await this.client.zrevrange(this.key(queue, "completed"), 0, stop);
        break;
      case "delayed":
        ids = await this.client.zrange(this.key(queue, "delayed"), 0, stop);
        break;
      case "waiting":
        ids = await this.client.lrange(this.key(queue, "wait"), 0, stop);
        break;
      case "active":
        ids = await this.client.lrange(this.key(queue, "active"), 0, stop);
        break;
    }

    const jobs: JobRecord[] = [];
    for (const id of ids) {
      const hash = await this.client.hgetall(this.key(queue, id));
      if (Object.keys(hash).length === 0) {
        continue;
      }
      jobs.push(parseBullMqJobHash(queue, id, state, hash));
    }

    return jobs;
  }

  async getJob(queue: string, jobId: string): Promise<JobRecord | null> {
    const hash = await this.client.hgetall(this.key(queue, jobId));
    if (Object.keys(hash).length === 0) {
      return null;
    }

    const [failedScore, completedScore, delayedScore] = await Promise.all([
      this.client.command("ZSCORE", this.key(queue, "failed"), jobId),
      this.client.command("ZSCORE", this.key(queue, "completed"), jobId),
      this.client.command("ZSCORE", this.key(queue, "delayed"), jobId),
    ]);

    let state: JobState = "waiting";
    if (failedScore !== null) {
      state = "failed";
    } else if (completedScore !== null) {
      state = "completed";
    } else if (delayedScore !== null) {
      state = "delayed";
    } else if (hash.processedOn && !hash.finishedOn) {
      state = "active";
    }

    return parseBullMqJobHash(queue, jobId, state, hash);
  }

  async retryJob(queue: string, jobId: string): Promise<RetryResult> {
    if (this.readOnly) {
      throw new ReadOnlyError(`retry job ${jobId} in queue ${queue}`);
    }

    const failedKey = this.key(queue, "failed");
    const score = await this.client.command("ZSCORE", failedKey, jobId);
    if (score === null) {
      return { queue, retried: 0, jobIds: [] };
    }

    const jobKey = this.key(queue, jobId);
    const waitKey = this.key(queue, "wait");

    await this.client.command("MULTI");
    await this.client.command("ZREM", failedKey, jobId);
    await this.client.command(
      "HDEL",
      jobKey,
      "failedReason",
      "stacktrace",
      "processedOn",
      "finishedOn",
      "returnvalue",
    );
    await this.client.command("RPUSH", waitKey, jobId);
    const execRes = await this.client.command("EXEC");

    if (execRes === null) {
      return { queue, retried: 0, jobIds: [] };
    }

    return { queue, retried: 1, jobIds: [jobId] };
  }

  async retryFailedBulk(queue: string, limit = 100): Promise<RetryResult> {
    if (this.readOnly) {
      throw new ReadOnlyError(`bulk retry failed jobs in queue ${queue}`);
    }

    const safeLimit = Math.max(1, Math.min(limit, 1000));
    const failedKey = this.key(queue, "failed");
    const waitKey = this.key(queue, "wait");
    const ids = await this.client.zrange(failedKey, 0, safeLimit - 1);

    if (ids.length === 0) {
      return { queue, retried: 0, jobIds: [] };
    }

    await this.client.command("MULTI");
    for (const jobId of ids) {
      const jobKey = this.key(queue, jobId);
      await this.client.command("ZREM", failedKey, jobId);
      await this.client.command(
        "HDEL",
        jobKey,
        "failedReason",
        "stacktrace",
        "processedOn",
        "finishedOn",
        "returnvalue",
      );
      await this.client.command("RPUSH", waitKey, jobId);
    }
    const execRes = await this.client.command("EXEC");

    if (execRes === null) {
      return { queue, retried: 0, jobIds: [] };
    }

    return { queue, retried: ids.length, jobIds: ids };
  }

  async close(): Promise<void> {
    if (this.ownsClient) {
      await this.client.close();
    }
  }
}
