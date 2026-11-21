export type QueueBackendKind = "bullmq" | "river" | "pg-boss";

export type JobState = "waiting" | "active" | "delayed" | "completed" | "failed";

export interface QueueStats {
  name: string;
  backend: QueueBackendKind;
  waiting: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
  paused?: boolean;
  throughputHistory?: number[];
}

export interface JobRecord {
  id: string;
  queue: string;
  name: string;
  state: JobState;
  data: unknown;
  failedReason?: string;
  stacktrace?: string[];
  attemptsMade: number;
  maxAttempts?: number;
  timestamp?: number;
  processedOn?: number;
  finishedOn?: number;
  delay?: number;
}

export interface ContextEntry {
  name: string;
  url: string;
  backend?: QueueBackendKind;
  readOnly?: boolean;
  prefix?: string;
  schema?: string;
}

export interface RetryResult {
  queue: string;
  retried: number;
  jobIds: string[];
}

export interface BackendOptions {
  readOnly: boolean;
  timeoutMs?: number;
  prefix?: string;
  schema?: string;
}

export interface QueueBackend {
  readonly kind: QueueBackendKind;
  readonly readOnly: boolean;
  listQueues(): Promise<QueueStats[]>;
  listJobs(queue: string, state: JobState, limit?: number): Promise<JobRecord[]>;
  getJob(queue: string, jobId: string): Promise<JobRecord | null>;
  retryJob(queue: string, jobId: string): Promise<RetryResult>;
  retryFailedBulk(queue: string, limit?: number): Promise<RetryResult>;
  close(): Promise<void>;
}

export interface DashboardSnapshot {
  version: string;
  target: string;
  contextName?: string;
  backend: QueueBackendKind;
  readOnly: boolean;
  capturedAt: string;
  queues: QueueStats[];
  inspectedQueue?: string;
  inspectedState?: JobState;
  inspectedJobs: JobRecord[];
}

export class ReadOnlyError extends Error {
  readonly code = "Q9S_READ_ONLY";

  constructor(action: string) {
    super(
      `Refused write action "${action}" in read-only mode. Re-run with --allow-write to permit mutations.`,
    );
    this.name = "ReadOnlyError";
  }
}
