import { describe, expect, it } from "vitest";
import { BullMqBackend, extractQueueNameFromKey } from "../src/backends/bullmq.js";
import { startMockRedisServer } from "../src/demo/fixture.js";
import { ReadOnlyError } from "../src/types.js";

describe("BullMqBackend", () => {
  it("extracts queue names cleanly from BullMQ keys", () => {
    expect(extractQueueNameFromKey("bull:webhooks:meta", "bull", "meta")).toBe("webhooks");
    expect(extractQueueNameFromKey("app:billing:sync:meta", "app", "meta")).toBe("billing:sync");
    expect(extractQueueNameFromKey("other:webhooks:meta", "bull", "meta")).toBeNull();
  });

  it("discovers seeded BullMQ queues and reads accurate counts and DLQ payloads", async () => {
    const server = await startMockRedisServer();
    const backend = await BullMqBackend.connect(server.url, { readOnly: true });

    try {
      const queues = await backend.listQueues();
      expect(queues.map((q) => q.name)).toEqual(["billing-sync", "emails", "webhooks"]);

      const webhooks = queues.find((q) => q.name === "webhooks");
      expect(webhooks).toMatchObject({
        name: "webhooks",
        backend: "bullmq",
        waiting: 37,
        active: 6,
        delayed: 19,
        completed: 942,
        failed: 14,
        paused: false,
      });
      expect(webhooks?.throughputHistory).toHaveLength(12);

      const failedJobs = await backend.listJobs("webhooks", "failed", 5);
      expect(failedJobs).toHaveLength(5);
      expect(failedJobs[0]?.id).toBe("wh-4814");
      expect(failedJobs[0]?.name).toBe("deliver-webhook");
      expect(failedJobs[0]?.failedReason).toContain("ECONNRESET");
      expect(failedJobs[0]?.stacktrace?.[0]).toContain("ECONNRESET");
      expect(failedJobs[0]?.attemptsMade).toBe(5);
      expect(failedJobs[0]?.maxAttempts).toBe(5);
      expect(failedJobs[0]?.data).toMatchObject({
        eventId: "evt_2026_11_4814",
        customerId: "cus_114",
      });

      const single = await backend.getJob("emails", "email-9041");
      expect(single?.state).toBe("failed");
      expect(single?.failedReason).toContain("SMTP 451");
    } finally {
      await backend.close();
      await server.close();
    }
  });

  it("blocks retries in read-only mode and performs atomic retries when write mode is enabled", async () => {
    const server = await startMockRedisServer();
    const roBackend = await BullMqBackend.connect(server.url, { readOnly: true });
    const rwBackend = await BullMqBackend.connect(server.url, { readOnly: false });

    try {
      await expect(roBackend.retryJob("webhooks", "wh-4814")).rejects.toBeInstanceOf(ReadOnlyError);
      await expect(roBackend.retryFailedBulk("webhooks", 10)).rejects.toBeInstanceOf(ReadOnlyError);

      // Retry single failed job
      const singleRes = await rwBackend.retryJob("webhooks", "wh-4814");
      expect(singleRes).toEqual({ queue: "webhooks", retried: 1, jobIds: ["wh-4814"] });

      // Retrying same job again returns 0 because it is no longer in failed set
      const repeatRes = await rwBackend.retryJob("webhooks", "wh-4814");
      expect(repeatRes).toEqual({ queue: "webhooks", retried: 0, jobIds: [] });

      const afterSingle = (await rwBackend.listQueues()).find((q) => q.name === "webhooks");
      expect(afterSingle?.failed).toBe(13);
      expect(afterSingle?.waiting).toBe(38);

      const retriedJob = await rwBackend.getJob("webhooks", "wh-4814");
      expect(retriedJob?.state).toBe("waiting");
      expect(retriedJob?.failedReason).toBeUndefined();
      expect(retriedJob?.stacktrace).toBeUndefined();

      // Bulk retry remaining failed jobs
      const bulkRes = await rwBackend.retryFailedBulk("webhooks", 50);
      expect(bulkRes.retried).toBe(13);
      expect(bulkRes.jobIds).toHaveLength(13);

      const afterBulk = (await rwBackend.listQueues()).find((q) => q.name === "webhooks");
      expect(afterBulk?.failed).toBe(0);
      expect(afterBulk?.waiting).toBe(51);
    } finally {
      await roBackend.close();
      await rwBackend.close();
      await server.close();
    }
  });
});
