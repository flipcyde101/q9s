import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  loadContexts,
  parseSimpleYamlContexts,
  redactContext,
  redactUrl,
  resolveContext,
} from "../src/config/contexts.js";
import {
  renderDashboard,
  renderJobInspector,
  renderQueueTable,
  renderSafetyBadge,
  renderSparkline,
} from "../src/render/dashboard.js";
import type { DashboardSnapshot } from "../src/types.js";

describe("Dashboard and sparkline renderer", () => {
  it("renders Unicode sparklines across varying distributions", () => {
    expect(renderSparkline([0, 1, 2, 3, 4, 5, 6, 7])).toBe("▁▂▃▄▅▆▇█");
    expect(renderSparkline([0, 0, 0, 0])).toBe("▁▁▁▁");
    expect(renderSparkline([10, 10, 10])).toBe("▄▄▄");
    expect(renderSparkline([])).toBe("--------");
    expect(renderSparkline(undefined)).toBe("--------");
  });

  it("renders explicit safety badges and queue tables without em dashes", () => {
    expect(renderSafetyBadge(true, false)).toBe("[READ-ONLY]");
    expect(renderSafetyBadge(false, false)).toBe("[WRITE ENABLED]");

    const snapshot: DashboardSnapshot = {
      version: "0.1.0",
      target: "redis://:***@127.0.0.1:6379",
      backend: "bullmq",
      readOnly: true,
      capturedAt: "2026-11-21T19:00:00.000Z",
      queues: [
        {
          name: "webhooks",
          backend: "bullmq",
          waiting: 37,
          active: 6,
          delayed: 19,
          completed: 942,
          failed: 14,
          throughputHistory: [70, 50, 30, 10],
        },
      ],
      inspectedQueue: "webhooks",
      inspectedState: "failed",
      inspectedJobs: [
        {
          id: "wh-4814",
          queue: "webhooks",
          name: "deliver-webhook",
          state: "failed",
          data: { eventId: "evt_1", status: 502 },
          failedReason: "read ECONNRESET",
          stacktrace: ["Error: read ECONNRESET", "    at dispatcher.ts:89:15"],
          attemptsMade: 5,
          maxAttempts: 5,
          timestamp: Date.UTC(2026, 10, 21, 18, 45, 0),
          finishedOn: Date.UTC(2026, 10, 21, 18, 45, 25),
        },
      ],
    };

    const output = renderDashboard(snapshot, { color: false });
    expect(output).toContain("q9s v0.1.0  [READ-ONLY]");
    expect(output).toContain("webhooks");
    expect(output).toContain("█▆▃▁");
    expect(output).toContain("wh-4814");
    expect(output).toContain("read ECONNRESET");
    expect(output).toContain('"eventId": "evt_1"');
    expect(output).not.toContain("\u2014");

    expect(renderQueueTable([])).toContain("no queues discovered");
    expect(renderJobInspector([], "emails", "failed")).toContain('no failed jobs found in "emails"');
  });
});

describe("Named contexts loader and credential redaction", () => {
  it("redacts passwords and sensitive query parameters from URLs", () => {
    expect(redactUrl("redis://default:mysecret@redis.prod:6379/0")).toBe(
      "redis://default:***@redis.prod:6379/0",
    );
    expect(redactUrl("redis://:onlypass@redis.prod:6379")).toBe("redis://:***@redis.prod:6379");
    expect(redactUrl("postgres://app:p%40ss@db.prod:5432/main?token=abc123")).toBe(
      "postgres://app:***@db.prod:5432/main?token=***",
    );

    const redacted = redactContext({
      name: "prod",
      url: "postgres://admin:supersecret@db.prod:5432/app",
      readOnly: true,
    });
    expect(redacted.url).not.toContain("supersecret");
    expect(redacted.url).toContain("***");
  });

  it("loads JSON and simple YAML contexts cleanly", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "q9s-ctx-"));
    try {
      const jsonFile = path.join(tmpDir, "contexts.json");
      fs.writeFileSync(
        jsonFile,
        JSON.stringify({
          contexts: {
            "prod-bull": {
              url: "rediss://default:secret@redis.internal:6379/0",
              backend: "bullmq",
              readOnly: true,
              prefix: "appbull",
            },
          },
        }),
        "utf8",
      );

      const fromJson = loadContexts(jsonFile);
      expect(fromJson).toHaveLength(1);
      expect(resolveContext("prod-bull", jsonFile)).toMatchObject({
        name: "prod-bull",
        backend: "bullmq",
        readOnly: true,
        prefix: "appbull",
      });

      const yamlText = [
        "# q9s contexts",
        "contexts:",
        "  staging-river:",
        "    url: postgres://app:yamlsecret@db.staging:5432/app",
        "    backend: river",
        "    schema: public",
        "    readOnly: true",
      ].join("\n");

      const parsedYaml = parseSimpleYamlContexts(yamlText);
      expect(parsedYaml).toEqual([
        {
          name: "staging-river",
          url: "postgres://app:yamlsecret@db.staging:5432/app",
          backend: "river",
          schema: "public",
          readOnly: true,
          prefix: undefined,
        },
      ]);

      const yamlFile = path.join(tmpDir, "contexts.yaml");
      fs.writeFileSync(yamlFile, yamlText, "utf8");
      expect(resolveContext("staging-river", yamlFile).schema).toBe("public");
      expect(() => resolveContext("missing", yamlFile)).toThrow(/not found/);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
