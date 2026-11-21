import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { runCli } from "../src/cli.js";
import { startMockRedisServer } from "../src/demo/fixture.js";

const DIST_CLI = path.resolve(process.cwd(), "dist", "cli.js");

function createCapturedIo() {
  const stdout: string[] = [];
  const stderr: string[] = [];
  return {
    stdout,
    stderr,
    io: {
      stdout: (line: string) => stdout.push(line),
      stderr: (line: string) => stderr.push(line),
      isTty: false,
    },
  };
}

describe("q9s CLI in-process behavior", () => {
  it("runs --demo and shows 3 queues, sparklines, webhooks DLQ, and [READ-ONLY] badge", async () => {
    const cap = createCapturedIo();
    const code = await runCli(["--demo"], cap.io);
    expect(code).toBe(0);

    const output = cap.stdout.join("\n");
    expect(output).toContain("[READ-ONLY]");
    expect(output).toContain("emails");
    expect(output).toContain("webhooks");
    expect(output).toContain("billing-sync");
    expect(output).toContain("DLQ / Job Inspector: webhooks (showing 3 of 14 failed jobs)");
    expect(output).toContain("ECONNRESET");
    expect(output).not.toContain("\u2014");
  });

  it("refuses mutations by default and requires --yes for bulk retries", async () => {
    const roCap = createCapturedIo();
    const roCode = await runCli(["--demo", "--retry", "wh-4814", "--queue", "webhooks"], roCap.io);
    expect(roCode).toBe(2);
    expect(roCap.stderr.join("\n")).toContain("read-only mode");

    const noYesCap = createCapturedIo();
    const noYesCode = await runCli(
      ["--demo", "--retry-failed", "webhooks", "--allow-write"],
      noYesCap.io,
    );
    expect(noYesCode).toBe(2);
    expect(noYesCap.stderr.join("\n")).toContain("--yes");

    const okCap = createCapturedIo();
    const okCode = await runCli(
      ["--demo", "--retry-failed", "webhooks", "--allow-write", "--yes", "--json"],
      okCap.io,
    );
    expect(okCode).toBe(0);
    const parsed = JSON.parse(okCap.stdout.join("\n"));
    expect(parsed).toMatchObject({
      action: "retry-failed",
      queue: "webhooks",
      retried: 14,
      readOnly: false,
    });
  });

  it("loads --ctx from config file, redacts password, and honors readOnly pin", async () => {
    const server = await startMockRedisServer({ password: "super-secret-password" });
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "q9s-cli-ctx-"));

    try {
      const cfgPath = path.join(tmpDir, "contexts.json");
      fs.writeFileSync(
        cfgPath,
        JSON.stringify({
          contexts: {
            "prod-demo": {
              url: server.url,
              backend: "bullmq",
              readOnly: true,
            },
          },
        }),
        "utf8",
      );

      const listCap = createCapturedIo();
      expect(await runCli(["--contexts", "--config", cfgPath], listCap.io)).toBe(0);
      expect(listCap.stdout.join("\n")).not.toContain("super-secret-password");
      expect(listCap.stdout.join("\n")).toContain("***");

      const runCap = createCapturedIo();
      expect(
        await runCli(["--ctx", "prod-demo", "--config", cfgPath, "--json"], runCap.io),
      ).toBe(0);
      const rawJson = runCap.stdout.join("\n");
      expect(rawJson).not.toContain("super-secret-password");
      const snapshot = JSON.parse(rawJson);
      expect(snapshot.contextName).toBe("prod-demo");
      expect(snapshot.queues).toHaveLength(3);

      // Pinned readOnly: true context refuses --allow-write
      const blockedCap = createCapturedIo();
      expect(
        await runCli(
          [
            "--ctx",
            "prod-demo",
            "--config",
            cfgPath,
            "--retry-failed",
            "webhooks",
            "--allow-write",
            "--yes",
          ],
          blockedCap.io,
        ),
      ).toBe(2);
      expect(blockedCap.stderr.join("\n")).toContain("pinned to readOnly: true");
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
      await server.close();
    }
  });
});

describe("Built dist/cli.js E2E smoke test", () => {
  it("executes dist/cli.js --demo --json cleanly over real localhost TCP", () => {
    expect(fs.existsSync(DIST_CLI)).toBe(true);

    const startedAt = Date.now();
    const out = execFileSync(process.execPath, [DIST_CLI, "--demo", "--json"], {
      encoding: "utf8",
      timeout: 10000,
    });
    const elapsedMs = Date.now() - startedAt;

    const snapshot = JSON.parse(out);
    expect(snapshot.version).toBe("0.1.0");
    expect(snapshot.backend).toBe("bullmq");
    expect(snapshot.readOnly).toBe(true);
    expect(snapshot.inspectedQueue).toBe("webhooks");
    expect(snapshot.queues).toHaveLength(3);

    const byName = Object.fromEntries(
      snapshot.queues.map((q: { name: string }) => [q.name, q]),
    );
    expect(byName["emails"]).toMatchObject({
      waiting: 12,
      active: 4,
      delayed: 8,
      completed: 1482,
      failed: 1,
    });
    expect(byName["webhooks"]).toMatchObject({
      waiting: 37,
      active: 6,
      delayed: 19,
      completed: 942,
      failed: 14,
    });
    expect(byName["billing-sync"]).toMatchObject({
      waiting: 5,
      active: 2,
      delayed: 3,
      completed: 618,
      failed: 2,
    });
    expect(snapshot.inspectedJobs).toHaveLength(3);
    expect(snapshot.inspectedJobs[0].failedReason).toContain("ECONNRESET");
    expect(elapsedMs).toBeLessThan(5000);
  });

  it("executes dist/cli.js --help cleanly", () => {
    const out = execFileSync(process.execPath, [DIST_CLI, "--help"], {
      encoding: "utf8",
      timeout: 10000,
    });
    expect(out).toContain("q9s v0.1.0");
    expect(out).toContain("--read-only");
    expect(out).toContain("--allow-write");
    expect(out).not.toContain("\u2014");
  });
});
