import type { DashboardSnapshot, JobRecord, JobState, QueueStats } from "../types.js";

const SPARKLINE_BLOCKS = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"] as const;

export interface RenderOptions {
  color?: boolean;
  maxStackLines?: number;
}

const ANSI = {
  reset: "\x1b[0m",
  bold: "\x1b[1m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  cyan: "\x1b[36m",
  white: "\x1b[37m",
} as const;

// Strip ANSI CSI, OSC, DCS/APC/PM, 2-byte ESC sequences, and C0/C1 control characters
// so untrusted queue names, payloads, or DLQ error messages cannot spoof terminal UI.
export function sanitizeTerminalText(raw: string): string {
  return raw
    .replace(/\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/g, "")
    .replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "")
    .replace(/\x1b[@-Z\\-_]/g, "")
    .replace(/[\x00-\x08\x0b-\x1f\x7f-\x9f]/g, "");
}

function sanitizeSingleLine(raw: string): string {
  return sanitizeTerminalText(raw).replace(/[\r\n]+/g, " ");
}

function style(text: string, codes: string, enabled: boolean): string {
  return enabled ? `${codes}${text}${ANSI.reset}` : text;
}

export function renderSparkline(values?: ReadonlyArray<number>): string {
  if (!values || values.length === 0) {
    return "--------";
  }

  const clean = values.map((v) => (Number.isFinite(v) && v >= 0 ? v : 0));
  const min = Math.min(...clean);
  const max = Math.max(...clean);

  if (max === min) {
    const block = max === 0 ? SPARKLINE_BLOCKS[0] : SPARKLINE_BLOCKS[3];
    return block.repeat(clean.length);
  }

  const range = max - min;
  return clean
    .map((v) => {
      const normalized = (v - min) / range;
      const idx = Math.min(
        SPARKLINE_BLOCKS.length - 1,
        Math.max(0, Math.round(normalized * (SPARKLINE_BLOCKS.length - 1))),
      );
      return SPARKLINE_BLOCKS[idx] ?? SPARKLINE_BLOCKS[0];
    })
    .join("");
}

export function renderSafetyBadge(readOnly: boolean, color = false): string {
  if (readOnly) {
    return style("[READ-ONLY]", `${ANSI.bold}${ANSI.green}`, color);
  }
  return style("[WRITE ENABLED]", `${ANSI.bold}${ANSI.yellow}`, color);
}

function padRight(text: string, width: number): string {
  return text.length >= width ? text : text + " ".repeat(width - text.length);
}

function padLeft(text: string, width: number): string {
  return text.length >= width ? text : " ".repeat(width - text.length) + text;
}

export function renderQueueTable(queues: ReadonlyArray<QueueStats>, options: RenderOptions = {}): string {
  const color = options.color ?? false;

  if (queues.length === 0) {
    return "  (no queues discovered)";
  }

  const headers = ["QUEUE", "BACKEND", "WAIT", "ACTIVE", "DELAYED", "DONE", "FAILED", "TREND"];

  const rows = queues.map((q) => {
    const safeName = sanitizeSingleLine(q.name);
    const nameLabel = q.paused ? `${safeName} (paused)` : safeName;
    const trend = renderSparkline(q.throughputHistory);
    return {
      raw: [
        nameLabel,
        q.backend,
        String(q.waiting),
        String(q.active),
        String(q.delayed),
        String(q.completed),
        String(q.failed),
        trend,
      ],
      failed: q.failed,
    };
  });

  const totals = queues.reduce(
    (acc, q) => ({
      waiting: acc.waiting + q.waiting,
      active: acc.active + q.active,
      delayed: acc.delayed + q.delayed,
      completed: acc.completed + q.completed,
      failed: acc.failed + q.failed,
    }),
    { waiting: 0, active: 0, delayed: 0, completed: 0, failed: 0 },
  );

  const totalRow = [
    `TOTAL (${queues.length})`,
    "",
    String(totals.waiting),
    String(totals.active),
    String(totals.delayed),
    String(totals.completed),
    String(totals.failed),
    "",
  ];

  const widths = headers.map((header, idx) => {
    const dataMax = Math.max(
      ...rows.map((r) => (r.raw[idx] ?? "").length),
      (totalRow[idx] ?? "").length,
    );
    return Math.max(header.length, dataMax);
  });

  const rightAlignCols = new Set([2, 3, 4, 5, 6]);
  const formatCells = (cells: string[], failedCount = 0, isHeader = false): string => {
    const formatted = cells.map((cell, idx) => {
      const width = widths[idx] ?? cell.length;
      const padded = rightAlignCols.has(idx) ? padLeft(cell, width) : padRight(cell, width);
      if (isHeader) {
        return style(padded, `${ANSI.bold}${ANSI.cyan}`, color);
      }
      if (idx === 6 && failedCount > 0) {
        return style(padded, `${ANSI.bold}${ANSI.red}`, color);
      }
      if (idx === 7 && cell.trim().length > 0) {
        return style(padded, ANSI.cyan, color);
      }
      return padded;
    });
    return `  ${formatted.join("  ")}`;
  };

  const ruleWidth = widths.reduce((sum, w) => sum + w, 0) + (widths.length - 1) * 2;
  const divider = `  ${"-".repeat(ruleWidth)}`;

  const lines: string[] = [
    formatCells(headers, 0, true),
    style(divider, ANSI.dim, color),
    ...rows.map((r) => formatCells(r.raw, r.failed, false)),
    style(divider, ANSI.dim, color),
    formatCells(totalRow, totals.failed, false),
  ];

  return lines.join("\n");
}

function formatTimestamp(ms: number | undefined): string {
  if (ms === undefined || !Number.isFinite(ms)) {
    return "n/a";
  }
  return new Date(ms).toISOString();
}

export function renderJobInspector(
  jobs: ReadonlyArray<JobRecord>,
  queueName: string,
  state: JobState,
  totalInState?: number,
  options: RenderOptions = {},
): string {
  const color = options.color ?? false;
  const maxStackLines = options.maxStackLines ?? 6;
  const safeQueue = sanitizeSingleLine(queueName);
  const countSuffix =
    totalInState !== undefined && totalInState > jobs.length
      ? ` (showing ${jobs.length} of ${totalInState} ${state} jobs)`
      : ` (${jobs.length} ${state} ${jobs.length === 1 ? "job" : "jobs"})`;

  const heading = style(
    `DLQ / Job Inspector: ${safeQueue}${countSuffix}`,
    `${ANSI.bold}${ANSI.white}`,
    color,
  );

  if (jobs.length === 0) {
    return `${heading}\n  (no ${state} jobs found in "${safeQueue}")`;
  }

  const blocks = jobs.map((job, idx) => {
    const lines: string[] = [];
    const attempts =
      job.maxAttempts !== undefined
        ? `${job.attemptsMade}/${job.maxAttempts}`
        : String(job.attemptsMade);

    const jobHeader = `  #${idx + 1}  id=${sanitizeSingleLine(job.id)}  name=${sanitizeSingleLine(job.name)}  state=${job.state}  attempts=${attempts}`;
    lines.push(style(jobHeader, ANSI.bold, color));

    if (job.failedReason) {
      lines.push(`      reason: ${style(sanitizeSingleLine(job.failedReason), ANSI.red, color)}`);
    }

    lines.push(
      style(
        `      created=${formatTimestamp(job.timestamp)}  finished=${formatTimestamp(job.finishedOn)}`,
        ANSI.dim,
        color,
      ),
    );

    const prettyData = sanitizeTerminalText(JSON.stringify(job.data ?? {}, null, 2))
      .split("\n")
      .map((line) => `        ${line}`)
      .join("\n");
    lines.push("      payload:");
    lines.push(prettyData);

    if (job.stacktrace && job.stacktrace.length > 0) {
      lines.push("      stacktrace:");
      const flatLines = job.stacktrace.flatMap((entry) => entry.split(/\r?\n/));
      const trimmed = flatLines.slice(0, maxStackLines);
      for (const traceLine of trimmed) {
        lines.push(`        ${style(sanitizeSingleLine(traceLine), ANSI.dim, color)}`);
      }
      if (flatLines.length > maxStackLines) {
        lines.push(
          `        ${style(`... (${flatLines.length - maxStackLines} more lines)`, ANSI.dim, color)}`,
        );
      }
    }

    return lines.join("\n");
  });

  return `${heading}\n${blocks.join("\n\n")}`;
}

export function renderDashboard(snapshot: DashboardSnapshot, options: RenderOptions = {}): string {
  const color = options.color ?? false;
  const badge = renderSafetyBadge(snapshot.readOnly, color);
  const title = style(`q9s v${snapshot.version}`, `${ANSI.bold}${ANSI.cyan}`, color);
  const safeTarget = sanitizeSingleLine(snapshot.target);
  const safeCtx = snapshot.contextName ? sanitizeSingleLine(snapshot.contextName) : undefined;

  const headerLines = [
    `${title}  ${badge}`,
    `  target:  ${safeTarget}${safeCtx ? ` (ctx: ${safeCtx})` : ""}`,
    `  backend: ${snapshot.backend}    captured: ${snapshot.capturedAt}`,
  ];

  const sections: string[] = [
    headerLines.join("\n"),
    "",
    renderQueueTable(snapshot.queues, options),
  ];

  if (snapshot.inspectedQueue) {
    const queueStat = snapshot.queues.find((q) => q.name === snapshot.inspectedQueue);
    const state = snapshot.inspectedState ?? "failed";
    const totalInState = queueStat
      ? state === "failed"
        ? queueStat.failed
        : state === "waiting"
          ? queueStat.waiting
          : state === "active"
            ? queueStat.active
            : state === "delayed"
              ? queueStat.delayed
              : queueStat.completed
      : undefined;

    sections.push(
      "",
      renderJobInspector(
        snapshot.inspectedJobs,
        snapshot.inspectedQueue,
        state,
        totalInState,
        options,
      ),
    );
  }

  const footer = snapshot.readOnly
    ? "Safety: read-only mode is active by default. Pass --allow-write to unlock --retry or --retry-failed."
    : "Warning: write mode is enabled (--allow-write). Mutations will modify live queue state.";

  sections.push("", style(footer, ANSI.dim, color));

  return sections.join("\n");
}

export function renderJson(payload: unknown): string {
  return JSON.stringify(payload, null, 2);
}
