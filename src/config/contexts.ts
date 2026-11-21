import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ContextEntry, QueueBackendKind } from "../types.js";

const VALID_BACKENDS = new Set<QueueBackendKind>(["bullmq", "river", "pg-boss"]);
const SENSITIVE_QUERY_KEYS = new Set([
  "password",
  "pass",
  "passwd",
  "pwd",
  "token",
  "secret",
  "apikey",
  "api_key",
  "auth",
]);

export function redactUrl(rawUrl: string): string {
  try {
    const parsed = new URL(rawUrl);
    const hasRedactedPassword = parsed.password.length > 0;

    const searchParams = new URLSearchParams(parsed.search);
    let modifiedQuery = false;
    for (const [key] of searchParams) {
      if (SENSITIVE_QUERY_KEYS.has(key.toLowerCase())) {
        searchParams.set(key, "***");
        modifiedQuery = true;
      }
    }

    const protocol = parsed.protocol;
    const authPart = (() => {
      if (parsed.username && hasRedactedPassword) {
        return `${parsed.username}:***@`;
      }
      if (hasRedactedPassword) {
        return `:***@`;
      }
      if (parsed.username) {
        return `${parsed.username}@`;
      }
      return "";
    })();

    const hostPart = parsed.port ? `${parsed.hostname}:${parsed.port}` : parsed.hostname;
    const queryStr = modifiedQuery ? `?${searchParams.toString()}` : parsed.search;
    return `${protocol}//${authPart}${hostPart}${parsed.pathname}${queryStr}`;
  } catch {
    let redacted = rawUrl.replace(/(:\/\/[^:@/?#\s]*:)([^/?#]+)(@)/g, "$1***$3");
    redacted = redacted.replace(
      /([?&](?:password|pass|passwd|pwd|token|secret|apikey|api_key|auth)=)([^&#\s]*)/gi,
      "$1***",
    );
    return redacted;
  }
}

export function redactContext(ctx: ContextEntry): ContextEntry {
  return {
    ...ctx,
    url: redactUrl(ctx.url),
  };
}

function stripInlineYamlComment(raw: string): string {
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i];
    if (ch === "'" && !inDouble) {
      inSingle = !inSingle;
    } else if (ch === '"' && !inSingle && raw[i - 1] !== "\\") {
      inDouble = !inDouble;
    } else if (ch === "#" && !inSingle && !inDouble) {
      if (i === 0 || /\s/.test(raw[i - 1]!)) {
        return raw.slice(0, i);
      }
    }
  }
  return raw;
}

function unquoteYamlValue(raw: string): string {
  const trimmed = raw.trim();
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"') && trimmed.length >= 2) ||
    (trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length >= 2)
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function parseBool(raw: unknown, fieldName = "readOnly"): boolean | undefined {
  if (raw === undefined || raw === null) {
    return undefined;
  }
  if (typeof raw === "boolean") {
    return raw;
  }
  if (typeof raw === "string") {
    const lower = raw.trim().toLowerCase();
    if (lower === "") {
      return undefined;
    }
    if (lower === "true" || lower === "yes" || lower === "1") {
      return true;
    }
    if (lower === "false" || lower === "no" || lower === "0") {
      return false;
    }
    throw new Error(`Invalid boolean value "${raw}" for "${fieldName}" in contexts config.`);
  }
  throw new Error(`Invalid boolean value for "${fieldName}" in contexts config.`);
}

function normalizeBackend(raw: unknown): QueueBackendKind | undefined {
  if (raw === undefined || raw === null || raw === "") {
    return undefined;
  }
  if (typeof raw !== "string") {
    throw new Error(`Invalid backend in contexts config. Expected bullmq, river, or pg-boss.`);
  }
  const normalized = raw.trim().toLowerCase() as QueueBackendKind;
  if (!VALID_BACKENDS.has(normalized)) {
    throw new Error(`Invalid backend "${raw}" in contexts config. Expected bullmq, river, or pg-boss.`);
  }
  return normalized;
}

function normalizeEntry(name: string, obj: Record<string, unknown>): ContextEntry {
  const url = typeof obj.url === "string" ? obj.url.trim() : "";
  if (!url) {
    throw new Error(`Context "${name}" is missing a required "url" field.`);
  }

  return {
    name,
    url,
    backend: normalizeBackend(obj.backend),
    readOnly: parseBool(obj.readOnly ?? obj.read_only, "readOnly"),
    prefix: typeof obj.prefix === "string" && obj.prefix.trim() ? obj.prefix.trim() : undefined,
    schema: typeof obj.schema === "string" && obj.schema.trim() ? obj.schema.trim() : undefined,
  };
}

export function parseSimpleYamlContexts(content: string): ContextEntry[] {
  const lines = content.split(/\r?\n/);
  const entries = new Map<string, Record<string, string>>();

  let insideContextsRoot = false;
  let ignoreCurrentTopSection = false;
  let currentName: string | null = null;
  let contextIndent: number | null = null;

  for (const rawLine of lines) {
    const withoutComment = stripInlineYamlComment(rawLine);
    if (withoutComment.trim().length === 0) {
      continue;
    }

    const indent = rawLine.length - rawLine.trimStart().length;
    const trimmed = withoutComment.trim();

    if (indent === 0 && trimmed === "contexts:") {
      insideContextsRoot = true;
      ignoreCurrentTopSection = false;
      currentName = null;
      contextIndent = null;
      continue;
    }

    if (indent === 0 && insideContextsRoot) {
      insideContextsRoot = false;
      ignoreCurrentTopSection = true;
      currentName = null;
      contextIndent = null;
      continue;
    }

    if (ignoreCurrentTopSection) {
      continue;
    }

    const colonIdx = trimmed.indexOf(":");
    if (colonIdx === -1) {
      continue;
    }

    const key = unquoteYamlValue(trimmed.slice(0, colonIdx));
    const value = unquoteYamlValue(trimmed.slice(colonIdx + 1));

    if (value === "") {
      if (!insideContextsRoot && indent === 0) {
        currentName = key;
        contextIndent = 0;
        if (!entries.has(currentName)) {
          entries.set(currentName, {});
        }
      } else if (insideContextsRoot && (contextIndent === null || indent === contextIndent)) {
        currentName = key;
        contextIndent = indent;
        if (!entries.has(currentName)) {
          entries.set(currentName, {});
        }
      }
      continue;
    }

    if (currentName !== null && contextIndent !== null && indent > contextIndent) {
      const target = entries.get(currentName);
      if (target) {
        target[key] = value;
      }
    }
  }

  const result: ContextEntry[] = [];
  for (const [name, fields] of entries) {
    result.push(normalizeEntry(name, fields));
  }
  return result;
}

export function parseJsonContexts(content: string): ContextEntry[] {
  const parsed = JSON.parse(content) as unknown;
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Contexts JSON must be an object.");
  }

  const root = parsed as Record<string, unknown>;
  const source = "contexts" in root ? root.contexts : root;

  if (Array.isArray(source)) {
    return source.map((item, idx) => {
      if (!item || typeof item !== "object") {
        throw new Error(`Invalid context entry at index ${idx}.`);
      }
      const rec = item as Record<string, unknown>;
      const name = typeof rec.name === "string" ? rec.name.trim() : "";
      if (!name) {
        throw new Error(`Context entry at index ${idx} is missing "name".`);
      }
      return normalizeEntry(name, rec);
    });
  }

  if (source && typeof source === "object") {
    const entries: ContextEntry[] = [];
    for (const [name, value] of Object.entries(source as Record<string, unknown>)) {
      if (!value || typeof value !== "object") {
        throw new Error(`Invalid context definition for "${name}".`);
      }
      entries.push(normalizeEntry(name, value as Record<string, unknown>));
    }
    return entries;
  }

  throw new Error("Invalid contexts structure in config file.");
}

export function getDefaultConfigPaths(): string[] {
  const envPath = process.env.Q9S_CONFIG;
  if (envPath && envPath.trim().length > 0) {
    return [path.resolve(envPath.trim())];
  }
  const baseDir = path.join(os.homedir(), ".config", "q9s");
  return [
    path.join(baseDir, "contexts.json"),
    path.join(baseDir, "contexts.yaml"),
    path.join(baseDir, "contexts.yml"),
  ];
}

export function findConfigFile(explicitPath?: string): string | null {
  if (explicitPath) {
    const resolved = path.resolve(explicitPath);
    if (!fs.existsSync(resolved)) {
      throw new Error(`Contexts config file not found: ${resolved}`);
    }
    return resolved;
  }

  for (const candidate of getDefaultConfigPaths()) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

export function loadContexts(explicitPath?: string): ContextEntry[] {
  const filePath = findConfigFile(explicitPath);
  if (!filePath) {
    return [];
  }

  const content = fs.readFileSync(filePath, "utf8");
  const lowerPath = filePath.toLowerCase();
  if (lowerPath.endsWith(".yaml") || lowerPath.endsWith(".yml")) {
    return parseSimpleYamlContexts(content);
  }
  return parseJsonContexts(content);
}

export function resolveContext(name: string, explicitPath?: string): ContextEntry {
  const contexts = loadContexts(explicitPath);
  const match = contexts.find((ctx) => ctx.name === name);
  if (!match) {
    const available = contexts.map((c) => c.name).join(", ");
    const hint = available
      ? `Available contexts: ${available}`
      : `Create ${path.join(os.homedir(), ".config", "q9s", "contexts.json")} to define named contexts.`;
    throw new Error(`Context "${name}" not found. ${hint}`);
  }
  return match;
}
