import net from "node:net";
import tls from "node:tls";

export type RespValue = string | number | null | RespValue[];

export class RespError extends Error {
  readonly code = "RESP_SERVER_ERROR";

  constructor(message: string) {
    super(message);
    this.name = "RespError";
  }
}

export class RespProtocolError extends Error {
  readonly code = "RESP_PROTOCOL_ERROR";

  constructor(message: string) {
    super(message);
    this.name = "RespProtocolError";
  }
}

export class RespTimeoutError extends Error {
  readonly code = "RESP_TIMEOUT";

  constructor(message: string) {
    super(message);
    this.name = "RespTimeoutError";
  }
}

export interface ParsedRedisUrl {
  host: string;
  port: number;
  username?: string;
  password?: string;
  db: number;
  tls: boolean;
  servername?: string;
}

export interface RespConnectOptions {
  timeoutMs?: number;
  rejectUnauthorized?: boolean;
}

interface PendingCommand {
  resolve: (value: RespValue) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

const CRLF = Buffer.from("\r\n", "utf8");

export function parseRedisUrl(rawUrl: string): ParsedRedisUrl {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error(`Invalid Redis URL: ${rawUrl}`);
  }

  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Unsupported Redis protocol "${parsed.protocol}". Expected redis:// or rediss://`);
  }

  const host = parsed.hostname || "127.0.0.1";
  const port = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`Invalid Redis port: ${parsed.port}`);
  }

  const username = parsed.username ? decodeURIComponent(parsed.username) : undefined;
  const password = parsed.password ? decodeURIComponent(parsed.password) : undefined;

  let db = 0;
  const trimmedPath = parsed.pathname.replace(/^\/+/, "");
  if (trimmedPath.length > 0) {
    const parsedDb = Number.parseInt(trimmedPath, 10);
    if (!Number.isInteger(parsedDb) || parsedDb < 0) {
      throw new Error(`Invalid Redis database index: ${trimmedPath}`);
    }
    db = parsedDb;
  }

  const useTls = parsed.protocol === "rediss:";
  return {
    host,
    port,
    username,
    password,
    db,
    tls: useTls,
    servername: useTls && net.isIP(host) === 0 ? host : undefined,
  };
}

export function encodeRespCommand(args: ReadonlyArray<string | number | Buffer>): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`, "utf8")];
  for (const arg of args) {
    const buf = Buffer.isBuffer(arg) ? arg : Buffer.from(String(arg), "utf8");
    parts.push(Buffer.from(`$${buf.length}\r\n`, "utf8"), buf, CRLF);
  }
  return Buffer.concat(parts);
}

export interface ParsedFrame {
  value: RespValue | RespError;
  nextOffset: number;
}

function findCrlf(buf: Buffer, offset: number): number {
  for (let i = offset; i < buf.length - 1; i++) {
    if (buf[i] === 0x0d && buf[i + 1] === 0x0a) {
      return i;
    }
  }
  return -1;
}

export function parseRespFrame(buf: Buffer, offset = 0): ParsedFrame | null {
  if (offset >= buf.length) {
    return null;
  }

  const prefix = buf[offset];
  const lineEnd = findCrlf(buf, offset + 1);
  if (lineEnd === -1) {
    return null;
  }

  const line = buf.toString("utf8", offset + 1, lineEnd);
  const afterLine = lineEnd + 2;

  switch (prefix) {
    case 0x2b: // '+' Simple String
      return { value: line, nextOffset: afterLine };

    case 0x2d: // '-' Error
      return { value: new RespError(line), nextOffset: afterLine };

    case 0x3a: { // ':' Integer
      const num = Number.parseInt(line, 10);
      if (Number.isNaN(num)) {
        throw new RespProtocolError(`Invalid RESP integer: "${line}"`);
      }
      return { value: num, nextOffset: afterLine };
    }

    case 0x24: { // '$' Bulk String
      const byteLength = Number.parseInt(line, 10);
      if (Number.isNaN(byteLength)) {
        throw new RespProtocolError(`Invalid RESP bulk length: "${line}"`);
      }
      if (byteLength === -1) {
        return { value: null, nextOffset: afterLine };
      }
      if (byteLength < 0) {
        throw new RespProtocolError(`Invalid negative RESP bulk length: ${byteLength}`);
      }
      const payloadEnd = afterLine + byteLength;
      if (buf.length < payloadEnd + 2) {
        return null;
      }
      if (buf[payloadEnd] !== 0x0d || buf[payloadEnd + 1] !== 0x0a) {
        throw new RespProtocolError("Missing CRLF after RESP bulk string payload");
      }
      const text = buf.toString("utf8", afterLine, payloadEnd);
      return { value: text, nextOffset: payloadEnd + 2 };
    }

    case 0x2a: { // '*' Array
      const count = Number.parseInt(line, 10);
      if (Number.isNaN(count)) {
        throw new RespProtocolError(`Invalid RESP array length: "${line}"`);
      }
      if (count === -1) {
        return { value: null, nextOffset: afterLine };
      }
      if (count < 0) {
        throw new RespProtocolError(`Invalid negative RESP array length: ${count}`);
      }
      const items: RespValue[] = [];
      let cursor = afterLine;
      for (let i = 0; i < count; i++) {
        const elem = parseRespFrame(buf, cursor);
        if (!elem) {
          return null;
        }
        if (elem.value instanceof RespError) {
          items.push(elem.value.message);
        } else {
          items.push(elem.value);
        }
        cursor = elem.nextOffset;
      }
      return { value: items, nextOffset: cursor };
    }

    default:
      throw new RespProtocolError(
        `Unexpected RESP2 type byte: 0x${(prefix ?? 0).toString(16)}`,
      );
  }
}

export class RespParser {
  private buffer: Buffer = Buffer.alloc(0);

  push(chunk: Buffer): Array<RespValue | RespError> {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    const frames: Array<RespValue | RespError> = [];

    let offset = 0;
    while (offset < this.buffer.length) {
      const parsed = parseRespFrame(this.buffer, offset);
      if (!parsed) {
        break;
      }
      frames.push(parsed.value);
      offset = parsed.nextOffset;
    }

    if (offset > 0) {
      this.buffer = offset === this.buffer.length ? Buffer.alloc(0) : this.buffer.subarray(offset);
    }

    return frames;
  }

  get bufferedBytes(): number {
    return this.buffer.length;
  }
}

export class RespClient {
  private readonly socket: net.Socket;
  private readonly parser = new RespParser();
  private readonly pending: PendingCommand[] = [];
  private readonly timeoutMs: number;
  private closed = false;

  private constructor(socket: net.Socket, timeoutMs: number) {
    this.socket = socket;
    this.timeoutMs = timeoutMs;

    this.socket.on("data", (chunk: Buffer) => {
      try {
        const frames = this.parser.push(chunk);
        for (const frame of frames) {
          const item = this.pending.shift();
          if (!item) {
            continue;
          }
          clearTimeout(item.timer);
          if (frame instanceof RespError) {
            item.reject(frame);
          } else {
            item.resolve(frame);
          }
        }
      } catch (err) {
        const protocolErr = err instanceof Error ? err : new Error(String(err));
        this.failAllPending(protocolErr);
        this.socket.destroy(protocolErr);
      }
    });

    this.socket.on("error", (err: Error) => {
      this.failAllPending(err);
    });

    this.socket.on("close", () => {
      this.closed = true;
      this.failAllPending(new Error("Redis connection closed"));
    });
  }

  static async connect(
    target: string | ParsedRedisUrl,
    options: RespConnectOptions = {},
  ): Promise<RespClient> {
    const config = typeof target === "string" ? parseRedisUrl(target) : target;
    const timeoutMs = options.timeoutMs ?? 5000;

    const socket = await new Promise<net.Socket>((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (!settled) {
          settled = true;
          sock.destroy();
          reject(
            new RespTimeoutError(
              `Timed out connecting to Redis at ${config.host}:${config.port} after ${timeoutMs}ms`,
            ),
          );
        }
      }, timeoutMs);

      const onConnect = () => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          sock.setNoDelay(true);
          sock.removeListener("error", onError);
          resolve(sock);
        }
      };

      const onError = (err: Error) => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          reject(err);
        }
      };

      const sock: net.Socket = config.tls
        ? tls.connect({
            host: config.host,
            port: config.port,
            servername: config.servername,
            rejectUnauthorized: options.rejectUnauthorized ?? true,
          })
        : net.createConnection({
            host: config.host,
            port: config.port,
          });

      sock.once(config.tls ? "secureConnect" : "connect", onConnect);
      sock.once("error", onError);
    });

    const client = new RespClient(socket, timeoutMs);

    try {
      if (config.password !== undefined) {
        if (config.username !== undefined && config.username.length > 0) {
          await client.command("AUTH", config.username, config.password);
        } else {
          await client.command("AUTH", config.password);
        }
      }
      if (config.db > 0) {
        await client.command("SELECT", config.db);
      }
    } catch (err) {
      await client.close();
      throw err;
    }

    return client;
  }

  command(...args: Array<string | number | Buffer>): Promise<RespValue> {
    if (this.closed || this.socket.destroyed) {
      return Promise.reject(new Error("Cannot send command on closed Redis connection"));
    }

    const encoded = encodeRespCommand(args);
    const cmdName = String(args[0] ?? "UNKNOWN");

    return new Promise<RespValue>((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.pending.indexOf(entry);
        if (idx !== -1) {
          this.pending.splice(idx, 1);
        }
        reject(new RespTimeoutError(`Redis command ${cmdName} timed out after ${this.timeoutMs}ms`));
      }, this.timeoutMs);

      const entry: PendingCommand = { resolve, reject, timer };
      this.pending.push(entry);

      this.socket.write(encoded, (err) => {
        if (err) {
          const idx = this.pending.indexOf(entry);
          if (idx !== -1) {
            this.pending.splice(idx, 1);
          }
          clearTimeout(timer);
          reject(err);
        }
      });
    });
  }

  async ping(): Promise<string> {
    const res = await this.command("PING");
    return String(res ?? "");
  }

  async scan(
    cursor: string,
    pattern?: string,
    count?: number,
  ): Promise<{ cursor: string; keys: string[] }> {
    const args: Array<string | number> = ["SCAN", cursor];
    if (pattern !== undefined) {
      args.push("MATCH", pattern);
    }
    if (count !== undefined) {
      args.push("COUNT", count);
    }
    const res = await this.command(...args);
    if (!Array.isArray(res) || res.length < 2) {
      throw new RespProtocolError("Unexpected SCAN response shape");
    }
    const nextCursor = String(res[0] ?? "0");
    const rawKeys = Array.isArray(res[1]) ? res[1] : [];
    const keys = rawKeys.map((k) => String(k ?? ""));
    return { cursor: nextCursor, keys };
  }

  async scanAll(pattern: string, count = 200): Promise<string[]> {
    const seen = new Set<string>();
    let cursor = "0";
    let iterations = 0;
    const maxIterations = 5000;

    do {
      const page = await this.scan(cursor, pattern, count);
      for (const key of page.keys) {
        seen.add(key);
      }
      cursor = page.cursor;
      iterations++;
    } while (cursor !== "0" && iterations < maxIterations);

    return Array.from(seen).sort();
  }

  async llen(key: string): Promise<number> {
    const res = await this.command("LLEN", key);
    return typeof res === "number" ? res : Number.parseInt(String(res ?? "0"), 10) || 0;
  }

  async zcard(key: string): Promise<number> {
    const res = await this.command("ZCARD", key);
    return typeof res === "number" ? res : Number.parseInt(String(res ?? "0"), 10) || 0;
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    const res = await this.command("LRANGE", key, start, stop);
    return Array.isArray(res) ? res.map((item) => String(item ?? "")) : [];
  }

  async zrange(key: string, start: number, stop: number): Promise<string[]> {
    const res = await this.command("ZRANGE", key, start, stop);
    return Array.isArray(res) ? res.map((item) => String(item ?? "")) : [];
  }

  async zrevrange(key: string, start: number, stop: number): Promise<string[]> {
    const res = await this.command("ZREVRANGE", key, start, stop);
    return Array.isArray(res) ? res.map((item) => String(item ?? "")) : [];
  }

  async hget(key: string, field: string): Promise<string | null> {
    const res = await this.command("HGET", key, field);
    return res === null ? null : String(res);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    const res = await this.command("HGETALL", key);
    const record: Record<string, string> = {};
    if (!Array.isArray(res)) {
      return record;
    }
    for (let i = 0; i + 1 < res.length; i += 2) {
      const field = String(res[i] ?? "");
      const value = String(res[i + 1] ?? "");
      record[field] = value;
    }
    return record;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;

    if (this.socket.destroyed) {
      this.failAllPending(new Error("Redis connection closed"));
      return;
    }

    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(forceTimer);
        resolve();
      };
      const forceTimer = setTimeout(() => {
        this.socket.destroy();
        resolve();
      }, 250);

      this.socket.once("close", done);
      this.socket.end();
    });
  }

  private failAllPending(err: Error): void {
    while (this.pending.length > 0) {
      const item = this.pending.shift();
      if (item) {
        clearTimeout(item.timer);
        item.reject(err);
      }
    }
  }
}
