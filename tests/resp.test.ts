import net from "node:net";
import { describe, expect, it } from "vitest";
import {
  encodeRespCommand,
  parseRedisUrl,
  parseRespFrame,
  RespClient,
  RespError,
  RespParser,
  RespTimeoutError,
} from "../src/clients/resp.js";
import { startMockRedisServer } from "../src/demo/fixture.js";

describe("RESP2 parser and encoder", () => {
  it("encodes commands with exact UTF-8 byte lengths", () => {
    const encoded = encodeRespCommand(["SET", "k:emoji", "hi 🚀"]);
    const expectedPayload = Buffer.from("hi 🚀", "utf8");
    expect(encoded.toString("utf8")).toBe(
      `*3\r\n$3\r\nSET\r\n$7\r\nk:emoji\r\n$${expectedPayload.length}\r\nhi 🚀\r\n`,
    );
  });

  it("parses simple strings, errors, integers, bulk strings, nulls, and nested arrays", () => {
    expect(parseRespFrame(Buffer.from("+PONG\r\n"))?.value).toBe("PONG");
    const errFrame = parseRespFrame(Buffer.from("-ERR unknown command\r\n"))?.value;
    expect(errFrame).toBeInstanceOf(RespError);
    expect((errFrame as RespError).message).toBe("ERR unknown command");

    expect(parseRespFrame(Buffer.from(":42\r\n"))?.value).toBe(42);
    expect(parseRespFrame(Buffer.from("$-1\r\n"))?.value).toBeNull();
    expect(parseRespFrame(Buffer.from("*-1\r\n"))?.value).toBeNull();
    expect(parseRespFrame(Buffer.from("$11\r\nhello\r\nworl\r\n"))?.value).toBe("hello\r\nworl");

    const nested = parseRespFrame(
      Buffer.from("*2\r\n$1\r\n0\r\n*2\r\n$14\r\nbull:mail:meta\r\n$18\r\nbull:webhooks:meta\r\n"),
    );
    expect(nested?.value).toEqual(["0", ["bull:mail:meta", "bull:webhooks:meta"]]);
  });

  it("reassembles frames split byte-by-byte across TCP chunks", () => {
    const parser = new RespParser();
    const full = Buffer.from("+OK\r\n:17\r\n$6\r\n🚀ok\r\n", "utf8");
    const frames: unknown[] = [];

    for (let i = 0; i < full.length; i++) {
      frames.push(...parser.push(full.subarray(i, i + 1)));
    }

    expect(frames).toEqual(["OK", 17, "🚀ok"]);
    expect(parser.bufferedBytes).toBe(0);
  });

  it("parses redis:// and rediss:// URLs accurately", () => {
    const plain = parseRedisUrl("redis://default:s3cr%40t@redis.local:6380/2");
    expect(plain).toMatchObject({
      host: "redis.local",
      port: 6380,
      username: "default",
      password: "s3cr@t",
      db: 2,
      tls: false,
    });

    const secure = parseRedisUrl("rediss://cache.example.com");
    expect(secure).toMatchObject({
      host: "cache.example.com",
      port: 6379,
      db: 0,
      tls: true,
      servername: "cache.example.com",
    });

    expect(() => parseRedisUrl("http://localhost:6379")).toThrow(/Unsupported Redis protocol/);
  });
});

describe("RespClient live TCP integration", () => {
  it("communicates with an in-process mock Redis server including AUTH, SELECT, and errors", async () => {
    const server = await startMockRedisServer({ password: "secret-pass", seedBullMqDemo: false });
    try {
      const unauthed = await RespClient.connect(`redis://127.0.0.1:${server.port}`, {
        timeoutMs: 1000,
      });
      await expect(unauthed.ping()).rejects.toThrow(/NOAUTH/);
      await unauthed.close();

      const client = await RespClient.connect(
        `redis://:secret-pass@127.0.0.1:${server.port}/3`,
      );
      expect(await client.ping()).toBe("PONG");
      expect(await client.command("SET", "q9s:test", "hello")).toBe("OK");
      expect(await client.command("GET", "q9s:test")).toBe("hello");
      await expect(client.command("NO_SUCH_CMD")).rejects.toBeInstanceOf(RespError);
      // Connection remains usable after server error
      expect(await client.ping()).toBe("PONG");
      await client.close();
    } finally {
      await server.close();
    }
  });

  it("enforces command timeouts cleanly", async () => {
    const accepted = new Set<net.Socket>();
    const silentServer = net.createServer((sock) => {
      accepted.add(sock);
      sock.on("close", () => accepted.delete(sock));
      // Intentionally never reply to test timeout
    });
    await new Promise<void>((resolve) => silentServer.listen(0, "127.0.0.1", () => resolve()));
    const addr = silentServer.address() as net.AddressInfo;

    const client = await RespClient.connect(`redis://127.0.0.1:${addr.port}`, { timeoutMs: 150 });
    try {
      await expect(client.ping()).rejects.toBeInstanceOf(RespTimeoutError);
    } finally {
      await client.close();
      for (const sock of accepted) {
        sock.destroy();
      }
      await new Promise<void>((resolve) => silentServer.close(() => resolve()));
    }
  });
});
