# q9s

**`k9s` for background job queues.** A zero-dependency Node.js CLI for inspecting BullMQ (Redis), River (Postgres), and pg-boss (Postgres) queues and dead-letter queues (DLQs) safely from your terminal.

> [!IMPORTANT]
> **Read-only by default (`--read-only`).** `q9s` refuses all mutations unless you explicitly pass `--allow-write`. Bulk retries additionally require `--yes`. Passwords in Redis/Postgres URLs and named contexts are always redacted in terminal and JSON output.

## 15-second demo

[![Q9S inspects a failed webhook job in read-only mode](./docs/media/q9s-poster.png)](./docs/media/q9s-demo.mp4)

[Open the demo video (MP4)](./docs/media/q9s-demo.mp4)

## Try it in 1 second

Run the self-contained demo (starts a live in-process mock BullMQ Redis TCP server over localhost, talks real RESP2, auto-discovers 3 queues, renders sparklines and the spiking `webhooks` DLQ stack trace, and shuts down cleanly):

```bash
npx q9s --demo
```

Machine-readable JSON mode:

```bash
npx q9s --demo --json
```

Inspect a specific queue:

```bash
npx q9s --demo --inspect billing-sync
```

## Architecture

```mermaid
flowchart LR
    CLI["q9s CLI<br/>(--demo / URL / --ctx)"] --> Guard["Safety Gate<br/>--read-only default<br/>--allow-write + --yes"]
    Guard --> Bull["BullMqBackend<br/>src/backends/bullmq.ts"]
    Guard --> Pg["PostgresQueueBackend<br/>src/backends/postgres.ts"]
    Bull --> Resp["Pure-TS RESP2 Client<br/>node:net / node:tls"]
    Resp --> Redis[("Redis / Valkey<br/>bull:*:meta, wait, failed")]
    Pg --> Psql["psql CLI (execFileSync)<br/>PGPASSWORD + read-only txn"]
    Psql --> Postgres[("PostgreSQL<br/>river_job / pgboss.job")]
    Bull --> Render["Dashboard & Sparkline Renderer<br/>Terminal UI / --json"]
    Pg --> Render
```

## Features

- **Zero runtime npm dependencies**: pure TypeScript over Node.js 24 built-ins (`node:net`, `node:tls`, `node:child_process`). No native addons, no heavy web frameworks.
- **BullMQ over raw RESP2**: connects directly over `redis://` or `rediss://`, supports `AUTH` and `SELECT`, auto-discovers `bull:*:meta` queues, reads `LLEN`/`ZCARD` counts and `HGETALL` job payloads, and executes atomic `MULTI`/`EXEC` retries.
- **River & pg-boss over safe `psql`**: auto-detects `river_job` vs `pgboss.job`, isolates credentials via `PGPASSWORD` env vars (never in process args or logs), enforces `default_transaction_read_only=on` for reads, and validates queue names and job IDs strictly before querying.
- **DLQ Inspector**: renders pretty-printed JSON job payloads, attempt counters, failure reasons, and stack traces alongside Unicode throughput sparklines (`▁▂▃▄▅▆▇█`).
- **Named contexts**: switch between environments using `~/.config/q9s/contexts.json` or `~/.config/q9s/contexts.yaml` with automatic credential redaction and per-context `readOnly: true` pinning.

## Honest Comparison

| Capability | `q9s` (v0.1) | Bull Board | River UI | pg-boss Studio | `redis-cli` / `psql` |
| --- | --- | --- | --- | --- | --- |
| Interface | Terminal CLI + `--json` | Self-hosted Web UI | Self-hosted Web UI | Self-hosted Web UI | Raw REPL |
| Zero app integration required | Yes (point at Redis/PG) | No (mount middleware) | No (run Go server) | No (run web server) | Yes |
| Multi-backend (BullMQ + River + pg-boss) | Yes | BullMQ only | River only | pg-boss only | Manual queries |
| Read-only by default | Yes (`--read-only`) | Opt-in config | Opt-in config | Opt-in config | No |
| Runtime dependencies | 0 npm packages | Express/Fastify + UI | Go binary | Node + web stack | Native client |
| Interactive fullscreen TUI / live tail | Planned (v0.2+) | Web auto-refresh | Web auto-refresh | Web auto-refresh | Manual `watch` |
| Job creation / payload editing | Not in v0.1 | Yes | Limited | Yes | Manual |

## Installation

Requires **Node.js >= 24.0.0**. Postgres backends require `psql` on your `PATH`.

```bash
npm install -g q9s
```

Or run directly with `npx`:

```bash
npx q9s redis://localhost:6379
npx q9s postgres://app:secret@localhost:5432/mydb
```

## Named Contexts

Create `~/.config/q9s/contexts.json` (or `~/.config/q9s/contexts.yaml`):

```json
{
  "contexts": {
    "prod-bull": {
      "url": "rediss://default:supersecret@redis.internal:6379/0",
      "backend": "bullmq",
      "prefix": "bull",
      "readOnly": true
    },
    "staging-river": {
      "url": "postgres://app:supersecret@db.staging.internal:5432/app",
      "backend": "river",
      "schema": "public"
    },
    "staging-pgboss": {
      "url": "postgres://app:supersecret@db.staging.internal:5432/app",
      "backend": "pg-boss",
      "schema": "pgboss"
    }
  }
}
```

Or in simple YAML (`~/.config/q9s/contexts.yaml`):

```yaml
contexts:
  prod-bull:
    url: rediss://default:supersecret@redis.internal:6379/0
    backend: bullmq
    readOnly: true
```

Use contexts from the CLI:

```bash
# List contexts (credentials are always redacted)
q9s --contexts

# Inspect a named context
q9s --ctx prod-bull
```

## Safe Retry Workflows

By default, any mutation flag is refused with exit code `2`:

```bash
# Refused in default read-only mode:
q9s --demo --retry wh-4814 --queue webhooks
```

Unlock a single job retry explicitly:

```bash
q9s redis://localhost:6379 --queue webhooks --retry wh-4814 --allow-write
```

Unlock a bulk DLQ retry (requires both `--allow-write` and `--yes`):

```bash
q9s redis://localhost:6379 --retry-failed webhooks --limit 25 --allow-write --yes
```

Contexts marked `"readOnly": true` in config refuse `--allow-write` even if passed on the command line.

## CLI Reference

| Flag | Description |
| --- | --- |
| `<url>` | Target `redis://`, `rediss://`, `postgres://`, or `postgresql://` URL |
| `--demo` | Launch live in-process mock BullMQ Redis TCP server with realistic DLQ data |
| `--ctx <name>` | Load named context from `~/.config/q9s/contexts.json` or `.yaml` |
| `--contexts` | Print configured contexts with redacted credentials |
| `--config <path>` | Explicit contexts file path (or set `Q9S_CONFIG`) |
| `--backend <kind>` | Override auto-detection: `bullmq`, `river`, or `pg-boss` |
| `--prefix <prefix>` | BullMQ Redis key prefix (default: `bull`) |
| `--schema <schema>` | Postgres schema override (default: auto-detected) |
| `--timeout <ms>` | Connection/query timeout in ms (default: `5000`) |
| `--inspect <queue>` | Inspect jobs in a specific queue (default: queue with most failed jobs) |
| `--state <state>` | Job state: `waiting`, `active`, `delayed`, `completed`, `failed` (default: `failed`) |
| `--limit <n>` | Max jobs to inspect (default: `3`) or bulk-retry (default: `100`) |
| `--read-only` | Enforce read-only mode (default: `true`) |
| `--allow-write` | Explicitly allow `--retry` and `--retry-failed` mutations |
| `--retry <jobId>` | Retry one failed job (requires `--queue`/`--inspect` and `--allow-write`) |
| `--queue <queue>` | Target queue for `--retry` |
| `--retry-failed <queue>` | Bulk retry failed jobs in `<queue>` (requires `--allow-write` and `--yes`) |
| `-y`, `--yes` | Confirm bulk write action |
| `--json` | Emit structured JSON |
| `--no-color` | Disable ANSI colors |
| `-h`, `--help` | Show CLI usage |
| `-v`, `--version` | Show version |

## Development

```bash
npm install
npm run typecheck
npm run build
npm test
```

## License

MIT. See [LICENSE](./LICENSE).
