# aiops-agent

A standalone, zero-dependency log collection agent. Install it on any Linux
server with one command; it discovers Docker containers, systemd services, and
log files automatically, streams every log line, buffers locally when the
network is down, and ships batches to the storage backend you pick at install
time — the central aiops server, or **directly into your own PostgreSQL,
MySQL, or MongoDB database**.

```
┌────────────────────────── your server ──────────────────────────┐
│  Docker events + log streams ─┐                                 │      ┌► aiops-server ─► SQLite
│  systemd journal (journalctl) ─┼─► enrich ─► disk buffer ─► sink│──────┼► PostgreSQL / MySQL
│  file tails (/var/log/*) ──────┤   (host,     (survives          │      └► MongoDB / Atlas
│  host metrics ────────────────┘    service,    outages)          │
│                                    git_sha)                      │
└─────────────────────────────────────────────────────────────────┘
```

Both the agent and the server are **single JavaScript files with zero npm
dependencies** — the file is the bundle. Even the PostgreSQL, MySQL, and
MongoDB clients are built in (native wire protocols, SCRAM/TLS auth, DNS SRV
for `mongodb+srv://`). Agent needs Node ≥ 20; server needs Node ≥ 22
(built-in SQLite).

## Install the agent (on each server)

```bash
curl -fsSL https://github.com/yashmishra2006/aiops-agent/releases/latest/download/install.sh | sudo bash
```

The installer asks where logs should go:

```
Where should this agent send logs?

  1) aiops server   — central log store with search API (needs an API key)
  2) PostgreSQL     — write directly into your own database
  3) MySQL          — write directly into your own database
  4) MongoDB        — write directly into your own database
```

Option 1 asks for your endpoint + API key; options 2–4 ask for a connection
string (e.g. `postgresql://user:pass@host:5432/logs` or
`mongodb+srv://user:pass@cluster.mongodb.net/logs`). For unattended installs,
pass the answers as flags and nothing is asked:

```bash
# ship to an aiops server
curl -fsSL https://github.com/yashmishra2006/aiops-agent/releases/latest/download/install.sh | sudo bash -s -- \
  --api-key sk_live_xxx --endpoint https://logs.yourdomain.com

# or write straight into your own database
curl -fsSL https://github.com/yashmishra2006/aiops-agent/releases/latest/download/install.sh | sudo bash -s -- \
  --connection-string 'postgresql://user:pass@db.example.com:5432/logs'
```

Or from a local checkout / tarball: `sudo ./install.sh [flags]`.

The installer verifies the release checksum, installs to `/opt/aiops-agent`
(bringing its own Node runtime if the server has none), writes
`/etc/aiops-agent/config.yaml` with root-only `600` permissions, registers the
`aiops-agent` systemd service, and starts it.

## Direct database sinks

With a `sink.connection_string` configured, the agent skips the aiops server
entirely and batch-inserts events into your database, creating the schema on
first contact:

- **PostgreSQL** (`postgres://`, `postgresql://`) — table `aiops_logs`
  (`meta JSONB`), indexes on timestamp/service/level; creates the database
  itself if it doesn't exist. Supports SCRAM-SHA-256/md5 auth and TLS
  (`?sslmode=require|prefer|disable`).
- **MySQL** (`mysql://`) — same table shape (`meta JSON`), utf8mb4;
  `mysql_native_password` and `caching_sha2_password` auth, TLS when the
  server offers it.
- **MongoDB** (`mongodb://`, `mongodb+srv://`) — collection `aiops_logs`,
  one document per event with real BSON dates. SRV lookup, replica-set
  primary discovery, SCRAM-SHA-256/SHA-1 auth, TLS. Works with Atlas.

The disk buffer, batching, and retry/backoff behavior are identical in every
mode — if your database is down, logs queue locally and nothing is lost.
Table/collection name is configurable via `sink.table`.

```bash
systemctl status aiops-agent     # is it running?
journalctl -u aiops-agent -f     # agent's own diagnostics
```

## Run the server (central log store)

```bash
node --experimental-sqlite server/aiops-server.js
# or:
docker build -t aiops-server server/ && docker run -p 8480:8480 -v aiops-data:/data aiops-server
```

On first run it generates an API key, prints it, and stores it next to the DB.
Set `AIOPS_API_KEYS=key1,key2` to manage keys yourself. Put it behind TLS
(nginx/caddy/traefik) in production — the agent speaks HTTPS natively.

## Query your logs

```bash
KEY=sk_live_xxx
curl -H "Authorization: Bearer $KEY" 'http://localhost:8480/v1/logs?level=ERROR&limit=20'
curl -H "Authorization: Bearer $KEY" 'http://localhost:8480/v1/logs?service=backend&q=timeout'
curl -H "Authorization: Bearer $KEY" 'http://localhost:8480/v1/stats'
curl -H "Authorization: Bearer $KEY" 'http://localhost:8480/v1/agents'
```

Filters: `q` (substring), `level`, `service`, `hostname`, `container`,
`source` (docker | systemd | file | metrics | docker-events | agent),
`environment`, `since`/`until` (ISO timestamps), `limit`.

## Remote configuration

*(aiops server sink only — agents writing directly to a database are
configured through their local `/etc/aiops-agent/config.yaml`.)*

Change what an agent collects without touching the server it runs on:

```bash
curl -X PUT -H "Authorization: Bearer $KEY" -H 'content-type: application/json' \
  http://localhost:8480/v1/agents/<agent-id>/config \
  -d '{"collectors":{"files":{"paths":["/var/log/*.log","/var/log/nginx/*.log"]}}}'
```

The agent polls every 60 s, persists the override, and restarts its collectors.

## Log event shape

```json
{
  "timestamp": "2026-07-18T10:00:00.000Z",
  "hostname": "prod-api-1",
  "environment": "production",
  "source": "docker",
  "service": "backend",
  "container": "backend-3",
  "level": "ERROR",
  "git_sha": "d82b9e",
  "message": "connection timeout to db"
}
```

`service` and `git_sha` come from Docker labels (`com.docker.compose.service`,
`aiops.service`, `aiops.git_sha`, `org.opencontainers.image.revision`) so
deploys are correlatable with incidents.

## Reliability

Every event is appended to a disk-backed queue
(`/var/lib/aiops-agent/queue/`) before shipping. If the server is unreachable
the agent retries with exponential backoff and keeps collecting; the queue is
capped (200 MB default) and drops oldest-first only when full. Nothing is lost
in a temporary outage.

## Extending: collectors

Each source is a class with `start()` / `stop()` that calls `emit(event)`
(see `DockerCollector`, `JournaldCollector`, `FileCollector`,
`MetricsCollector` in [agent/aiops-agent.js](agent/aiops-agent.js)). Adding
nginx/postgres/redis support = adding another collector class and a config
block; the buffering, shipping, auth, and retry machinery is shared.

## Publishing a release

```bash
./scripts/release.sh
```

This builds `dist/aiops-agent-<version>.tar.gz` + checksum, tags the commit,
and creates a GitHub Release carrying the tarball, its checksum, and
`install.sh` itself — so the `releases/latest/download/install.sh` one-liner
always serves the current installer, resolves the newest release, verifies the
checksum, and installs. To self-host instead, run `./scripts/package.sh` and
point `AIOPS_DOWNLOAD_BASE` at wherever you upload the files.

## Layout

```
agent/aiops-agent.js          the agent daemon (single file, zero deps)
agent/config.example.yaml     agent config template
server/aiops-server.js        ingest + search server (single file, zero deps)
server/Dockerfile             containerized server
install.sh                    curl | bash installer
packaging/aiops-agent.service systemd unit (hardened)
scripts/package.sh            builds the release tarball + checksum
scripts/release.sh            tags + publishes a GitHub Release
```
