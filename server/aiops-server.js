#!/usr/bin/env node
/**
 * aiops-server — minimal central log store for aiops-agent.
 *
 * Receives gzip NDJSON batches from agents, authenticates API keys, stores
 * logs in SQLite, and exposes a search API plus per-agent remote config.
 *
 * Zero runtime dependencies — requires Node.js >= 22 (built-in node:sqlite).
 * Run with:  node --experimental-sqlite aiops-server.js
 * (the flag is unnecessary on Node >= 23)
 *
 * Environment:
 *   AIOPS_PORT       listen port                  (default 8480)
 *   AIOPS_DB         SQLite path                  (default ./aiops.db)
 *   AIOPS_API_KEYS   comma-separated valid keys   (default: generated on first run,
 *                    printed to stdout and stored in <db dir>/api-keys.txt)
 */
'use strict';

const http = require('http');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');

const PORT = parseInt(process.env.AIOPS_PORT || '8480', 10);
const DB_PATH = process.env.AIOPS_DB || path.join(process.cwd(), 'aiops.db');

// ---------------------------------------------------------------------------
// API keys: from env, or generated once and persisted next to the DB
// ---------------------------------------------------------------------------
function loadApiKeys() {
  if (process.env.AIOPS_API_KEYS) {
    return new Set(process.env.AIOPS_API_KEYS.split(',').map((s) => s.trim()).filter(Boolean));
  }
  const keyFile = path.join(path.dirname(path.resolve(DB_PATH)), 'api-keys.txt');
  if (fs.existsSync(keyFile)) {
    return new Set(fs.readFileSync(keyFile, 'utf8').split('\n').map((s) => s.trim()).filter(Boolean));
  }
  const key = 'sk_live_' + crypto.randomBytes(24).toString('hex');
  fs.writeFileSync(keyFile, key + '\n', { mode: 0o600 });
  console.log(`[aiops-server] generated API key: ${key}`);
  console.log(`[aiops-server] (stored in ${keyFile}; set AIOPS_API_KEYS to override)`);
  return new Set([key]);
}
const API_KEYS = loadApiKeys();
const REVOKED_KEYS = new Set(); // populate via DELETE /v1/keys/:key

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------
const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT NOT NULL,
    received_at TEXT NOT NULL,
    agent_id    TEXT,
    hostname    TEXT,
    environment TEXT,
    source      TEXT,
    service     TEXT,
    container   TEXT,
    level       TEXT,
    git_sha     TEXT,
    message     TEXT,
    meta        TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
  CREATE INDEX IF NOT EXISTS idx_logs_service   ON logs(service, timestamp);
  CREATE INDEX IF NOT EXISTS idx_logs_level     ON logs(level, timestamp);
  CREATE INDEX IF NOT EXISTS idx_logs_hostname  ON logs(hostname, timestamp);
  CREATE TABLE IF NOT EXISTS agents (
    agent_id     TEXT PRIMARY KEY,
    hostname     TEXT,
    environment  TEXT,
    version      TEXT,
    platform     TEXT,
    registered_at TEXT,
    last_seen_at TEXT
  );
  CREATE TABLE IF NOT EXISTS agent_configs (
    agent_id TEXT PRIMARY KEY,
    version  TEXT NOT NULL,
    config   TEXT NOT NULL
  );
`);

const insertLog = db.prepare(`
  INSERT INTO logs (timestamp, received_at, agent_id, hostname, environment,
                    source, service, container, level, git_sha, message, meta)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);
const upsertAgent = db.prepare(`
  INSERT INTO agents (agent_id, hostname, environment, version, platform, registered_at, last_seen_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(agent_id) DO UPDATE SET
    hostname = excluded.hostname, environment = excluded.environment,
    version = excluded.version, platform = excluded.platform,
    last_seen_at = excluded.last_seen_at
`);
const touchAgent = db.prepare(`UPDATE agents SET last_seen_at = ? WHERE agent_id = ?`);

const KNOWN_FIELDS = new Set([
  'timestamp', 'agent_id', 'hostname', 'environment', 'source',
  'service', 'container', 'level', 'git_sha', 'message',
]);

function storeBatch(events, agentIdHeader) {
  const now = new Date().toISOString();
  let stored = 0;
  db.exec('BEGIN');
  try {
    for (const e of events) {
      if (!e || typeof e !== 'object' || typeof e.message !== 'string') continue;
      const meta = {};
      for (const [k, v] of Object.entries(e)) if (!KNOWN_FIELDS.has(k)) meta[k] = v;
      insertLog.run(
        e.timestamp || now, now,
        e.agent_id || agentIdHeader || null,
        e.hostname || null, e.environment || null,
        e.source || null, e.service || null, e.container || null,
        e.level || null, e.git_sha || null,
        e.message,
        Object.keys(meta).length ? JSON.stringify(meta) : null,
      );
      stored++;
    }
    db.exec('COMMIT');
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }
  if (agentIdHeader) touchAgent.run(now, agentIdHeader);
  return stored;
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 50 * 1024 * 1024) { req.destroy(); reject(new Error('body too large')); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      let buf = Buffer.concat(chunks);
      if (req.headers['content-encoding'] === 'gzip') {
        try { buf = zlib.gunzipSync(buf); } catch (e) { return reject(new Error('bad gzip')); }
      }
      resolve(buf);
    });
    req.on('error', reject);
  });
}

function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(body);
}

function authenticate(req) {
  const auth = req.headers.authorization || '';
  const key = auth.replace(/^Bearer\s+/i, '').trim();
  if (!key || REVOKED_KEYS.has(key)) return null;
  return API_KEYS.has(key) ? key : null;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const route = `${req.method} ${url.pathname}`;

  try {
    if (route === 'GET /health') return json(res, 200, { ok: true, version: '0.1.0' });

    if (!authenticate(req)) {
      return json(res, 401, { error: 'invalid or missing API key' });
    }

    // --- ingestion -------------------------------------------------------
    if (route === 'POST /v1/ingest') {
      const body = await readBody(req);
      let events;
      if ((req.headers['content-type'] || '').includes('ndjson')) {
        events = body.toString('utf8').split('\n').filter(Boolean).map((l) => {
          try { return JSON.parse(l); } catch { return null; }
        }).filter(Boolean);
      } else {
        const parsed = JSON.parse(body.toString('utf8'));
        events = Array.isArray(parsed) ? parsed : parsed.events || [];
      }
      if (!events.length) return json(res, 400, { error: 'no events' });
      const stored = storeBatch(events, req.headers['x-agent-id']);
      return json(res, 200, { stored });
    }

    // --- agent lifecycle -------------------------------------------------
    if (route === 'POST /v1/agents/register') {
      const body = JSON.parse((await readBody(req)).toString('utf8') || '{}');
      const agentId = body.agent_id || crypto.randomUUID();
      const now = new Date().toISOString();
      upsertAgent.run(agentId, body.hostname || null, body.environment || null,
        body.version || null, body.platform || null, now, now);
      return json(res, 200, { agent_id: agentId });
    }

    if (route === 'GET /v1/agents') {
      const rows = db.prepare('SELECT * FROM agents ORDER BY last_seen_at DESC').all();
      return json(res, 200, { agents: rows });
    }

    // --- remote config ---------------------------------------------------
    let m = url.pathname.match(/^\/v1\/agents\/([\w-]+)\/config$/);
    if (m && req.method === 'GET') {
      const row = db.prepare('SELECT version, config FROM agent_configs WHERE agent_id = ?').get(m[1]);
      if (!row) return json(res, 200, { version: null, config: {} });
      return json(res, 200, { version: row.version, config: JSON.parse(row.config) });
    }
    if (m && req.method === 'PUT') {
      const body = JSON.parse((await readBody(req)).toString('utf8'));
      const version = crypto.randomBytes(8).toString('hex');
      db.prepare(`
        INSERT INTO agent_configs (agent_id, version, config) VALUES (?, ?, ?)
        ON CONFLICT(agent_id) DO UPDATE SET version = excluded.version, config = excluded.config
      `).run(m[1], version, JSON.stringify(body));
      return json(res, 200, { version });
    }

    // --- search ----------------------------------------------------------
    if (route === 'GET /v1/logs') {
      const q = url.searchParams;
      const where = [];
      const params = [];
      const eq = { level: 'level', service: 'service', hostname: 'hostname',
                   source: 'source', environment: 'environment', container: 'container' };
      for (const [param, col] of Object.entries(eq)) {
        const v = q.get(param);
        if (v) { where.push(`${col} = ?`); params.push(v); }
      }
      if (q.get('q')) { where.push('message LIKE ?'); params.push(`%${q.get('q')}%`); }
      if (q.get('since')) { where.push('timestamp >= ?'); params.push(q.get('since')); }
      if (q.get('until')) { where.push('timestamp <= ?'); params.push(q.get('until')); }
      const limit = Math.min(parseInt(q.get('limit') || '100', 10), 1000);
      const sql = 'SELECT * FROM logs' +
        (where.length ? ' WHERE ' + where.join(' AND ') : '') +
        ' ORDER BY timestamp DESC LIMIT ?';
      params.push(limit);
      const rows = db.prepare(sql).all(...params);
      for (const r of rows) if (r.meta) try { r.meta = JSON.parse(r.meta); } catch { }
      return json(res, 200, { count: rows.length, logs: rows });
    }

    if (route === 'GET /v1/stats') {
      const total = db.prepare('SELECT COUNT(*) AS n FROM logs').get().n;
      const byLevel = db.prepare(
        'SELECT level, COUNT(*) AS n FROM logs GROUP BY level ORDER BY n DESC').all();
      const byService = db.prepare(
        'SELECT service, COUNT(*) AS n FROM logs GROUP BY service ORDER BY n DESC LIMIT 20').all();
      return json(res, 200, { total, by_level: byLevel, by_service: byService });
    }

    return json(res, 404, { error: 'not found' });
  } catch (err) {
    return json(res, err.message === 'bad gzip' ? 400 : 500, { error: err.message });
  }
});

server.listen(PORT, () => {
  console.log(`[aiops-server] listening on :${PORT}, db=${DB_PATH}`);
});

process.on('SIGTERM', () => { server.close(); db.close(); process.exit(0); });
process.on('SIGINT', () => { server.close(); db.close(); process.exit(0); });
