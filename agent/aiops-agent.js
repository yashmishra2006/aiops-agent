#!/usr/bin/env node
/**
 * aiops-agent — standalone log collection daemon.
 *
 * Collects logs from Docker containers, the systemd journal, and plain files,
 * enriches them with host/service metadata, buffers them on disk, and ships
 * them in gzip-compressed batches to a central ingest server.
 *
 * Zero runtime dependencies — requires only Node.js >= 20.
 *
 * Usage:
 *   aiops-agent.js [--config /etc/aiops-agent/config.yaml] [--once] [--version]
 */
'use strict';

const fs = require('fs');
const os = require('os');
const net = require('net');
const tls = require('tls');
const dns = require('dns');
const path = require('path');
const http = require('http');
const https = require('https');
const zlib = require('zlib');
const crypto = require('crypto');
const { spawn, execFileSync } = require('child_process');

const VERSION = '0.2.0';

// ---------------------------------------------------------------------------
// Minimal YAML subset parser (nested maps, lists of scalars, scalar values).
// Enough for the agent config file; not a general YAML implementation.
// ---------------------------------------------------------------------------
function parseYaml(text) {
  const root = {};
  // stack of [indent, container]
  const stack = [[-1, root]];
  let pendingKey = null; // key waiting for a nested block
  const lines = text.split(/\r?\n/);
  for (const raw of lines) {
    const noComment = raw.replace(/(^|\s)#.*$/, '');
    if (!noComment.trim()) continue;
    const indent = noComment.match(/^ */)[0].length;
    const line = noComment.trim();

    while (stack.length > 1 && indent <= stack[stack.length - 1][0]) stack.pop();
    let container = stack[stack.length - 1][1];

    if (line.startsWith('- ')) {
      if (!Array.isArray(container)) {
        // convert pending empty map into a list
        const parent = stack[stack.length - 2];
        if (parent && pendingKey && parent[1][pendingKey] === container &&
            Object.keys(container).length === 0) {
          container = parent[1][pendingKey] = [];
          stack[stack.length - 1][1] = container;
        } else {
          continue; // malformed; skip
        }
      }
      container.push(parseScalar(line.slice(2).trim()));
      continue;
    }

    const m = line.match(/^([^:]+):\s*(.*)$/);
    if (!m) continue;
    const key = m[1].trim();
    const val = m[2].trim();
    if (val === '') {
      const child = {};
      container[key] = child;
      stack.push([indent, child]);
      pendingKey = key;
    } else {
      container[key] = parseScalar(val);
    }
  }
  return root;
}

function parseScalar(s) {
  if (/^(['"]).*\1$/.test(s)) return s.slice(1, -1);
  if (s === 'true') return true;
  if (s === 'false') return false;
  if (s === 'null' || s === '~') return null;
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  if (/^-?\d+\.\d+$/.test(s)) return parseFloat(s);
  return s;
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const DEFAULT_CONFIG = {
  endpoint: process.env.AIOPS_ENDPOINT || 'http://localhost:8480',
  api_key: process.env.AIOPS_API_KEY || '',
  environment: process.env.AIOPS_ENVIRONMENT || 'production',
  // Where logs are shipped. type: aiops (HTTP + API key, the default),
  // postgres, mysql, or mongodb (direct writes via connection_string).
  // An empty type is inferred from the connection string scheme.
  sink: {
    type: '',
    connection_string: process.env.AIOPS_CONNECTION_STRING || '',
    table: 'aiops_logs',   // SQL table / MongoDB collection name
  },
  hostname: '',                 // defaults to os.hostname()
  state_dir: '/var/lib/aiops-agent',
  collectors: {
    docker: { enabled: true, socket: '/var/run/docker.sock' },
    journald: { enabled: true, units: [] }, // empty = all units
    files: {
      enabled: true,
      paths: ['/var/log/*.log', '/var/log/syslog', '/opt/*/logs/*.log'],
    },
    metrics: { enabled: true, interval_seconds: 60 },
  },
  buffer: {
    max_bytes: 200 * 1024 * 1024, // cap disk usage; oldest segments dropped
    segment_bytes: 1024 * 1024,
  },
  ship: {
    batch_size: 500,
    flush_interval_seconds: 3,
    remote_config_interval_seconds: 60,
  },
};

function deepMerge(base, extra) {
  const out = Array.isArray(base) ? base.slice() : { ...base };
  if (!extra || typeof extra !== 'object') return out;
  for (const [k, v] of Object.entries(extra)) {
    if (v && typeof v === 'object' && !Array.isArray(v) &&
        out[k] && typeof out[k] === 'object' && !Array.isArray(out[k])) {
      out[k] = deepMerge(out[k], v);
    } else {
      out[k] = v;
    }
  }
  return out;
}

function loadConfig(configPath) {
  let cfg = DEFAULT_CONFIG;
  if (configPath && fs.existsSync(configPath)) {
    cfg = deepMerge(cfg, parseYaml(fs.readFileSync(configPath, 'utf8')));
  }
  // Remote config overrides, persisted by the config poller.
  const remotePath = path.join(cfg.state_dir, 'config.remote.json');
  try {
    if (fs.existsSync(remotePath)) {
      cfg = deepMerge(cfg, JSON.parse(fs.readFileSync(remotePath, 'utf8')).config || {});
    }
  } catch { /* corrupt remote config: ignore, base config still applies */ }
  if (!cfg.hostname) cfg.hostname = os.hostname();
  if (!cfg.sink.type) {
    cfg.sink.type = cfg.sink.connection_string
      ? inferSinkType(cfg.sink.connection_string) : 'aiops';
  }
  return cfg;
}

function inferSinkType(cs) {
  if (/^mongodb(\+srv)?:/i.test(cs)) return 'mongodb';
  if (/^postgres(ql)?:/i.test(cs)) return 'postgres';
  if (/^mysql:/i.test(cs)) return 'mysql';
  throw new Error(`cannot infer sink type from connection string: ${cs.split(':')[0]}://…`);
}

// ---------------------------------------------------------------------------
// Logging (the agent's own diagnostics, to stderr/journal)
// ---------------------------------------------------------------------------
function log(level, msg, extra) {
  const line = `[${new Date().toISOString()}] ${level.toUpperCase()} ${msg}` +
    (extra ? ` ${JSON.stringify(extra)}` : '');
  process.stderr.write(line + '\n');
}

// ---------------------------------------------------------------------------
// Level detection
// ---------------------------------------------------------------------------
const LEVEL_RE = /\b(FATAL|CRITICAL|CRIT|ERROR|ERR|WARNING|WARN|INFO|DEBUG|TRACE)\b/i;
const LEVEL_MAP = {
  FATAL: 'FATAL', CRITICAL: 'FATAL', CRIT: 'FATAL',
  ERROR: 'ERROR', ERR: 'ERROR',
  WARNING: 'WARN', WARN: 'WARN',
  INFO: 'INFO', DEBUG: 'DEBUG', TRACE: 'DEBUG',
};
const JOURNAL_PRIORITY = { 0: 'FATAL', 1: 'FATAL', 2: 'FATAL', 3: 'ERROR', 4: 'WARN', 5: 'INFO', 6: 'INFO', 7: 'DEBUG' };

function detectLevel(message, fallback) {
  const m = LEVEL_RE.exec(message);
  return m ? LEVEL_MAP[m[1].toUpperCase()] : (fallback || 'INFO');
}

// ---------------------------------------------------------------------------
// Disk-backed buffer queue (NDJSON segment files)
// ---------------------------------------------------------------------------
class BufferQueue {
  constructor(dir, { maxBytes, segmentBytes }) {
    this.dir = dir;
    this.maxBytes = maxBytes;
    this.segmentBytes = segmentBytes;
    this.currentPath = null;
    this.currentSize = 0;
    this.seq = 0;
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    // clear empty segments left over from seal() rotations in a previous run
    for (const seg of this.segments()) {
      try { if (fs.statSync(seg).size === 0) fs.unlinkSync(seg); } catch { }
    }
  }

  push(event) {
    const line = JSON.stringify(event) + '\n';
    if (!this.currentPath || this.currentSize + line.length > this.segmentBytes) {
      this._rotate();
    }
    fs.appendFileSync(this.currentPath, line);
    this.currentSize += Buffer.byteLength(line);
  }

  _rotate() {
    this.currentPath = path.join(
      this.dir, `${Date.now()}-${String(this.seq++).padStart(6, '0')}.ndjson`);
    this.currentSize = 0;
    fs.writeFileSync(this.currentPath, '');
    this._enforceCap();
  }

  segments() {
    return fs.readdirSync(this.dir)
      .filter((f) => f.endsWith('.ndjson'))
      .sort()
      .map((f) => path.join(this.dir, f));
  }

  oldestReadable() {
    for (const seg of this.segments()) {
      try {
        if (fs.statSync(seg).size > 0) return seg;
      } catch { /* removed concurrently */ }
    }
    return null;
  }

  // Before shipping the active segment, rotate so collectors append elsewhere.
  // The shipped segment is then immutable: no appends can race with commit().
  seal(segPath) {
    if (segPath === this.currentPath) this._rotate();
  }

  readEvents(segPath, limit) {
    const lines = fs.readFileSync(segPath, 'utf8').split('\n').filter(Boolean);
    const events = [];
    for (const l of lines.slice(0, limit)) {
      try { events.push(JSON.parse(l)); } catch { /* skip corrupt line */ }
    }
    return { events, total: lines.length, lines };
  }

  // After a successful ship, drop the shipped prefix of the (sealed) segment.
  commit(segPath, shippedCount, allLines) {
    if (shippedCount >= allLines.length) {
      fs.unlinkSync(segPath);
    } else {
      fs.writeFileSync(segPath, allLines.slice(shippedCount).join('\n') + '\n');
    }
  }

  _enforceCap() {
    let total = 0;
    const segs = this.segments().map((p) => {
      const size = (() => { try { return fs.statSync(p).size; } catch { return 0; } })();
      total += size;
      return { p, size };
    });
    while (total > this.maxBytes && segs.length > 1) {
      const victim = segs.shift(); // oldest
      try {
        fs.unlinkSync(victim.p);
        total -= victim.size;
        log('warn', 'buffer cap exceeded, dropped oldest segment', { segment: victim.p });
      } catch { break; }
    }
  }
}

// ---------------------------------------------------------------------------
// HTTP client (endpoint + API key), gzip POST, JSON GET
// ---------------------------------------------------------------------------
class ApiClient {
  constructor(cfg) {
    this.endpoint = cfg.endpoint.replace(/\/+$/, '');
    this.apiKey = cfg.api_key;
    this.agentId = null;
  }

  _request(method, urlPath, body, headers = {}) {
    return new Promise((resolve, reject) => {
      const url = new URL(this.endpoint + urlPath);
      const mod = url.protocol === 'https:' ? https : http;
      const req = mod.request(url, {
        method,
        headers: {
          authorization: `Bearer ${this.apiKey}`,
          'user-agent': `aiops-agent/${VERSION}`,
          ...(this.agentId ? { 'x-agent-id': this.agentId } : {}),
          ...headers,
        },
        timeout: 30000,
      }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          let json = null;
          try { json = JSON.parse(text); } catch { /* non-JSON body */ }
          resolve({ status: res.statusCode, json, text });
        });
      });
      req.on('timeout', () => req.destroy(new Error('request timeout')));
      req.on('error', reject);
      if (body) req.write(body);
      req.end();
    });
  }

  postGzipNdjson(urlPath, events) {
    const ndjson = events.map((e) => JSON.stringify(e)).join('\n');
    const gz = zlib.gzipSync(Buffer.from(ndjson));
    return this._request('POST', urlPath, gz, {
      'content-type': 'application/x-ndjson',
      'content-encoding': 'gzip',
    });
  }

  postJson(urlPath, obj) {
    return this._request('POST', urlPath, JSON.stringify(obj), {
      'content-type': 'application/json',
    });
  }

  getJson(urlPath) {
    return this._request('GET', urlPath);
  }
}

// ---------------------------------------------------------------------------
// Sinks — where batches of events are shipped.
//   AiopsSink    HTTP + API key to an aiops-server (default)
//   PostgresSink direct inserts over the Postgres wire protocol
//   MySQLSink    direct inserts over the MySQL protocol
//   MongoSink    direct inserts via OP_MSG (mongodb:// and mongodb+srv://)
// All are zero-dependency implementations on net/tls/dns/crypto.
// ---------------------------------------------------------------------------
class SinkError extends Error {
  // kind: 'transient' (retry w/ backoff) | 'auth' (bad credentials/config,
  // long backoff) | 'drop' (poisoned batch, discard instead of blocking queue)
  constructor(message, kind) { super(message); this.kind = kind || 'transient'; }
}

const SINK_FIELDS = new Set([
  'timestamp', 'agent_id', 'hostname', 'environment', 'source',
  'service', 'container', 'level', 'git_sha', 'message',
]);

function splitMeta(e) {
  const meta = {};
  for (const [k, v] of Object.entries(e)) if (!SINK_FIELDS.has(k)) meta[k] = v;
  return Object.keys(meta).length ? meta : null;
}

function safeIdent(name, fallback) {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name || '') ? name : fallback;
}

class AiopsSink {
  constructor(client) { this.client = client; }
  async send(events) {
    let res;
    try { res = await this.client.postGzipNdjson('/v1/ingest', events); }
    catch (err) { throw new SinkError(err.message, 'transient'); }
    if (res.status >= 200 && res.status < 300) return;
    if (res.status === 401 || res.status === 403) {
      throw new SinkError('server rejected API key; check config', 'auth');
    }
    if (res.status === 400) throw new SinkError('server rejected batch (400)', 'drop');
    throw new SinkError(`server returned ${res.status}`, 'transient');
  }
  close() { }
}

// --- shared low-level helpers ----------------------------------------------

// Promise-based reader over a socket: read(n) resolves once n bytes arrived.
class StreamReader {
  constructor(sock) {
    this.buf = Buffer.alloc(0);
    this.err = null;
    this.waiter = null;
    sock.on('data', (c) => { this.buf = Buffer.concat([this.buf, c]); this._pump(); });
    const fail = (e) => { if (!this.err) this.err = e || new Error('connection closed'); this._pump(); };
    sock.on('error', fail);
    sock.on('close', () => fail(this.err));
  }
  _pump() {
    if (!this.waiter) return;
    if (this.buf.length >= this.waiter.n) {
      const w = this.waiter; this.waiter = null;
      const out = this.buf.subarray(0, w.n);
      this.buf = this.buf.subarray(w.n);
      w.resolve(out);
    } else if (this.err) {
      const w = this.waiter; this.waiter = null;
      w.reject(this.err);
    }
  }
  read(n) {
    return new Promise((resolve, reject) => {
      this.waiter = { n, resolve, reject };
      this._pump();
    });
  }
}

function cstr(s) { return Buffer.concat([Buffer.from(String(s), 'utf8'), Buffer.from([0])]); }

function tcpConnect(host, port) {
  return new Promise((resolve, reject) => {
    const s = net.connect({ host, port }, () => { s.removeListener('error', reject); resolve(s); });
    s.once('error', reject);
    s.setKeepAlive(true, 30000);
  });
}

function tlsUpgrade(sock, host, rejectUnauthorized) {
  return new Promise((resolve, reject) => {
    const t = tls.connect({ socket: sock, servername: host, rejectUnauthorized }, () => {
      t.removeListener('error', reject); resolve(t);
    });
    t.once('error', reject);
  });
}

// RFC 5802 SCRAM client (SHA-256 for Postgres/modern Mongo, SHA-1 for old Mongo)
function scramClient(username, password, hashName) {
  const keyLen = hashName === 'sha256' ? 32 : 20;
  const nonce = crypto.randomBytes(18).toString('base64');
  const user = String(username).replace(/=/g, '=3D').replace(/,/g, '=2C');
  const clientFirstBare = `n=${user},r=${nonce}`;
  return {
    clientFirst: 'n,,' + clientFirstBare,
    handleServerFirst(serverFirst) {
      const attrs = {};
      for (const part of serverFirst.split(',')) attrs[part[0]] = part.slice(2);
      if (!attrs.r || !attrs.r.startsWith(nonce)) throw new Error('SCRAM nonce mismatch');
      const salted = crypto.pbkdf2Sync(password, Buffer.from(attrs.s, 'base64'),
        parseInt(attrs.i, 10), keyLen, hashName);
      const clientKey = crypto.createHmac(hashName, salted).update('Client Key').digest();
      const storedKey = crypto.createHash(hashName).update(clientKey).digest();
      const withoutProof = `c=biws,r=${attrs.r}`;
      const authMessage = `${clientFirstBare},${serverFirst},${withoutProof}`;
      const clientSig = crypto.createHmac(hashName, storedKey).update(authMessage).digest();
      const proof = Buffer.from(clientKey.map((b, i) => b ^ clientSig[i])).toString('base64');
      const serverKey = crypto.createHmac(hashName, salted).update('Server Key').digest();
      const serverSig = crypto.createHmac(hashName, serverKey).update(authMessage).digest().toString('base64');
      return { clientFinal: `${withoutProof},p=${proof}`, serverSig };
    },
  };
}

// --- PostgreSQL -------------------------------------------------------------
class PostgresSink {
  constructor(cfg) {
    const u = new URL(cfg.sink.connection_string);
    this.host = u.hostname;
    this.port = parseInt(u.port, 10) || 5432;
    this.user = decodeURIComponent(u.username) || 'postgres';
    this.password = decodeURIComponent(u.password || '');
    this.database = decodeURIComponent(u.pathname.replace(/^\//, '')) || this.user;
    this.sslmode = u.searchParams.get('sslmode') || 'prefer';
    this.table = safeIdent(cfg.sink.table, 'aiops_logs');
    this.sock = null;
    this.r = null;
  }

  _send(type, body) {
    const msg = Buffer.alloc(5 + body.length);
    msg.write(type, 0);
    msg.writeUInt32BE(4 + body.length, 1);
    body.copy(msg, 5);
    this.sock.write(msg);
  }

  async _readMsg() {
    const head = await this.r.read(5);
    const len = head.readUInt32BE(1);
    const body = len > 4 ? await this.r.read(len - 4) : Buffer.alloc(0);
    return { type: String.fromCharCode(head[0]), body };
  }

  _pgError(body) {
    const fields = {};
    let i = 0;
    while (i < body.length && body[i] !== 0) {
      const code = String.fromCharCode(body[i]);
      const end = body.indexOf(0, i + 1);
      fields[code] = body.toString('utf8', i + 1, end);
      i = end + 1;
    }
    const err = new Error(`postgres: ${fields.M || 'unknown error'}`);
    err.code = fields.C || '';
    return err;
  }

  async _connect(databaseOverride) {
    const database = databaseOverride || this.database;
    let sock = await tcpConnect(this.host, this.port);
    try {
      if (this.sslmode !== 'disable') {
        const sslReq = Buffer.alloc(8);
        sslReq.writeUInt32BE(8, 0);
        sslReq.writeUInt32BE(80877103, 4);
        sock.write(sslReq);
        const answer = await new Promise((resolve, reject) => {
          sock.once('data', resolve);
          sock.once('error', reject);
        });
        if (answer[0] === 0x53 /* 'S' */) {
          sock = await tlsUpgrade(sock, this.host, false);
        } else if (this.sslmode === 'require') {
          throw new Error('postgres server does not support TLS (sslmode=require)');
        }
      }
      this.sock = sock;
      this.r = new StreamReader(sock);

      const params = { user: this.user, database, application_name: 'aiops-agent', client_encoding: 'UTF8' };
      const paramBufs = [];
      for (const [k, v] of Object.entries(params)) paramBufs.push(cstr(k), cstr(v));
      const payload = Buffer.concat([Buffer.from([0, 3, 0, 0]), ...paramBufs, Buffer.from([0])]);
      const startup = Buffer.alloc(4 + payload.length);
      startup.writeUInt32BE(4 + payload.length, 0);
      payload.copy(startup, 4);
      sock.write(startup);

      let scram = null, expectServerSig = null;
      for (;;) {
        const { type, body } = await this._readMsg();
        if (type === 'R') {
          const code = body.readUInt32BE(0);
          if (code === 0) continue;                       // AuthenticationOk
          if (code === 3) {                               // cleartext
            this._send('p', cstr(this.password));
          } else if (code === 5) {                        // md5
            const inner = crypto.createHash('md5').update(this.password + this.user).digest('hex');
            const outer = crypto.createHash('md5')
              .update(Buffer.concat([Buffer.from(inner), body.subarray(4, 8)])).digest('hex');
            this._send('p', cstr('md5' + outer));
          } else if (code === 10) {                       // SASL
            const mechs = body.subarray(4).toString('utf8').split('\0').filter(Boolean);
            if (!mechs.includes('SCRAM-SHA-256')) {
              throw new Error(`unsupported SASL mechanisms: ${mechs.join(',')}`);
            }
            scram = scramClient('', this.password, 'sha256');
            const first = Buffer.from(scram.clientFirst, 'utf8');
            const lenBuf = Buffer.alloc(4);
            lenBuf.writeUInt32BE(first.length, 0);
            this._send('p', Buffer.concat([cstr('SCRAM-SHA-256'), lenBuf, first]));
          } else if (code === 11) {                       // SASL continue
            const out = scram.handleServerFirst(body.subarray(4).toString('utf8'));
            expectServerSig = out.serverSig;
            this._send('p', Buffer.from(out.clientFinal, 'utf8'));
          } else if (code === 12) {                       // SASL final
            const fin = body.subarray(4).toString('utf8');
            if (fin !== 'v=' + expectServerSig) throw new Error('SCRAM server verification failed');
          } else {
            throw new Error(`unsupported postgres auth method (${code})`);
          }
        } else if (type === 'E') {
          throw this._pgError(body);
        } else if (type === 'Z') {
          break;                                          // ReadyForQuery
        } // ignore S (ParameterStatus), K (BackendKeyData), N (Notice)
      }
    } catch (err) {
      try { sock.destroy(); } catch { }
      this.sock = null;
      throw err;
    }
  }

  async _query(sql) {
    this._send('Q', cstr(sql));
    let error = null;
    for (;;) {
      const { type, body } = await this._readMsg();
      if (type === 'E') error = this._pgError(body);
      else if (type === 'Z') {
        if (error) throw error;
        return;
      } // ignore C/T/D/I/N/S
    }
  }

  async _ensureConnected() {
    if (this.sock && !this.sock.destroyed) return;
    try {
      await this._connect();
    } catch (err) {
      // Database missing? Create it via the maintenance db, then reconnect.
      if (String(err.code).startsWith('3D')) {
        await this._connect('postgres');
        const dbIdent = safeIdent(this.database, null);
        if (!dbIdent) throw err;
        await this._query(`CREATE DATABASE ${dbIdent}`);
        this.sock.destroy();
        this.sock = null;
        await this._connect();
      } else {
        throw err;
      }
    }
    await this._query(
      `CREATE TABLE IF NOT EXISTS ${this.table} (
         id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
         timestamp TIMESTAMPTZ, received_at TIMESTAMPTZ,
         agent_id TEXT, hostname TEXT, environment TEXT, source TEXT,
         service TEXT, container TEXT, level TEXT, git_sha TEXT,
         message TEXT, meta JSONB);
       CREATE INDEX IF NOT EXISTS ${this.table}_ts_idx ON ${this.table} (timestamp);
       CREATE INDEX IF NOT EXISTS ${this.table}_service_idx ON ${this.table} (service, timestamp);
       CREATE INDEX IF NOT EXISTS ${this.table}_level_idx ON ${this.table} (level, timestamp)`);
  }

  _lit(v) {
    if (v === null || v === undefined) return 'NULL';
    return "'" + String(v).replace(/\0/g, '').replace(/'/g, "''") + "'";
  }

  async send(events) {
    try {
      await this._ensureConnected();
      const now = new Date().toISOString();
      const rows = events.map((e) => {
        const meta = splitMeta(e);
        return '(' + [
          this._lit(e.timestamp || now), this._lit(now),
          this._lit(e.agent_id), this._lit(e.hostname), this._lit(e.environment),
          this._lit(e.source), this._lit(e.service), this._lit(e.container),
          this._lit(e.level), this._lit(e.git_sha), this._lit(e.message),
          this._lit(meta ? JSON.stringify(meta) : null),
        ].join(',') + ')';
      });
      await this._query(
        `INSERT INTO ${this.table} (timestamp, received_at, agent_id, hostname, environment,` +
        ` source, service, container, level, git_sha, message, meta) VALUES ${rows.join(',')}`);
    } catch (err) {
      this.close();
      const cls = String(err.code || '').slice(0, 2);
      if (cls === '28' || cls === '3D') throw new SinkError(err.message, 'auth');
      if (cls === '22' || cls === '23' || cls === '54') throw new SinkError(err.message, 'drop');
      throw new SinkError(err.message, 'transient');
    }
  }

  close() {
    if (this.sock) try { this.sock.destroy(); } catch { }
    this.sock = null;
  }
}

// --- MySQL ------------------------------------------------------------------
class MySQLSink {
  constructor(cfg) {
    const u = new URL(cfg.sink.connection_string);
    this.host = u.hostname;
    this.port = parseInt(u.port, 10) || 3306;
    this.user = decodeURIComponent(u.username) || 'root';
    this.password = decodeURIComponent(u.password || '');
    this.database = safeIdent(decodeURIComponent(u.pathname.replace(/^\//, '')), 'aiops');
    const sslParam = u.searchParams.get('ssl') || u.searchParams.get('tls') ||
      u.searchParams.get('sslmode') || u.searchParams.get('ssl-mode') || '';
    this.sslDisabled = ['false', '0', 'disable', 'disabled'].includes(sslParam.toLowerCase());
    this.table = safeIdent(cfg.sink.table, 'aiops_logs');
    this.sock = null;
    this.r = null;
    this.seq = 0;
  }

  async _readPacket() {
    const head = await this.r.read(4);
    this.seq = head[3];
    return this.r.read(head.readUIntLE(0, 3));
  }

  _writePacket(body) {
    const head = Buffer.alloc(4);
    head.writeUIntLE(body.length, 0, 3);
    head[3] = (this.seq + 1) & 0xff;
    this.seq = head[3];
    this.sock.write(Buffer.concat([head, body]));
  }

  _mysqlError(p) {
    // ERR packet: 0xFF, int16 code, '#' + 5-char sqlstate, message
    const code = p.readUInt16LE(1);
    let off = 3;
    if (p[off] === 0x23 /* '#' */) off += 6;
    const err = new Error(`mysql: ${p.toString('utf8', off)} (${code})`);
    err.code = code;
    return err;
  }

  _nativeToken(scramble) {
    if (!this.password) return Buffer.alloc(0);
    const sha1 = (b) => crypto.createHash('sha1').update(b).digest();
    const stage1 = sha1(Buffer.from(this.password, 'utf8'));
    const stage2 = sha1(Buffer.concat([scramble, sha1(stage1)]));
    return Buffer.from(stage1.map((b, i) => b ^ stage2[i]));
  }

  _sha2Token(scramble) {
    if (!this.password) return Buffer.alloc(0);
    const sha256 = (b) => crypto.createHash('sha256').update(b).digest();
    const p1 = sha256(Buffer.from(this.password, 'utf8'));
    const p2 = sha256(Buffer.concat([sha256(sha256(Buffer.from(this.password, 'utf8'))), scramble]));
    return Buffer.from(p1.map((b, i) => b ^ p2[i]));
  }

  _authToken(plugin, scramble) {
    if (plugin === 'caching_sha2_password') return this._sha2Token(scramble);
    return this._nativeToken(scramble); // mysql_native_password
  }

  async _connect() {
    this.sock = await tcpConnect(this.host, this.port);
    this.r = new StreamReader(this.sock);
    const hs = await this._readPacket();
    if (hs[0] === 0xff) throw this._mysqlError(hs);
    if (hs[0] !== 10) throw new Error(`unsupported mysql protocol version ${hs[0]}`);

    let i = hs.indexOf(0, 1) + 1;      // skip server version cstring
    i += 4;                            // thread id
    const auth1 = hs.subarray(i, i + 8); i += 8 + 1;
    const capLow = hs.readUInt16LE(i); i += 2;
    i += 1 + 2;                        // charset, status
    const capHigh = hs.readUInt16LE(i); i += 2;
    const authLen = hs[i]; i += 1 + 10;
    const serverCaps = capLow | (capHigh << 16);
    let auth2 = Buffer.alloc(0);
    if (serverCaps & 0x8000) {         // CLIENT_SECURE_CONNECTION
      const n = Math.max(13, authLen - 8);
      auth2 = hs.subarray(i, i + n); i += n;
    }
    let plugin = 'mysql_native_password';
    if (serverCaps & 0x80000) {        // CLIENT_PLUGIN_AUTH
      const z = hs.indexOf(0, i);
      plugin = hs.toString('utf8', i, z < 0 ? hs.length : z);
    }
    let scramble = Buffer.concat([auth1, auth2]).subarray(0, 20);

    const useTls = !this.sslDisabled && !!(serverCaps & 0x800); // CLIENT_SSL
    let caps = 0x1 | 0x200 | 0x2000 | 0x8000 | 0x80000 | 0x10000; // LONG_PASSWORD|PROTOCOL_41|TRANSACTIONS|SECURE_CONNECTION|PLUGIN_AUTH|MULTI_STATEMENTS
    if (useTls) caps |= 0x800;

    const prefix = Buffer.alloc(32);
    prefix.writeUInt32LE(caps >>> 0, 0);
    prefix.writeUInt32LE(0x1000000, 4);  // max packet
    prefix[8] = 45;                      // utf8mb4
    if (useTls) {
      this._writePacket(prefix);         // SSLRequest
      this.sock = await tlsUpgrade(this.sock, this.host, false);
      this.r = new StreamReader(this.sock);
    }
    const token = this._authToken(plugin, scramble);
    this._writePacket(Buffer.concat([
      prefix, cstr(this.user), Buffer.from([token.length]), token, cstr(plugin),
    ]));

    for (;;) {
      const p = await this._readPacket();
      if (p[0] === 0x00) break;                    // OK
      if (p[0] === 0xff) throw this._mysqlError(p);
      if (p[0] === 0xfe) {                         // AuthSwitchRequest
        const z = p.indexOf(0, 1);
        plugin = p.toString('utf8', 1, z);
        scramble = p.subarray(z + 1, z + 21);
        this._writePacket(this._authToken(plugin, scramble));
      } else if (p[0] === 0x01) {                  // AuthMoreData (caching_sha2)
        if (p[1] === 0x03) continue;               // fast auth ok; OK packet next
        if (p[1] === 0x04) {                       // full auth needed
          if (useTls) {
            this._writePacket(cstr(this.password));
          } else {
            this._writePacket(Buffer.from([0x02])); // request server RSA key
            const keyPkt = await this._readPacket();
            const pem = keyPkt.subarray(1).toString('utf8');
            const pw = Buffer.from(this.password + '\0', 'utf8');
            const xored = Buffer.from(pw.map((b, j) => b ^ scramble[j % scramble.length]));
            this._writePacket(crypto.publicEncrypt(
              { key: pem, padding: crypto.constants.RSA_PKCS1_OAEP_PADDING }, xored));
          }
        }
      } else {
        throw new Error(`unexpected mysql auth packet 0x${p[0].toString(16)}`);
      }
    }
  }

  async _query(sql) {
    this.seq = -1; // COM packets restart sequencing at 0
    this._writePacket(Buffer.concat([Buffer.from([0x03]), Buffer.from(sql, 'utf8')]));
    const p = await this._readPacket();
    if (p[0] === 0xff) throw this._mysqlError(p);
    // OK (0x00) for DDL/INSERT; anything else is unexpected for our statements
  }

  async _ensureConnected() {
    if (this.sock && !this.sock.destroyed) return;
    await this._connect();
    await this._query(`CREATE DATABASE IF NOT EXISTS ${this.database}`);
    await this._query(
      `CREATE TABLE IF NOT EXISTS ${this.database}.${this.table} (
         id BIGINT AUTO_INCREMENT PRIMARY KEY,
         timestamp DATETIME(3), received_at DATETIME(3),
         agent_id VARCHAR(64), hostname VARCHAR(255), environment VARCHAR(64),
         source VARCHAR(32), service VARCHAR(255), container VARCHAR(255),
         level VARCHAR(16), git_sha VARCHAR(64),
         message MEDIUMTEXT, meta JSON,
         INDEX idx_ts (timestamp), INDEX idx_service (service, timestamp),
         INDEX idx_level (level, timestamp)
       ) CHARACTER SET utf8mb4`);
  }

  _lit(v) {
    if (v === null || v === undefined) return 'NULL';
    return "'" + String(v).replace(/\0/g, '')
      .replace(/\\/g, '\\\\').replace(/'/g, "\\'") + "'";
  }

  _datetime(iso) {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString().replace('T', ' ').replace('Z', '');
  }

  async send(events) {
    try {
      await this._ensureConnected();
      const now = new Date().toISOString();
      const rows = events.map((e) => {
        const meta = splitMeta(e);
        return '(' + [
          this._lit(this._datetime(e.timestamp || now)), this._lit(this._datetime(now)),
          this._lit(e.agent_id), this._lit(e.hostname), this._lit(e.environment),
          this._lit(e.source), this._lit(e.service), this._lit(e.container),
          this._lit(e.level), this._lit(e.git_sha), this._lit(e.message),
          this._lit(meta ? JSON.stringify(meta) : null),
        ].join(',') + ')';
      });
      await this._query(
        `INSERT INTO ${this.database}.${this.table} (timestamp, received_at, agent_id,` +
        ` hostname, environment, source, service, container, level, git_sha, message, meta)` +
        ` VALUES ${rows.join(',')}`);
    } catch (err) {
      this.close();
      if ([1044, 1045, 1049, 1698, 2003].includes(err.code)) {
        throw new SinkError(err.message, 'auth');
      }
      if ([1048, 1054, 1064, 1366, 1406, 3140].includes(err.code)) {
        throw new SinkError(err.message, 'drop');
      }
      throw new SinkError(err.message, 'transient');
    }
  }

  close() {
    if (this.sock) try { this.sock.destroy(); } catch { }
    this.sock = null;
  }
}

// --- MongoDB ----------------------------------------------------------------
// Minimal BSON encode/decode for the document shapes we produce and the
// server replies we read (numbers, strings, bools, dates, binary, docs).
function bsonEncode(obj) {
  const parts = [];
  const int32 = (n) => { const b = Buffer.alloc(4); b.writeInt32LE(n, 0); return b; };
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined) continue;
    const name = cstr(k);
    if (v === null) parts.push(Buffer.from([10]), name);
    else if (typeof v === 'string') {
      const s = Buffer.from(v, 'utf8');
      parts.push(Buffer.from([2]), name, int32(s.length + 1), s, Buffer.from([0]));
    } else if (typeof v === 'boolean') {
      parts.push(Buffer.from([8]), name, Buffer.from([v ? 1 : 0]));
    } else if (typeof v === 'number') {
      if (Number.isInteger(v) && Math.abs(v) <= 0x7fffffff) {
        parts.push(Buffer.from([16]), name, int32(v));
      } else {
        const b = Buffer.alloc(8); b.writeDoubleLE(v, 0);
        parts.push(Buffer.from([1]), name, b);
      }
    } else if (v instanceof Date) {
      const b = Buffer.alloc(8); b.writeBigInt64LE(BigInt(v.getTime()), 0);
      parts.push(Buffer.from([9]), name, b);
    } else if (Buffer.isBuffer(v)) {
      parts.push(Buffer.from([5]), name, int32(v.length), Buffer.from([0]), v);
    } else if (Array.isArray(v)) {
      const doc = bsonEncode(Object.fromEntries(v.map((x, i) => [String(i), x])));
      parts.push(Buffer.from([4]), name, doc);
    } else if (typeof v === 'object') {
      parts.push(Buffer.from([3]), name, bsonEncode(v));
    }
  }
  const inner = Buffer.concat(parts);
  const out = Buffer.alloc(4 + inner.length + 1);
  out.writeInt32LE(out.length, 0);
  inner.copy(out, 4);
  return out;
}

function bsonDecode(buf, start = 0) {
  const len = buf.readInt32LE(start);
  const out = {};
  let i = start + 4;
  const end = start + len - 1;
  while (i < end) {
    const type = buf[i++];
    const z = buf.indexOf(0, i);
    const key = buf.toString('utf8', i, z);
    i = z + 1;
    switch (type) {
      case 1: out[key] = buf.readDoubleLE(i); i += 8; break;
      case 2: { const sl = buf.readInt32LE(i); out[key] = buf.toString('utf8', i + 4, i + 4 + sl - 1); i += 4 + sl; break; }
      case 3: { const dl = buf.readInt32LE(i); out[key] = bsonDecode(buf, i); i += dl; break; }
      case 4: { const dl = buf.readInt32LE(i); out[key] = Object.values(bsonDecode(buf, i)); i += dl; break; }
      case 5: { const bl = buf.readInt32LE(i); out[key] = Buffer.from(buf.subarray(i + 5, i + 5 + bl)); i += 5 + bl; break; }
      case 6: case 10: out[key] = null; break;
      case 7: out[key] = buf.subarray(i, i + 12).toString('hex'); i += 12; break;
      case 8: out[key] = buf[i++] === 1; break;
      case 9: out[key] = new Date(Number(buf.readBigInt64LE(i))); i += 8; break;
      case 11: { let z2 = buf.indexOf(0, i); z2 = buf.indexOf(0, z2 + 1); i = z2 + 1; out[key] = null; break; }
      case 16: out[key] = buf.readInt32LE(i); i += 4; break;
      case 17: case 18: out[key] = Number(buf.readBigInt64LE(i)); i += 8; break;
      case 19: out[key] = null; i += 16; break;
      default: throw new Error(`unsupported BSON type ${type} in server reply`);
    }
  }
  return out;
}

// mongodb:// URLs can hold comma-separated host lists, so URL() can't parse them.
function parseMongoUrl(cs) {
  const m = cs.match(/^(mongodb(?:\+srv)?):\/\/(?:([^:@/]*)(?::([^@/]*))?@)?([^/?]+)(?:\/([^?]*))?(?:\?(.*))?$/);
  if (!m) throw new Error('invalid mongodb connection string');
  const params = new URLSearchParams(m[6] || '');
  return {
    srv: m[1] === 'mongodb+srv',
    user: decodeURIComponent(m[2] || ''),
    password: decodeURIComponent(m[3] || ''),
    hosts: m[4].split(',').map((h) => {
      const [host, port] = h.split(':');
      return { host, port: parseInt(port, 10) || 27017 };
    }),
    db: decodeURIComponent(m[5] || '') || 'aiops',
    params,
  };
}

class MongoSink {
  constructor(cfg) {
    const u = parseMongoUrl(cfg.sink.connection_string);
    Object.assign(this, u);
    this.collection = safeIdent(cfg.sink.table, 'aiops_logs');
    const tlsParam = (u.params.get('tls') || u.params.get('ssl') || '').toLowerCase();
    this.tls = u.srv ? tlsParam !== 'false' : tlsParam === 'true';
    this.tlsInsecure = ['true', '1'].includes(
      (u.params.get('tlsAllowInvalidCertificates') || u.params.get('tlsInsecure') || '').toLowerCase());
    this.authSource = u.params.get('authSource') || 'admin';
    this.sock = null;
    this.r = null;
    this.reqId = 0;
  }

  async _cmd(doc) {
    const bson = bsonEncode(doc);
    const msg = Buffer.alloc(16 + 4 + 1 + bson.length);
    msg.writeInt32LE(msg.length, 0);
    msg.writeInt32LE(++this.reqId, 4);
    msg.writeInt32LE(0, 8);
    msg.writeInt32LE(2013, 12);        // OP_MSG
    msg.writeInt32LE(0, 16);           // flagBits
    msg[20] = 0;                       // section kind 0
    bson.copy(msg, 21);
    this.sock.write(msg);

    const head = await this.r.read(16);
    const body = await this.r.read(head.readInt32LE(0) - 16);
    const reply = bsonDecode(body, 5); // skip flagBits(4) + section kind(1)
    if (reply.ok !== 1) {
      const err = new Error(`mongodb: ${reply.errmsg || 'command failed'}`);
      err.code = reply.code;
      throw err;
    }
    return reply;
  }

  async _connectHost({ host, port }) {
    let sock = await tcpConnect(host, port);
    if (this.tls) sock = await tlsUpgrade(sock, host, !this.tlsInsecure ? true : false);
    this.sock = sock;
    this.r = new StreamReader(sock);

    const hello = {
      hello: 1, $db: 'admin',
      client: {
        application: { name: 'aiops-agent' },
        driver: { name: 'aiops-agent', version: VERSION },
        os: { type: os.platform() },
      },
    };
    if (this.user) hello.saslSupportedMechs = `${this.authSource}.${this.user}`;
    let helloReply;
    try {
      helloReply = await this._cmd(hello);
    } catch {
      helloReply = await this._cmd({ ismaster: 1, $db: 'admin' });
    }
    return helloReply;
  }

  async _auth(mechs) {
    let mech, hash, password;
    if (!mechs || mechs.includes('SCRAM-SHA-256')) {
      mech = 'SCRAM-SHA-256'; hash = 'sha256'; password = this.password;
    } else {
      mech = 'SCRAM-SHA-1'; hash = 'sha1';
      password = crypto.createHash('md5')
        .update(`${this.user}:mongo:${this.password}`).digest('hex');
    }
    const scram = scramClient(this.user, password, hash);
    let res = await this._cmd({
      saslStart: 1, mechanism: mech,
      payload: Buffer.from(scram.clientFirst, 'utf8'),
      options: { skipEmptyExchange: true },
      $db: this.authSource,
    });
    const out = scram.handleServerFirst(res.payload.toString('utf8'));
    res = await this._cmd({
      saslContinue: 1, conversationId: res.conversationId,
      payload: Buffer.from(out.clientFinal, 'utf8'),
      $db: this.authSource,
    });
    const final = res.payload.toString('utf8');
    if (!final.includes('v=' + out.serverSig)) {
      throw new Error('mongodb SCRAM server verification failed');
    }
    while (!res.done) {
      res = await this._cmd({
        saslContinue: 1, conversationId: res.conversationId,
        payload: Buffer.alloc(0), $db: this.authSource,
      });
    }
  }

  async _connect() {
    let hosts = this.hosts;
    if (this.srv) {
      const recs = await dns.promises.resolveSrv(`_mongodb._tcp.${this.hosts[0].host}`);
      hosts = recs.map((r) => ({ host: r.name, port: r.port }));
      try {
        const txt = await dns.promises.resolveTxt(this.hosts[0].host);
        const opts = new URLSearchParams(txt.flat().join(''));
        if (!this.params.has('authSource') && opts.get('authSource')) {
          this.authSource = opts.get('authSource');
        }
      } catch { /* no TXT options */ }
    }
    let lastErr = null;
    for (const h of hosts) {
      try {
        let hello = await this._connectHost(h);
        if (!hello.isWritablePrimary && !hello.ismaster && hello.primary) {
          this.close();
          const [ph, pp] = hello.primary.split(':');
          hello = await this._connectHost({ host: ph, port: parseInt(pp, 10) || 27017 });
        }
        if (this.user) {
          const mechs = hello.saslSupportedMechs;
          await this._auth(Array.isArray(mechs) ? mechs : null);
        }
        return;
      } catch (err) {
        lastErr = err;
        this.close();
      }
    }
    throw lastErr || new Error('mongodb: no reachable hosts');
  }

  async send(events) {
    try {
      if (!this.sock || this.sock.destroyed) await this._connect();
      const now = new Date();
      const res = await this._cmd({
        insert: this.collection, $db: this.db, ordered: false,
        documents: events.map((e) => ({
          ...e,
          timestamp: e.timestamp ? new Date(e.timestamp) : now,
          received_at: now,
        })),
      });
      if (res.writeErrors?.length) {
        log('warn', 'mongodb rejected some documents', {
          rejected: res.writeErrors.length, of: events.length,
        });
      }
    } catch (err) {
      this.close();
      if (err.code === 18 || err.code === 13) {  // AuthenticationFailed / Unauthorized
        throw new SinkError(err.message, 'auth');
      }
      throw new SinkError(err.message, 'transient');
    }
  }

  close() {
    if (this.sock) try { this.sock.destroy(); } catch { }
    this.sock = null;
  }
}

function createSink(cfg, apiClient) {
  switch (cfg.sink.type) {
    case 'aiops': return new AiopsSink(apiClient);
    case 'postgres': return new PostgresSink(cfg);
    case 'mysql': return new MySQLSink(cfg);
    case 'mongodb': return new MongoSink(cfg);
    default: throw new Error(`unknown sink type: ${cfg.sink.type}`);
  }
}

// ---------------------------------------------------------------------------
// Agent identity + persistent state
// ---------------------------------------------------------------------------
class AgentState {
  constructor(stateDir) {
    this.path = path.join(stateDir, 'state.json');
    fs.mkdirSync(stateDir, { recursive: true, mode: 0o700 });
    this.data = { agent_id: null, file_offsets: {}, remote_config_version: null };
    try {
      this.data = { ...this.data, ...JSON.parse(fs.readFileSync(this.path, 'utf8')) };
    } catch { /* first run */ }
    if (!this.data.agent_id) {
      this.data.agent_id = crypto.randomUUID();
      this.save();
    }
  }
  save() {
    const tmp = this.path + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(this.data, null, 2), { mode: 0o600 });
    fs.renameSync(tmp, this.path);
  }
}

// ---------------------------------------------------------------------------
// Docker collector: event stream + per-container log streams over the socket
// ---------------------------------------------------------------------------
class DockerCollector {
  constructor(cfg, emit) {
    this.socketPath = cfg.collectors.docker.socket;
    this.emit = emit;
    this.streams = new Map(); // container id -> http request
    this.stopped = false;
  }

  _dockerRequest(apiPath, onResponse) {
    const req = http.request({ socketPath: this.socketPath, path: apiPath, method: 'GET' }, onResponse);
    req.on('error', (err) => onResponse(null, err));
    req.end();
    return req;
  }

  _dockerJson(apiPath) {
    return new Promise((resolve, reject) => {
      this._dockerRequest(apiPath, (res, err) => {
        if (err) return reject(err);
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
          catch (e) { reject(e); }
        });
      });
    });
  }

  async start() {
    try {
      const containers = await this._dockerJson('/containers/json');
      for (const c of containers) this._attach(c.Id);
      this._streamEvents();
      log('info', 'docker collector started', { containers: containers.length });
    } catch (err) {
      if (!this.stopped) {
        log('warn', 'docker unavailable, retrying in 30s', { error: err.message });
        this._retryTimer = setTimeout(() => this.start(), 30000);
      }
    }
  }

  _streamEvents() {
    const filters = encodeURIComponent(JSON.stringify({ type: ['container'] }));
    this.eventsReq = this._dockerRequest(`/events?filters=${filters}`, (res, err) => {
      if (err || !res) {
        if (!this.stopped) this._retryTimer = setTimeout(() => this.start(), 10000);
        return;
      }
      let buf = '';
      res.on('data', (chunk) => {
        buf += chunk.toString('utf8');
        let idx;
        while ((idx = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, idx); buf = buf.slice(idx + 1);
          if (!line.trim()) continue;
          try { this._handleEvent(JSON.parse(line)); } catch { /* skip */ }
        }
      });
      res.on('end', () => {
        if (!this.stopped) this._retryTimer = setTimeout(() => this.start(), 5000);
      });
    });
  }

  _handleEvent(ev) {
    const name = ev.Actor?.Attributes?.name || ev.id?.slice(0, 12);
    if (ev.Action === 'start') {
      this._attach(ev.id);
      this._emitMeta(ev, `container ${name} started`, 'INFO');
    } else if (ev.Action === 'die') {
      const code = ev.Actor?.Attributes?.exitCode;
      this._emitMeta(ev, `container ${name} exited with code ${code}`,
        code === '0' ? 'INFO' : 'ERROR');
    } else if (ev.Action === 'oom') {
      this._emitMeta(ev, `container ${name} killed: out of memory`, 'FATAL');
    } else if (ev.Action === 'restart') {
      this._emitMeta(ev, `container ${name} restarted`, 'WARN');
    }
  }

  _emitMeta(ev, message, level) {
    this.emit({
      timestamp: new Date((ev.time || Math.floor(Date.now() / 1000)) * 1000).toISOString(),
      source: 'docker-events',
      container: ev.Actor?.Attributes?.name || ev.id?.slice(0, 12),
      service: ev.Actor?.Attributes['com.docker.compose.service'] || null,
      level,
      message,
    });
  }

  async _attach(id) {
    if (this.streams.has(id) || this.stopped) return;
    let inspect;
    try { inspect = await this._dockerJson(`/containers/${id}/json`); }
    catch { return; }
    const name = (inspect.Name || '').replace(/^\//, '') || id.slice(0, 12);
    const labels = inspect.Config?.Labels || {};
    const meta = {
      container: name,
      service: labels['com.docker.compose.service'] ||
               labels['aiops.service'] || name.replace(/-\d+$/, ''),
      git_sha: labels['aiops.git_sha'] || labels['org.opencontainers.image.revision'] || null,
      image: inspect.Config?.Image || null,
    };
    const tty = !!inspect.Config?.Tty;
    const since = Math.floor(Date.now() / 1000);
    const logPath = `/containers/${id}/logs?follow=true&stdout=true&stderr=true&timestamps=true&since=${since}`;
    const req = this._dockerRequest(logPath, (res, err) => {
      if (err || !res) { this.streams.delete(id); return; }
      if (tty) {
        this._consumeLines(res, meta, 'stdout');
      } else {
        this._consumeMultiplexed(res, meta);
      }
      res.on('end', () => this.streams.delete(id));
      res.on('error', () => this.streams.delete(id));
    });
    this.streams.set(id, req);
  }

  // Raw line stream (tty containers)
  _consumeLines(res, meta, streamName) {
    let buf = '';
    res.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let idx;
      while ((idx = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, idx); buf = buf.slice(idx + 1);
        this._emitLogLine(line, meta, streamName);
      }
    });
  }

  // Docker multiplexed stream: 8-byte header [type,0,0,0,size_be32] then payload
  _consumeMultiplexed(res, meta) {
    let buf = Buffer.alloc(0);
    res.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= 8) {
        const streamType = buf[0];
        const size = buf.readUInt32BE(4);
        if (buf.length < 8 + size) break;
        const payload = buf.subarray(8, 8 + size).toString('utf8');
        buf = buf.subarray(8 + size);
        const streamName = streamType === 2 ? 'stderr' : 'stdout';
        for (const line of payload.split('\n')) {
          if (line.trim()) this._emitLogLine(line, meta, streamName);
        }
      }
    });
  }

  _emitLogLine(line, meta, streamName) {
    // With timestamps=true docker prefixes: 2026-07-18T00:00:00.000000000Z <msg>
    let timestamp = new Date().toISOString();
    let message = line;
    const m = line.match(/^(\d{4}-\d{2}-\d{2}T[\d:.]+Z?)\s(.*)$/s);
    if (m) { timestamp = new Date(m[1]).toISOString(); message = m[2]; }
    if (!message.trim()) return;
    this.emit({
      timestamp,
      source: 'docker',
      container: meta.container,
      service: meta.service,
      git_sha: meta.git_sha,
      image: meta.image,
      stream: streamName,
      level: detectLevel(message, streamName === 'stderr' ? 'ERROR' : 'INFO'),
      message: message.slice(0, 32 * 1024),
    });
  }

  stop() {
    this.stopped = true;
    clearTimeout(this._retryTimer);
    if (this.eventsReq) this.eventsReq.destroy();
    for (const req of this.streams.values()) req.destroy();
    this.streams.clear();
  }
}

// ---------------------------------------------------------------------------
// journald collector: subscribes via `journalctl -f -o json`
// ---------------------------------------------------------------------------
class JournaldCollector {
  constructor(cfg, emit) {
    this.units = cfg.collectors.journald.units || [];
    this.emit = emit;
    this.stopped = false;
  }

  start() {
    const args = ['-f', '-o', 'json', '-n', '0', '--no-pager'];
    for (const u of this.units) args.push('-u', u);
    let proc;
    try {
      proc = spawn('journalctl', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    } catch (err) {
      log('warn', 'journalctl not available, journald collector disabled', { error: err.message });
      return;
    }
    this.proc = proc;
    let buf = '';
    proc.stdout.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      let idx;
      while ((idx = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, idx); buf = buf.slice(idx + 1);
        if (!line.trim()) continue;
        try { this._handleEntry(JSON.parse(line)); } catch { /* skip */ }
      }
    });
    proc.on('error', (err) => {
      log('warn', 'journald collector error', { error: err.message });
    });
    proc.on('exit', (code) => {
      if (!this.stopped) {
        log('warn', `journalctl exited (${code}), restarting in 10s`);
        this._retryTimer = setTimeout(() => this.start(), 10000);
      }
    });
    log('info', 'journald collector started', { units: this.units.length || 'all' });
  }

  _handleEntry(e) {
    let message = e.MESSAGE;
    if (Array.isArray(message)) message = Buffer.from(message).toString('utf8');
    if (typeof message !== 'string' || !message.trim()) return;
    const usec = parseInt(e.__REALTIME_TIMESTAMP, 10);
    const unit = e._SYSTEMD_UNIT || e.SYSLOG_IDENTIFIER || 'system';
    this.emit({
      timestamp: Number.isFinite(usec)
        ? new Date(usec / 1000).toISOString()
        : new Date().toISOString(),
      source: 'systemd',
      service: unit.replace(/\.service$/, ''),
      level: JOURNAL_PRIORITY[e.PRIORITY] || detectLevel(message),
      pid: e._PID ? parseInt(e._PID, 10) : null,
      message: message.slice(0, 32 * 1024),
    });
  }

  stop() {
    this.stopped = true;
    clearTimeout(this._retryTimer);
    if (this.proc) this.proc.kill('SIGTERM');
  }
}

// ---------------------------------------------------------------------------
// File collector: glob paths, tail appended lines, survive rotation/truncation
// ---------------------------------------------------------------------------
class FileCollector {
  constructor(cfg, state, emit) {
    this.patterns = cfg.collectors.files.paths || [];
    this.state = state;
    this.emit = emit;
    this.offsets = state.data.file_offsets; // path -> { offset, inode }
    this.partial = new Map();               // path -> trailing partial line
    this.initialized = false;               // first scan skips pre-existing history
  }

  start() {
    this.timer = setInterval(() => this._scan(), 2000);
    this.timer.unref?.();
    this._scan();
    log('info', 'file collector started', { patterns: this.patterns });
  }

  _expand(pattern) {
    // Supports '*' within path segments (no '**').
    const segs = pattern.split('/').filter((s, i) => s !== '' || i === 0);
    let paths = ['/'];
    for (const seg of segs.slice(1)) {
      const next = [];
      for (const base of paths) {
        if (seg.includes('*')) {
          const re = new RegExp('^' + seg.split('*').map(escapeRe).join('.*') + '$');
          let entries = [];
          try { entries = fs.readdirSync(base); } catch { continue; }
          for (const e of entries) if (re.test(e)) next.push(path.join(base, e));
        } else {
          next.push(path.join(base, seg));
        }
      }
      paths = next;
    }
    return paths.filter((p) => {
      try { return fs.statSync(p).isFile(); } catch { return false; }
    });
  }

  _scan() {
    const seen = new Set();
    for (const pattern of this.patterns) {
      for (const file of this._expand(pattern)) {
        seen.add(file);
        this._tail(file);
      }
    }
    // forget state for files that disappeared
    for (const p of Object.keys(this.offsets)) {
      if (!seen.has(p)) { delete this.offsets[p]; this.partial.delete(p); }
    }
    this.initialized = true;
  }

  _tail(file) {
    let stat;
    try { stat = fs.statSync(file); } catch { return; }
    let st = this.offsets[file];
    if (!st || st.inode !== stat.ino || stat.size < st.offset) {
      // Files present at agent startup: skip their history, tail from the end.
      // Files that appear later, or were rotated/truncated: read from the start.
      const skipHistory = !st && !this.initialized;
      st = this.offsets[file] = { offset: skipHistory ? stat.size : 0, inode: stat.ino };
      this.partial.delete(file);
    }
    if (stat.size === st.offset) return;

    const toRead = Math.min(stat.size - st.offset, 4 * 1024 * 1024);
    const buf = Buffer.alloc(toRead);
    let fd;
    try {
      fd = fs.openSync(file, 'r');
      fs.readSync(fd, buf, 0, toRead, st.offset);
    } catch { return; }
    finally { if (fd !== undefined) try { fs.closeSync(fd); } catch { } }

    st.offset += toRead;
    let text = (this.partial.get(file) || '') + buf.toString('utf8');
    const lines = text.split('\n');
    this.partial.set(file, lines.pop()); // keep incomplete tail for next read

    const service = path.basename(file).replace(/\.log$/, '');
    for (const line of lines) {
      if (!line.trim()) continue;
      this.emit({
        timestamp: new Date().toISOString(),
        source: 'file',
        file,
        service,
        level: detectLevel(line),
        message: line.slice(0, 32 * 1024),
      });
    }
  }

  stop() {
    clearInterval(this.timer);
    this.state.data.file_offsets = this.offsets;
    this.state.save();
  }
}

function escapeRe(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

// ---------------------------------------------------------------------------
// Metrics collector: basic host vitals as periodic events
// ---------------------------------------------------------------------------
class MetricsCollector {
  constructor(cfg, emit) {
    this.interval = (cfg.collectors.metrics.interval_seconds || 60) * 1000;
    this.emit = emit;
  }
  start() {
    this.timer = setInterval(() => this._sample(), this.interval);
    this.timer.unref?.();
    this._sample();
  }
  _sample() {
    const load = os.loadavg();
    const memTotal = os.totalmem();
    const memFree = os.freemem();
    let disk = null;
    try {
      const out = execFileSync('df', ['-B1', '--output=size,avail', '/'], { encoding: 'utf8' });
      const [size, avail] = out.trim().split('\n').pop().trim().split(/\s+/).map(Number);
      disk = { total_bytes: size, available_bytes: avail };
    } catch { /* df unavailable */ }
    this.emit({
      timestamp: new Date().toISOString(),
      source: 'metrics',
      service: 'host',
      level: 'INFO',
      message: `load1=${load[0].toFixed(2)} mem_used_pct=${(((memTotal - memFree) / memTotal) * 100).toFixed(1)}` +
        (disk ? ` disk_used_pct=${(((disk.total_bytes - disk.available_bytes) / disk.total_bytes) * 100).toFixed(1)}` : ''),
      metrics: {
        load1: load[0], load5: load[1], load15: load[2],
        mem_total_bytes: memTotal, mem_free_bytes: memFree,
        uptime_seconds: os.uptime(),
        ...(disk || {}),
      },
    });
  }
  stop() { clearInterval(this.timer); }
}

// ---------------------------------------------------------------------------
// Shipper: drains the buffer queue to the server in gzip batches
// ---------------------------------------------------------------------------
class Shipper {
  constructor(queue, sink, cfg) {
    this.queue = queue;
    this.sink = sink;
    this.batchSize = cfg.ship.batch_size;
    this.intervalMs = cfg.ship.flush_interval_seconds * 1000;
    this.backoffMs = 0;
    this.stopped = false;
  }

  start() { this._loop(); }

  async _loop() {
    while (!this.stopped) {
      const shipped = await this._flushOnce();
      const wait = this.backoffMs || (shipped ? 50 : this.intervalMs);
      await sleep(wait);
    }
  }

  async _flushOnce() {
    const seg = this.queue.oldestReadable();
    if (!seg) return false;
    this.queue.seal(seg);
    const { events, lines } = this.queue.readEvents(seg, this.batchSize);
    if (events.length === 0) { this.queue.commit(seg, lines.length, lines); return false; }
    try {
      await this.sink.send(events);
      this.queue.commit(seg, events.length, lines);
      this.backoffMs = 0;
      return true;
    } catch (err) {
      if (err.kind === 'drop') {
        // poisoned batch: drop it rather than blocking the queue forever
        log('warn', 'sink rejected batch, dropping', { count: events.length, error: err.message });
        this.queue.commit(seg, events.length, lines);
      } else if (err.kind === 'auth') {
        log('error', `sink auth/config error: ${err.message}; retrying in 5m`);
        this.backoffMs = 5 * 60 * 1000;
      } else {
        this.backoffMs = Math.min((this.backoffMs || 1000) * 2, 60000);
        log('warn', 'ship failed, will retry', { error: err.message, backoff_ms: this.backoffMs });
      }
    }
    return false;
  }

  async flushAll(deadlineMs) {
    const start = Date.now();
    while (Date.now() - start < deadlineMs) {
      if (!this.queue.oldestReadable()) return;
      const ok = await this._flushOnce();
      if (!ok && this.backoffMs) return; // server down; keep data buffered
    }
  }

  stop() { this.stopped = true; }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ---------------------------------------------------------------------------
// Remote config poller
// ---------------------------------------------------------------------------
class ConfigPoller {
  constructor(client, state, cfg, onChange) {
    this.client = client;
    this.state = state;
    this.intervalMs = cfg.ship.remote_config_interval_seconds * 1000;
    this.remotePath = path.join(cfg.state_dir, 'config.remote.json');
    this.onChange = onChange;
  }
  start() {
    this.timer = setInterval(() => this._poll(), this.intervalMs);
    this.timer.unref?.();
  }
  async _poll() {
    try {
      const res = await this.client.getJson(`/v1/agents/${this.state.data.agent_id}/config`);
      if (res.status !== 200 || !res.json) return;
      const { version, config } = res.json;
      if (version && version !== this.state.data.remote_config_version) {
        fs.writeFileSync(this.remotePath, JSON.stringify({ version, config }, null, 2), { mode: 0o600 });
        this.state.data.remote_config_version = version;
        this.state.save();
        log('info', 'remote config updated, restarting collectors', { version });
        this.onChange();
      }
    } catch { /* server unreachable; try again next tick */ }
  }
  stop() { clearInterval(this.timer); }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
function parseArgs(argv) {
  const args = { config: '/etc/aiops-agent/config.yaml' };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--config') args.config = argv[++i];
    else if (argv[i] === '--version') { console.log(VERSION); process.exit(0); }
    else if (argv[i] === '--help') {
      console.log('usage: aiops-agent [--config PATH] [--version]');
      process.exit(0);
    }
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv);
  let cfg = loadConfig(args.config);
  if (cfg.sink.type === 'aiops' && !cfg.api_key) {
    log('error', 'no API key configured (set api_key in config or AIOPS_API_KEY)');
  }

  const state = new AgentState(cfg.state_dir);
  const queue = new BufferQueue(path.join(cfg.state_dir, 'queue'), {
    maxBytes: cfg.buffer.max_bytes,
    segmentBytes: cfg.buffer.segment_bytes,
  });
  const client = new ApiClient(cfg);
  client.agentId = state.data.agent_id;

  // Enrich every event with host-level metadata before buffering.
  const emit = (event) => {
    event.hostname = cfg.hostname;
    event.environment = cfg.environment;
    event.agent_id = state.data.agent_id;
    queue.push(event);
  };

  let collectors = [];
  const startCollectors = () => {
    cfg = loadConfig(args.config); // re-read incl. remote overrides
    collectors = [];
    if (cfg.collectors.docker.enabled) collectors.push(new DockerCollector(cfg, emit));
    if (cfg.collectors.journald.enabled) collectors.push(new JournaldCollector(cfg, emit));
    if (cfg.collectors.files.enabled) collectors.push(new FileCollector(cfg, state, emit));
    if (cfg.collectors.metrics.enabled) collectors.push(new MetricsCollector(cfg, emit));
    for (const c of collectors) c.start();
  };
  const stopCollectors = () => { for (const c of collectors) c.stop(); };

  const sink = createSink(cfg, client);

  // Registration and remote config only exist on the aiops server sink;
  // direct database sinks just write rows/documents.
  let poller = null;
  if (cfg.sink.type === 'aiops') {
    // Register with the server (best effort; works offline too).
    try {
      const res = await client.postJson('/v1/agents/register', {
        agent_id: state.data.agent_id,
        hostname: cfg.hostname,
        environment: cfg.environment,
        version: VERSION,
        platform: `${os.platform()}/${os.arch()}`,
      });
      if (res.status === 200 && res.json?.agent_id) {
        state.data.agent_id = res.json.agent_id;
        client.agentId = res.json.agent_id;
        state.save();
        log('info', 'registered with server', { agent_id: state.data.agent_id });
      } else if (res.status === 401 || res.status === 403) {
        log('error', 'server rejected API key during registration; logs will buffer locally');
      }
    } catch (err) {
      log('warn', 'server unreachable, buffering locally', { error: err.message });
    }
  }

  startCollectors();

  const shipper = new Shipper(queue, sink, cfg);
  shipper.start();

  if (cfg.sink.type === 'aiops') {
    poller = new ConfigPoller(client, state, cfg, () => {
      stopCollectors();
      startCollectors();
    });
    poller.start();
  }

  emit({
    timestamp: new Date().toISOString(),
    source: 'agent',
    service: 'aiops-agent',
    level: 'INFO',
    message: `aiops-agent ${VERSION} started on ${cfg.hostname}`,
  });
  log('info', `aiops-agent ${VERSION} started`, {
    hostname: cfg.hostname,
    agent_id: state.data.agent_id,
    sink: cfg.sink.type,
    ...(cfg.sink.type === 'aiops' ? { endpoint: cfg.endpoint } : {}),
  });

  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log('info', `received ${signal}, flushing and shutting down`);
    stopCollectors();
    if (poller) poller.stop();
    shipper.stop();
    await shipper.flushAll(5000);
    sink.close();
    state.save();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

if (require.main === module) {
  main().catch((err) => {
    log('error', 'fatal', { error: err.stack || err.message });
    process.exit(1);
  });
} else {
  // for tests
  module.exports = {
    parseYaml, loadConfig, inferSinkType, detectLevel, deepMerge,
    scramClient, bsonEncode, bsonDecode, parseMongoUrl,
    BufferQueue, Shipper, SinkError,
    AiopsSink, PostgresSink, MySQLSink, MongoSink, createSink,
  };
}
