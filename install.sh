#!/usr/bin/env bash
#
# aiops-agent installer
#
#   curl -fsSL https://github.com/yashmishra2006/aiops-agent/releases/latest/download/install.sh | sudo bash
#     (asks interactively where logs should be stored)
#
# Non-interactive:
#   ... | sudo bash -s -- --api-key sk_xxx --endpoint https://logs.yourdomain.com
#   ... | sudo bash -s -- --connection-string 'postgresql://user:pass@host:5432/logs'
#   ... | sudo bash -s -- --connection-string 'mongodb+srv://user:pass@cluster.mongodb.net/logs'
#   add --collectors recommended | --collectors nginx,postgresql,metrics,... to
#   pick log sources without prompts (default: recommended = everything)
#
# Also works from a local checkout/tarball:  sudo ./install.sh
#
# Releases are fetched from GitHub Releases of $REPO; set AIOPS_DOWNLOAD_BASE
# to serve them from your own host instead.
set -euo pipefail

# ------------------------------------------------------------------ defaults
REPO="${AIOPS_REPO:-yashmishra2006/aiops-agent}"
DOWNLOAD_BASE="${AIOPS_DOWNLOAD_BASE:-}"
VERSION="${AIOPS_VERSION:-latest}"
INSTALL_DIR=/opt/aiops-agent
CONFIG_DIR=/etc/aiops-agent
STATE_DIR=/var/lib/aiops-agent
NODE_VERSION=22.14.0

API_KEY="${AIOPS_API_KEY:-}"
ENDPOINT="${AIOPS_ENDPOINT:-}"
CONNECTION_STRING="${AIOPS_CONNECTION_STRING:-}"
ENVIRONMENT="${AIOPS_ENVIRONMENT:-production}"
COLLECTORS="${AIOPS_COLLECTORS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-key)           API_KEY="$2"; shift 2 ;;
    --endpoint)          ENDPOINT="$2"; shift 2 ;;
    --connection-string) CONNECTION_STRING="$2"; shift 2 ;;
    --collectors)        COLLECTORS="$2"; shift 2 ;;
    --environment)       ENVIRONMENT="$2"; shift 2 ;;
    --version)           VERSION="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

log()  { echo -e "\033[1;32m==>\033[0m $*"; }
fail() { echo -e "\033[1;31merror:\033[0m $*" >&2; exit 1; }

check_connection_string() {
  case "$1" in
    postgres://*|postgresql://*|mysql://*|mongodb://*|mongodb+srv://*) ;;
    *) fail "unsupported connection string (expected postgres://, postgresql://, mysql://, mongodb:// or mongodb+srv://)" ;;
  esac
}

# ------------------------------------------------------- log source catalog
SOURCES=(journal docker system metrics nginx apache postgresql mysql mongodb redis auth audit kernel mail cron apps)
RECOMMENDED="journal docker system metrics apps"

desc_for() {
  case "$1" in
    journal)    echo "full systemd journal — every service that logs to it" ;;
    docker)     echo "Docker containers — log streams + start/stop/OOM events" ;;
    system)     echo "system log files — /var/log/*.log, syslog, messages" ;;
    metrics)    echo "host metrics — CPU load, memory, disk usage" ;;
    nginx)      echo "nginx — access + error logs" ;;
    apache)     echo "Apache httpd — access + error logs" ;;
    postgresql) echo "PostgreSQL server logs" ;;
    mysql)      echo "MySQL / MariaDB server logs" ;;
    mongodb)    echo "MongoDB server logs" ;;
    redis)      echo "Redis server logs" ;;
    auth)       echo "logins, sudo, fail2ban — auth.log / secure" ;;
    audit)      echo "auditd security audit trail" ;;
    kernel)     echo "kernel messages — kern.log" ;;
    mail)       echo "mail server — mail.log / maillog" ;;
    cron)       echo "cron job logs" ;;
    apps)       echo "app logs — /opt/*/logs, PM2, supervisor" ;;
  esac
}

detect_source() {
  case "$1" in
    journal)    command -v journalctl >/dev/null ;;
    docker)     [[ -S /var/run/docker.sock || -S /run/docker.sock ]] ;;
    system)     return 0 ;;
    metrics)    return 0 ;;
    nginx)      [[ -d /var/log/nginx ]] ;;
    apache)     [[ -d /var/log/apache2 || -d /var/log/httpd ]] ;;
    postgresql) [[ -d /var/log/postgresql || -d /var/lib/pgsql ]] ;;
    mysql)      [[ -d /var/log/mysql || -d /var/log/mariadb || -f /var/log/mysqld.log ]] ;;
    mongodb)    [[ -d /var/log/mongodb ]] ;;
    redis)      [[ -d /var/log/redis ]] ;;
    auth)       [[ -f /var/log/auth.log || -f /var/log/secure ]] ;;
    audit)      [[ -f /var/log/audit/audit.log ]] ;;
    kernel)     [[ -f /var/log/kern.log ]] ;;
    mail)       [[ -f /var/log/mail.log || -f /var/log/maillog ]] ;;
    cron)       [[ -f /var/log/cron ]] ;;
    apps)       return 0 ;;
    *)          return 1 ;;
  esac
}

paths_for() {
  case "$1" in
    system)     echo '/var/log/*.log /var/log/syslog /var/log/messages' ;;
    nginx)      echo '/var/log/nginx/*.log' ;;
    apache)     echo '/var/log/apache2/*.log /var/log/httpd/*log' ;;
    postgresql) echo '/var/log/postgresql/*.log /var/lib/pgsql/*/log/*.log /var/lib/pgsql/*/data/log/*.log' ;;
    mysql)      echo '/var/log/mysql/*.log /var/log/mariadb/*.log /var/log/mysqld.log' ;;
    mongodb)    echo '/var/log/mongodb/*.log' ;;
    redis)      echo '/var/log/redis/*.log' ;;
    auth)       echo '/var/log/auth.log /var/log/secure /var/log/fail2ban.log' ;;
    audit)      echo '/var/log/audit/audit.log' ;;
    kernel)     echo '/var/log/kern.log' ;;
    mail)       echo '/var/log/mail.log /var/log/maillog' ;;
    cron)       echo '/var/log/cron' ;;
    apps)       echo '/opt/*/logs/*.log /var/log/supervisor/*.log /root/.pm2/logs/*.log /home/*/.pm2/logs/*.log' ;;
  esac
}

# journald units that also cover the source (catches services logging to the
# journal instead of, or in addition to, their files)
units_for() {
  case "$1" in
    nginx)      echo 'nginx.service' ;;
    apache)     echo 'apache2.service httpd.service' ;;
    postgresql) echo 'postgresql.service' ;;
    mysql)      echo 'mysql.service mysqld.service mariadb.service' ;;
    mongodb)    echo 'mongod.service' ;;
    redis)      echo 'redis.service redis-server.service' ;;
    cron)       echo 'cron.service crond.service' ;;
  esac
}

in_sources() {
  local s
  for s in "${SOURCES[@]}"; do [[ "$s" == "$1" ]] && return 0; done
  return 1
}

# Turns the chosen source list into collector settings for the config file.
build_collection() {
  SEL_DOCKER=false SEL_JOURNAL=false SEL_METRICS=false SEL_FILES=false
  JOURNAL_UNITS="" FILE_PATHS=""
  local journal_all=false s u
  for s in $1; do
    case "$s" in
      journal) SEL_JOURNAL=true; journal_all=true ;;
      docker)  SEL_DOCKER=true ;;
      metrics) SEL_METRICS=true ;;
      *)
        FILE_PATHS+=" $(paths_for "$s")"
        u="$(units_for "$s")"
        if [[ -n "$u" ]]; then SEL_JOURNAL=true; JOURNAL_UNITS+=" $u"; fi
        ;;
    esac
  done
  [[ "$journal_all" == true ]] && JOURNAL_UNITS=""   # empty units = all units
  FILE_PATHS="${FILE_PATHS# }"
  JOURNAL_UNITS="${JOURNAL_UNITS# }"
  [[ -n "$FILE_PATHS" ]] && SEL_FILES=true
}

# --------------------------------------------------- choose a storage backend
# Interactive unless credentials were already given via flags/env. Reads from
# /dev/tty so it also works when piped through `curl | sudo bash`.
if [[ -z "$API_KEY" && -z "$CONNECTION_STRING" && -r /dev/tty && -w /dev/tty ]]; then
  echo                                                                > /dev/tty
  echo "Where should this agent send logs?"                           > /dev/tty
  echo                                                                > /dev/tty
  echo "  1) aiops server   — central log store with search API (needs an API key)" > /dev/tty
  echo "  2) PostgreSQL     — write directly into your own database" > /dev/tty
  echo "  3) MySQL          — write directly into your own database" > /dev/tty
  echo "  4) MongoDB        — write directly into your own database" > /dev/tty
  echo                                                                > /dev/tty
  CHOICE=""
  while [[ ! "$CHOICE" =~ ^[1-4]$ ]]; do
    printf "Choice [1-4]: " > /dev/tty
    read -r CHOICE < /dev/tty || fail "no input"
  done
  case "$CHOICE" in
    1)
      printf "aiops server endpoint (e.g. https://logs.yourdomain.com): " > /dev/tty
      read -r ENDPOINT < /dev/tty
      printf "API key: " > /dev/tty
      read -r API_KEY < /dev/tty
      [[ -n "$ENDPOINT" && -n "$API_KEY" ]] || fail "endpoint and API key are required"
      ;;
    2)
      printf "PostgreSQL connection string (postgresql://user:pass@host:5432/dbname): " > /dev/tty
      read -r CONNECTION_STRING < /dev/tty
      ;;
    3)
      printf "MySQL connection string (mysql://user:pass@host:3306/dbname): " > /dev/tty
      read -r CONNECTION_STRING < /dev/tty
      ;;
    4)
      printf "MongoDB connection string (mongodb+srv://user:pass@cluster/dbname): " > /dev/tty
      read -r CONNECTION_STRING < /dev/tty
      ;;
  esac

  # ---- which log sources -------------------------------------------------
  if [[ -z "$COLLECTORS" ]]; then
    echo                                                              > /dev/tty
    echo "Which logs should it collect?"                              > /dev/tty
    echo                                                              > /dev/tty
    echo "  1) Recommended — system journal, Docker, log files, host metrics (everything)" > /dev/tty
    echo "  2) Custom      — pick individual sources"                 > /dev/tty
    echo                                                              > /dev/tty
    SCOPE=""
    while [[ ! "$SCOPE" =~ ^[12]$ ]]; do
      printf "Choice [1-2]: " > /dev/tty
      read -r SCOPE < /dev/tty || fail "no input"
    done
    if [[ "$SCOPE" == 2 ]]; then
      echo                                                            > /dev/tty
      echo "Log sources (✓ = found on this machine):"                 > /dev/tty
      echo                                                            > /dev/tty
      i=1
      for s in "${SOURCES[@]}"; do
        if detect_source "$s"; then mark="✓"; else mark=" "; fi
        printf "  %2d) %s %-11s %s\n" "$i" "$mark" "$s" "$(desc_for "$s")" > /dev/tty
        i=$((i + 1))
      done
      echo                                                            > /dev/tty
      COLLECTORS=""
      while [[ -z "$COLLECTORS" ]]; do
        printf "Sources to collect (numbers or names, comma-separated, e.g. 1,5,7): " > /dev/tty
        read -r PICKS < /dev/tty || fail "no input"
        COLLECTORS=""
        ok=true
        IFS=', ' read -ra PARTS <<< "$PICKS"
        for p in "${PARTS[@]}"; do
          [[ -z "$p" ]] && continue
          if [[ "$p" =~ ^[0-9]+$ ]] && (( p >= 1 && p <= ${#SOURCES[@]} )); then
            COLLECTORS+="${SOURCES[$((p - 1))]},"
          elif in_sources "$p"; then
            COLLECTORS+="$p,"
          else
            echo "  unknown source: $p" > /dev/tty
            ok=false
          fi
        done
        [[ "$ok" == true && -n "$COLLECTORS" ]] || COLLECTORS=""
      done
      COLLECTORS="${COLLECTORS%,}"
    else
      COLLECTORS=recommended
    fi
  fi
fi
[[ -z "$CONNECTION_STRING" ]] || check_connection_string "$CONNECTION_STRING"

# resolve the source list (flags, prompt answer, or default) into settings
if [[ -z "$COLLECTORS" || "$COLLECTORS" == recommended ]]; then
  CHOSEN="$RECOMMENDED"
else
  CHOSEN="${COLLECTORS//,/ }"
  for s in $CHOSEN; do
    in_sources "$s" || fail "unknown collector '$s' (valid: ${SOURCES[*]}, or 'recommended')"
  done
fi
build_collection "$CHOSEN"
log "collecting: $CHOSEN"

[[ $EUID -eq 0 ]] || fail "must run as root (use sudo)"
command -v systemctl >/dev/null || fail "systemd is required"

# ------------------------------------------------------- locate the agent js
# Prefer a local copy (tarball / repo install); otherwise download + verify.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || echo /nonexistent)"
mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$STATE_DIR"
chmod 700 "$CONFIG_DIR" "$STATE_DIR"

if [[ -f "$SCRIPT_DIR/agent/aiops-agent.js" ]]; then
  log "installing agent from local files"
  cp "$SCRIPT_DIR/agent/aiops-agent.js" "$INSTALL_DIR/aiops-agent.js"
  cp "$SCRIPT_DIR/packaging/aiops-agent.service" /etc/systemd/system/aiops-agent.service
else
  if [[ -z "$DOWNLOAD_BASE" ]]; then
    if [[ "$VERSION" == "latest" ]]; then
      # GitHub redirects /releases/latest to /releases/tag/vX.Y.Z
      TAG="$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
        "https://github.com/$REPO/releases/latest" | sed 's|.*/tag/||')"
      [[ -n "$TAG" && "$TAG" != *"latest"* ]] \
        || fail "could not resolve the latest release of $REPO"
      VERSION="${TAG#v}"
    fi
    DOWNLOAD_BASE="https://github.com/$REPO/releases/download/v$VERSION"
  fi
  log "downloading aiops-agent v$VERSION"
  TMP="$(mktemp -d)"
  trap 'rm -rf "$TMP"' EXIT
  curl -fsSL "$DOWNLOAD_BASE/aiops-agent-$VERSION.tar.gz" -o "$TMP/agent.tar.gz"
  curl -fsSL "$DOWNLOAD_BASE/aiops-agent-$VERSION.tar.gz.sha256" -o "$TMP/agent.sha256"
  log "verifying checksum"
  (cd "$TMP" && echo "$(cat agent.sha256)  agent.tar.gz" | sha256sum -c -) \
    || fail "checksum verification failed — aborting"
  tar -xzf "$TMP/agent.tar.gz" -C "$TMP"
  cp "$TMP"/aiops-agent-*/agent/aiops-agent.js "$INSTALL_DIR/aiops-agent.js"
  cp "$TMP"/aiops-agent-*/packaging/aiops-agent.service /etc/systemd/system/aiops-agent.service
fi

# ------------------------------------------------------------- node runtime
# Use system node >= 20 if present; otherwise install a private runtime.
NODE_BIN=""
if command -v node >/dev/null; then
  NODE_MAJOR="$(node -e 'console.log(process.versions.node.split(".")[0])')"
  [[ "$NODE_MAJOR" -ge 20 ]] && NODE_BIN="$(command -v node)"
fi
if [[ -z "$NODE_BIN" ]]; then
  log "installing private Node.js runtime (v$NODE_VERSION)"
  ARCH="$(uname -m)"
  case "$ARCH" in
    x86_64)  NODE_ARCH=x64 ;;
    aarch64) NODE_ARCH=arm64 ;;
    *) fail "unsupported architecture: $ARCH" ;;
  esac
  NTMP="$(mktemp -d)"
  NODE_TARBALL="node-v$NODE_VERSION-linux-$NODE_ARCH.tar.xz"
  curl -fsSL "https://nodejs.org/dist/v$NODE_VERSION/$NODE_TARBALL" -o "$NTMP/$NODE_TARBALL"
  curl -fsSL "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt" -o "$NTMP/SHASUMS256.txt"
  (cd "$NTMP" && grep " $NODE_TARBALL\$" SHASUMS256.txt | sha256sum -c -) \
    || fail "node runtime checksum verification failed"
  mkdir -p "$INSTALL_DIR/runtime"
  tar -xJf "$NTMP/$NODE_TARBALL" -C "$INSTALL_DIR/runtime" --strip-components=1
  rm -rf "$NTMP"
  NODE_BIN="$INSTALL_DIR/runtime/bin/node"
fi
mkdir -p "$INSTALL_DIR/bin"
ln -sf "$NODE_BIN" "$INSTALL_DIR/bin/node"

# ------------------------------------------------------------------- config
write_config() {
  echo "# aiops-agent configuration (generated by install.sh)"
  echo
  printf 'environment: %s\n' "$ENVIRONMENT"
  echo
  if [[ -n "$CONNECTION_STRING" ]]; then
    echo "# Logs are written directly into your database; sink type is inferred"
    echo "# from the connection string scheme."
    echo "sink:"
    printf '  connection_string: %s\n' "$CONNECTION_STRING"
    echo "  table: aiops_logs"
  else
    echo "# Central aiops ingest server"
    printf 'endpoint: %s\n' "${ENDPOINT:-https://logs.yourdomain.com}"
    printf 'api_key: %s\n' "${API_KEY:-sk_live_replace_me}"
  fi
  echo
  echo "collectors:"
  echo "  docker:"
  printf '    enabled: %s\n' "$SEL_DOCKER"
  echo "    socket: /var/run/docker.sock"
  echo "  journald:"
  printf '    enabled: %s\n' "$SEL_JOURNAL"
  if [[ -n "$JOURNAL_UNITS" ]]; then
    echo "    units:"
    for u in $JOURNAL_UNITS; do printf '      - %s\n' "$u"; done
  fi
  echo "  files:"
  printf '    enabled: %s\n' "$SEL_FILES"
  if [[ -n "$FILE_PATHS" ]]; then
    echo "    paths:"
    for p in $FILE_PATHS; do printf '      - %s\n' "$p"; done
  fi
  echo "  metrics:"
  printf '    enabled: %s\n' "$SEL_METRICS"
  echo "    interval_seconds: 60"
}

if [[ -f "$CONFIG_DIR/config.yaml" ]]; then
  log "keeping existing config at $CONFIG_DIR/config.yaml"
else
  log "writing $CONFIG_DIR/config.yaml"
  write_config > "$CONFIG_DIR/config.yaml"
fi
chmod 600 "$CONFIG_DIR/config.yaml"
chown root:root "$CONFIG_DIR/config.yaml"

# ------------------------------------------------------------------ service
log "registering systemd service"
systemctl daemon-reload
systemctl enable aiops-agent >/dev/null 2>&1
systemctl restart aiops-agent

sleep 1
if systemctl is-active --quiet aiops-agent; then
  log "aiops-agent is running"
else
  fail "service failed to start — check: journalctl -u aiops-agent -n 50"
fi

echo
log "installed. next steps:"
if [[ -z "$API_KEY" && -z "$CONNECTION_STRING" ]]; then
  echo "  1. set your API key or connection string:  sudo nano $CONFIG_DIR/config.yaml"
  echo "  2. restart the agent:                      sudo systemctl restart aiops-agent"
fi
echo "  status:  systemctl status aiops-agent"
echo "  logs:    journalctl -u aiops-agent -f"
