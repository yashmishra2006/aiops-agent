#!/usr/bin/env bash
#
# aiops-agent installer
#
#   curl -fsSL https://raw.githubusercontent.com/yashmishra2006/aiops-agent/main/install.sh | sudo bash
#     (asks interactively where logs should be stored)
#
# Non-interactive:
#   ... | sudo bash -s -- --api-key sk_xxx --endpoint https://logs.yourdomain.com
#   ... | sudo bash -s -- --connection-string 'postgresql://user:pass@host:5432/logs'
#   ... | sudo bash -s -- --connection-string 'mongodb+srv://user:pass@cluster.mongodb.net/logs'
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-key)           API_KEY="$2"; shift 2 ;;
    --endpoint)          ENDPOINT="$2"; shift 2 ;;
    --connection-string) CONNECTION_STRING="$2"; shift 2 ;;
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
fi
[[ -z "$CONNECTION_STRING" ]] || check_connection_string "$CONNECTION_STRING"

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
  CONFIG_TEMPLATE="$SCRIPT_DIR/agent/config.example.yaml"
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
  CONFIG_TEMPLATE="$TMP"/aiops-agent-*/agent/config.example.yaml
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
  curl -fsSL "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-$NODE_ARCH.tar.xz" \
    -o "$NTMP/node.tar.xz"
  curl -fsSL "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt" -o "$NTMP/SHASUMS256.txt"
  (cd "$NTMP" && grep " node-v$NODE_VERSION-linux-$NODE_ARCH.tar.xz\$" SHASUMS256.txt | sha256sum -c -) \
    || fail "node runtime checksum verification failed"
  mkdir -p "$INSTALL_DIR/runtime"
  tar -xJf "$NTMP/node.tar.xz" -C "$INSTALL_DIR/runtime" --strip-components=1
  rm -rf "$NTMP"
  NODE_BIN="$INSTALL_DIR/runtime/bin/node"
fi
mkdir -p "$INSTALL_DIR/bin"
ln -sf "$NODE_BIN" "$INSTALL_DIR/bin/node"

# ------------------------------------------------------------------- config
if [[ -f "$CONFIG_DIR/config.yaml" ]]; then
  log "keeping existing config at $CONFIG_DIR/config.yaml"
elif [[ -n "$CONNECTION_STRING" ]]; then
  log "writing $CONFIG_DIR/config.yaml (direct database sink)"
  {
    echo "# aiops-agent configuration (generated by install.sh)"
    echo "# Logs are written directly into your database; sink type is inferred"
    echo "# from the connection string scheme."
    echo
    printf 'environment: %s\n' "$ENVIRONMENT"
    echo
    echo "sink:"
    printf '  connection_string: %s\n' "$CONNECTION_STRING"
    echo "  table: aiops_logs"
    cat <<'YAML'

collectors:
  docker:
    enabled: true
    socket: /var/run/docker.sock
  journald:
    enabled: true
  files:
    enabled: true
    paths:
      - /var/log/*.log
      - /var/log/syslog
      - /opt/*/logs/*.log
  metrics:
    enabled: true
    interval_seconds: 60
YAML
  } > "$CONFIG_DIR/config.yaml"
else
  log "writing $CONFIG_DIR/config.yaml"
  cp "$CONFIG_TEMPLATE" "$CONFIG_DIR/config.yaml"
  [[ -n "$ENDPOINT" ]] && sed -i "s|^endpoint:.*|endpoint: $ENDPOINT|" "$CONFIG_DIR/config.yaml"
  [[ -n "$API_KEY"  ]] && sed -i "s|^api_key:.*|api_key: $API_KEY|"   "$CONFIG_DIR/config.yaml"
  sed -i "s|^environment:.*|environment: $ENVIRONMENT|" "$CONFIG_DIR/config.yaml"
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
