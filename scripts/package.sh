#!/usr/bin/env bash
# Build a shippable release tarball: dist/aiops-agent-<version>.tar.gz + .sha256
# Normally invoked via scripts/release.sh, which publishes them as a GitHub
# Release so the curl | bash installer can find them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="$(node -e "console.log(require('child_process').execFileSync('node',['$ROOT/agent/aiops-agent.js','--version']).toString().trim())")"
DIST="$ROOT/dist"
STAGE="$DIST/aiops-agent-$VERSION"

rm -rf "$STAGE" && mkdir -p "$STAGE"
cp -r "$ROOT/agent" "$ROOT/packaging" "$ROOT/install.sh" "$STAGE/"
chmod +x "$STAGE/install.sh"

TARBALL="$DIST/aiops-agent-$VERSION.tar.gz"
tar -czf "$TARBALL" -C "$DIST" "aiops-agent-$VERSION"
sha256sum "$TARBALL" | awk '{print $1}' > "$TARBALL.sha256"
cp "$ROOT/install.sh" "$DIST/install.sh"
rm -rf "$STAGE"

echo "built:"
echo "  $TARBALL"
echo "  $TARBALL.sha256  ($(cat "$TARBALL.sha256"))"
echo "  $DIST/install.sh"
