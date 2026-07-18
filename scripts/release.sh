#!/usr/bin/env bash
# Publish a release: build the tarball, tag the commit, push, and create a
# GitHub Release carrying the assets install.sh downloads.
#
#   ./scripts/release.sh            # releases the version in agent/aiops-agent.js
#
# Requires: git remote "origin" on GitHub, gh CLI authenticated.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VERSION="$(node agent/aiops-agent.js --version)"
TAG="v$VERSION"

[[ -z "$(git status --porcelain)" ]] || { echo "error: uncommitted changes; commit first" >&2; exit 1; }

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "error: tag $TAG already exists — bump VERSION in agent/aiops-agent.js" >&2
  exit 1
fi

./scripts/package.sh

git tag -a "$TAG" -m "aiops-agent $TAG"
git push origin HEAD "$TAG"

gh release create "$TAG" \
  "dist/aiops-agent-$VERSION.tar.gz" \
  "dist/aiops-agent-$VERSION.tar.gz.sha256" \
  --title "aiops-agent $TAG" \
  --notes "Install or upgrade:

\`\`\`bash
curl -fsSL https://raw.githubusercontent.com/yashmishra2006/aiops-agent/main/install.sh | sudo bash
\`\`\`

The installer verifies the tarball against its sha256 before installing."

echo
echo "released $TAG. one-line install:"
echo "  curl -fsSL https://raw.githubusercontent.com/yashmishra2006/aiops-agent/main/install.sh | sudo bash"
