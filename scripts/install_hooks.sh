#!/usr/bin/env bash
# Git hooks of this public repository: gitleaks secret scan (pre-commit) and the
# commit-msg hook that strips attribution trailers (copied from General-DataLakehouse).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
install -m 0755 scripts/hooks/pre-commit .git/hooks/pre-commit
install -m 0755 scripts/hooks/commit-msg .git/hooks/commit-msg
echo "hooks installed: pre-commit (gitleaks), commit-msg"
