#!/usr/bin/env bash
# Install git hooks for eloqstore development.
# Run once after cloning the repo.
set -euo pipefail

git config core.hooksPath .githooks
echo "git hooks installed (.githooks/pre-commit)"
