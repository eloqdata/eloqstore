#!/usr/bin/env bash
# Sync pyproject.toml / Cargo.toml versions from VERSION.
# Called by .githooks/pre-commit. Also usable standalone via 'scripts/sync-version.sh'.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
ver=$(tr -d '[:space:]' < "${ROOT}/VERSION")
changed=0

sync_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    return
  fi
  local current
  current=$(grep -m1 '^version = "' "$path" | sed 's/^version = "\([^"]*\)".*/\1/' || true)
  if [ "$current" = "$ver" ]; then
    return
  fi
  sed -i "s/^version = \"[^\"]*\"/version = \"${ver}\"/" "$path"
  git add "$path"
  echo "  synced $(realpath --relative-to="$ROOT" "$path") → ${ver}"
  changed=1
}

for f in python/pyproject.toml rust/eloqstore-sys/Cargo.toml rust/eloqstore/Cargo.toml; do
  sync_file "${ROOT}/${f}"
done

eloq_cargo="${ROOT}/rust/eloqstore/Cargo.toml"
if [ -f "$eloq_cargo" ]; then
  dep_current=$(grep 'eloqstore-sys.*version = "' "$eloq_cargo" | sed 's/.*version = "\([^"]*\)".*/\1/' || true)
  if [ -n "$dep_current" ] && [ "$dep_current" != "$ver" ]; then
    sed -i "s/\(eloqstore-sys.*version = \"\)[^\"]*\(\"\)/\1${ver}\2/" "$eloq_cargo"
    git add "$eloq_cargo"
    echo "  synced rust/eloqstore/Cargo.toml dep → ${ver}"
    changed=1
  fi
fi

if [ $changed -eq 1 ]; then
  echo "pre-commit: version synced to ${ver}"
fi
