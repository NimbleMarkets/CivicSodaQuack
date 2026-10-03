#!/usr/bin/env bash
# Run one headless session: run.sh <session-name> <prompt...>
# Needs MODEL (see env.sh); API keys come from the vendor's environment variable.
set -u
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
session="$1"; shift
args=(--db "chicago=$DB" -m "$MODEL" --session "$session"
      --scratch-dir "$OUT/scratch" --session-dir "$OUT/sessions"
      --timeout 12m -v --prompt "$*")
[ -n "$BASE_URL" ] && args+=(--base-url "$BASE_URL")
"$BIN" "${args[@]}" > "$OUT/$session.json" 2> "$OUT/$session.err"
rc=$?
echo "finished $session rc=$rc" | tee -a "$OUT/progress.log"
