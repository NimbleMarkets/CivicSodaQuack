# Sourced by the other scripts. Resolves paths and reads the model settings.
# Run everything from anywhere; paths are relative to this file.
EX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ROOT="$(cd "$EX_DIR/../.." && pwd)"

# Which model to exercise. MODEL is vendor/model, e.g. anthropic/claude-sonnet-4-6,
# openai/gpt-5, google/gemini-2.5-pro, or compat/<name> with BASE_URL set.
: "${MODEL:?set MODEL, e.g. MODEL=anthropic/claude-sonnet-4-6}"
BASE_URL="${BASE_URL:-}"

# LABEL names this run; outputs go to .csq/ex-$LABEL/ so runs never mix.
LABEL="${LABEL:-$(echo "$MODEL" | tr '/:' '--')}"
OUT="$ROOT/.csq/ex-$LABEL"
DB="${DB:-$ROOT/.csq/chicago.duckdb}"
BIN="$ROOT/bin/csq-chat"
mkdir -p "$OUT"
