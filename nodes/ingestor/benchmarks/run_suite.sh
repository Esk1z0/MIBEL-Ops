#!/usr/bin/env bash
# Runs the whole manual test suite of the ingestor on this machine and packs the results in one file.
#
# Usage (from nodes/ingestor/, with the node's venv active):
#   benchmarks/run_suite.sh ENV_FILE [RUN_MINUTES]
#     ENV_FILE      file with the tokens (KEY=VALUE); it is only read, never copied into the results
#     RUN_MINUTES   minutes of service loop at the end (default 15; 0 skips it)
#   IDLE_SECONDS=120 (environment) controls the idle benchmark.
#
# Nothing is installed and nothing is sent anywhere: test mode with the jsonl sink in a temporary state
# directory, which is deleted at the end (it holds market data). The results contain no tokens and no
# hostname (only a short hash).
set -euo pipefail

ENV_FILE=${1:?usage: benchmarks/run_suite.sh ENV_FILE [RUN_MINUTES]}
RUN_MINUTES=${2:-15}
IDLE_SECONDS=${IDLE_SECONDS:-120}
METRICS_PORT=9108

[[ -f "$ENV_FILE" ]] || { echo "env file not found: $ENV_FILE"; exit 1; }
[[ -f pyproject.toml && -d benchmarks ]] || { echo "run it from nodes/ingestor/"; exit 1; }
command -v mibel-ingestor >/dev/null || { echo "mibel-ingestor not found: activate the venv (source .venv/bin/activate)"; exit 1; }

STAMP=$(date -u +%Y%m%dT%H%MZ)
OUT="benchmarks/results/suite-$(uname -m)-$STAMP"
mkdir -p "$OUT"
export MIBEL_STATE_DIR="$OUT/state"
TEST_ARGS=(--mode test --set sink.type=jsonl --env-file "$ENV_FILE")

step() { echo "[$(date +%H:%M:%S)] $*"; }

# --- 1. machine description ---
step "system description"
{
  uname -m
  python --version
  if [[ -r /proc/device-tree/model ]]; then tr -d '\0' < /proc/device-tree/model; echo; fi
  free -m
  echo "cpus: $(nproc)"
} > "$OUT/system.txt"

# --- 2. configuration ---
step "check and sources"
mibel-ingestor check "${TEST_ARGS[@]}" > "$OUT/check.json"
mibel-ingestor sources --env-file "$ENV_FILE" > "$OUT/sources.json"

# --- 3. one real pass with production rules (with peak memory when /usr/bin/time exists) ---
step "once: one real pass over every source"
TIMER=()
[[ -x /usr/bin/time ]] && TIMER=(/usr/bin/time -v)
"${TIMER[@]}" mibel-ingestor once "${TEST_ARGS[@]}" > "$OUT/once.json" 2> "$OUT/once.log" \
  || step "once: some job failed (see once.log)"

# --- 4. benchmarks ---
step "benchmark offline (synthetic payloads, no network)"
python benchmarks/bench.py offline --repeat 10 --out "$OUT/offline.json" > /dev/null
step "benchmark online (one real pass)"
python benchmarks/bench.py online --env-file "$ENV_FILE" --out "$OUT/online.json" > /dev/null
step "benchmark idle (${IDLE_SECONDS} s)"
python benchmarks/bench.py idle --seconds "$IDLE_SECONDS" --out "$OUT/idle.json" > /dev/null

# --- 5. service loop, with a metrics snapshot shortly before stopping it ---
if (( RUN_MINUTES > 0 )); then
  step "service loop for ${RUN_MINUTES} min"
  mibel-ingestor run "${TEST_ARGS[@]}" --set metrics.port="$METRICS_PORT" > "$OUT/run.log" 2>&1 &
  SERVICE_PID=$!
  sleep $(( RUN_MINUTES * 60 - 15 ))
  curl -s "127.0.0.1:${METRICS_PORT}/metrics" | grep -E '^(mibel_ingestor_|process_)' | grep -v '_bucket' > "$OUT/metrics.txt" || true
  sleep 15
  kill -TERM "$SERVICE_PID"
  wait "$SERVICE_PID" || step "service exited with an error (see run.log)"
fi

# --- 6. pack (without market data) ---
rm -rf "$OUT/state"
tar czf "$OUT.tar.gz" -C benchmarks/results "$(basename "$OUT")"
step "done: $OUT.tar.gz"
