#!/usr/bin/env bash
# bench.sh — Performance benchmarks for lattice-db.
#
# Measures realistic throughput and latency across the four scaling regimes:
#   1. Read scaling          — get against a hot key (cache-hit fast path).
#   2. Single-table writes   — put against one table (one JetStream leader).
#   3. Multi-table writes    — put across N tables (multiple JS leaders).
#   4. Transactions          — txn through the global ldb-txn WAL stream.
#
# Requires: nats CLI, jq, base64
#
# Usage:
#   bash tests/bench.sh                      # plain, localhost:4222
#   NATS_URL=host:port bash tests/bench.sh   # custom address
#   bash tests/bench.sh --tls                # mTLS via wasmcloud-data-tls secret
#
# Tunables (env vars):
#   BENCH_DURATION   total seconds per bench scenario        (default 10)
#   BENCH_CLIENTS    concurrent client connections           (default 16)
#   BENCH_MSG_SIZE   value payload size in bytes             (default 256)
#   BENCH_TABLES     number of tables for multi-table write  (default 8)
#
# Note: this benches the *cluster as observed by clients*. To measure
# read scaling vs replica count, deploy N replicas and re-run; lattice-db
# replicas join queue group `ldb-workers`, so adding replicas should
# scale reads near-linearly until NATS itself is the bottleneck.

set -euo pipefail

# ── TLS / NATS_URL setup (mirrors integration.sh) ───────────────────

NATS_EXTRA_ARGS=""
NATS_URL_SET="${NATS_URL:-}"

if [[ "${1:-}" == "--tls" ]] || [[ "${NATS_TLS:-}" == "1" ]]; then
  CERT_DIR=$(mktemp -d)
  trap "rm -rf $CERT_DIR" EXIT
  kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.ca\.crt}'  | base64 -d > "$CERT_DIR/ca.crt"
  kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.tls\.crt}' | base64 -d > "$CERT_DIR/tls.crt"
  kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.tls\.key}' | base64 -d > "$CERT_DIR/tls.key"
  NATS_EXTRA_ARGS="--tlsca $CERT_DIR/ca.crt --tlscert $CERT_DIR/tls.crt --tlskey $CERT_DIR/tls.key"
  NATS_URL_SET="${NATS_URL_SET:-nats:4222}"
  echo "TLS: using mTLS certs from wasmcloud-data-tls secret (server: ${NATS_URL_SET})"
fi

NATS_URL="${NATS_URL_SET:-localhost:4222}"
NATS="nats -s nats://${NATS_URL} ${NATS_EXTRA_ARGS}"

DURATION="${BENCH_DURATION:-10}"
CLIENTS="${BENCH_CLIENTS:-16}"
MSG_SIZE="${BENCH_MSG_SIZE:-256}"
NTABLES="${BENCH_TABLES:-8}"

RUN_ID="$(date +%s)"

# ── Sanity check ────────────────────────────────────────────────────

if ! command -v nats >/dev/null; then
  echo "error: 'nats' CLI not found" >&2
  exit 1
fi
if ! command -v jq >/dev/null; then
  echo "error: 'jq' not found" >&2
  exit 1
fi

echo "── lattice-db benchmark ────────────────────────────────────────"
echo "  NATS URL    : ${NATS_URL}"
echo "  duration    : ${DURATION}s per scenario"
echo "  clients     : ${CLIENTS}"
echo "  msg size    : ${MSG_SIZE} B"
echo "  multi-table : ${NTABLES} tables"
echo "  run id      : ${RUN_ID}"
echo

# Verify lattice-db is reachable.
PING_TABLE="bench_ping_${RUN_ID}"
if ! $NATS req --timeout 5s "ldb.exists" "{\"table\":\"${PING_TABLE}\",\"key\":\"x\"}" --raw >/dev/null 2>&1; then
  echo "error: cannot reach lattice-db on ${NATS_URL} — is the service running?" >&2
  exit 1
fi

# ── Helpers ─────────────────────────────────────────────────────────

# Build a base64 payload of approximately MSG_SIZE bytes wrapped in a JSON value.
make_value_b64() {
  head -c "$MSG_SIZE" /dev/urandom | base64 | tr -d '\n'
}

# Run `nats bench` in request/reply mode against a subject, with a fixed payload.
# Args: $1=label  $2=subject  $3=payload_file
bench_req() {
  local label="$1" subject="$2" payload_file="$3"
  echo "── ${label} ───────────────────────────────────────────────────"
  echo "  subject: ${subject}"
  $NATS bench service serve "${subject}.__never__" --noprogress >/dev/null 2>&1 || true

  # `nats bench request` is the right tool: it sends N requests with C clients
  # and reports msg/sec + latency percentiles.
  $NATS bench request "${subject}" \
    --clients "${CLIENTS}" \
    --size 0 \
    --msgs 0 \
    --duration "${DURATION}s" \
    --payload "@${payload_file}" \
    --no-progress 2>/dev/null || {
      # Older nats CLIs use a different subcommand layout; fall back.
      $NATS bench "${subject}" \
        --reply \
        --clients "${CLIENTS}" \
        --size "${MSG_SIZE}" \
        --msgs 100000 2>/dev/null
    }
  echo
}

# Manual rate loop — for scenarios `nats bench` can't model (multi-subject,
# txn). Spawns CLIENTS background workers each issuing requests as fast as
# possible for DURATION seconds. Reports total req/s and rough p50/p99.
manual_bench() {
  local label="$1"
  shift
  local -a build_payload_cmd=("$@")  # called as: build_payload_cmd <i>

  echo "── ${label} ───────────────────────────────────────────────────"
  local tmpdir
  tmpdir=$(mktemp -d)
  trap "rm -rf $tmpdir" RETURN

  local end=$(( $(date +%s) + DURATION ))

  for c in $(seq 1 "$CLIENTS"); do
    (
      local count=0
      local lat_file="${tmpdir}/lat-${c}"
      : > "$lat_file"
      while [[ $(date +%s) -lt $end ]]; do
        # Round-robin payload variation by request count (so multi-table etc. spreads load).
        local subject payload
        read -r subject payload < <("${build_payload_cmd[@]}" "$count" "$c")
        local t0 t1
        t0=$(date +%s%N)
        $NATS req --timeout 2s "$subject" "$payload" --raw >/dev/null 2>&1 || true
        t1=$(date +%s%N)
        echo $(( (t1 - t0) / 1000 )) >> "$lat_file"  # microseconds
        count=$((count + 1))
      done
      echo "$count" > "${tmpdir}/count-${c}"
    ) &
  done
  wait

  # Aggregate.
  local total=0
  for c in $(seq 1 "$CLIENTS"); do
    total=$(( total + $(cat "${tmpdir}/count-${c}" 2>/dev/null || echo 0) ))
  done
  local rate=$(( total / DURATION ))

  # Latency percentiles in microseconds → ms.
  cat "${tmpdir}"/lat-* 2>/dev/null | sort -n > "${tmpdir}/lat-all"
  local n p50 p99
  n=$(wc -l < "${tmpdir}/lat-all" | tr -d ' ')
  if [[ "$n" -gt 0 ]]; then
    p50=$(awk "NR==int(${n}*0.50)+1" "${tmpdir}/lat-all")
    p99=$(awk "NR==int(${n}*0.99)+1" "${tmpdir}/lat-all")
    printf "  total reqs : %d\n" "$total"
    printf "  throughput : %d req/s\n" "$rate"
    printf "  latency p50: %.2f ms\n" "$(echo "$p50/1000" | bc -l)"
    printf "  latency p99: %.2f ms\n" "$(echo "$p99/1000" | bc -l)"
  else
    echo "  (no successful requests)"
  fi
  echo
}

# ── Scenario 1: cache-hit reads ─────────────────────────────────────
# Pre-populate one key, then hammer ldb.get. Tests the in-memory cache
# fast path; should saturate at ~20–50k req/s per replica.

R1_TABLE="bench_read_${RUN_ID}"
R1_KEY="hot"
R1_VALUE="$(make_value_b64)"
$NATS req --timeout 5s "ldb.put" \
  "{\"table\":\"${R1_TABLE}\",\"key\":\"${R1_KEY}\",\"value\":\"${R1_VALUE}\"}" \
  --raw >/dev/null

build_get() {
  echo "ldb.get {\"table\":\"${R1_TABLE}\",\"key\":\"${R1_KEY}\"}"
}
manual_bench "1. Reads (cache hit, single hot key)" build_get

# ── Scenario 2: writes to a single table ────────────────────────────
# All writes go to one JetStream KV bucket → one Raft leader.
# Adding lattice-db replicas does NOT help this number.

W1_TABLE="bench_write_single_${RUN_ID}"
build_put_single() {
  local i="$1" c="$2"
  local val
  val="$(head -c "$MSG_SIZE" /dev/urandom | base64 | tr -d '\n')"
  echo "ldb.put {\"table\":\"${W1_TABLE}\",\"key\":\"k${c}_${i}\",\"value\":\"${val}\"}"
}
manual_bench "2. Writes (single table, one JS leader)" build_put_single

# ── Scenario 3: writes spread across N tables ───────────────────────
# Each table is a separate JetStream stream → spreads load across leaders.
# Should scale roughly linearly with NTABLES until NATS server CPU/disk
# is saturated.

build_put_multi() {
  local i="$1" c="$2"
  local t=$(( (i + c) % NTABLES ))
  local table="bench_write_multi_${RUN_ID}_${t}"
  local val
  val="$(head -c "$MSG_SIZE" /dev/urandom | base64 | tr -d '\n')"
  echo "ldb.put {\"table\":\"${table}\",\"key\":\"k${c}_${i}\",\"value\":\"${val}\"}"
}
manual_bench "3. Writes (${NTABLES} tables, parallel JS leaders)" build_put_multi

# ── Scenario 4: transactions ────────────────────────────────────────
# Every txn goes through the single global ldb-txn WAL stream — a hard
# cluster-wide bottleneck regardless of replica or table count.

T1_TABLE="bench_txn_${RUN_ID}"
build_txn() {
  local i="$1" c="$2"
  local val
  val="$(head -c "$MSG_SIZE" /dev/urandom | base64 | tr -d '\n')"
  # 2-op txn: a put and a put on different keys.
  local payload
  payload=$(cat <<EOF
{"ops":[
  {"op":"put","table":"${T1_TABLE}","key":"a${c}_${i}","value":"${val}"},
  {"op":"put","table":"${T1_TABLE}","key":"b${c}_${i}","value":"${val}"}
]}
EOF
)
  payload="$(echo "$payload" | tr -d '\n')"
  echo "ldb.txn ${payload}"
}
manual_bench "4. Transactions (global WAL bottleneck)" build_txn

# ── Cleanup hint ────────────────────────────────────────────────────

echo "── done ────────────────────────────────────────────────────────"
echo "Benchmark tables created (use 'nats kv ls' to inspect):"
echo "  ldb-${R1_TABLE}"
echo "  ldb-${W1_TABLE}"
echo "  ldb-${T1_TABLE}"
for t in $(seq 0 $((NTABLES - 1))); do
  echo "  ldb-bench_write_multi_${RUN_ID}_${t}"
done
echo
echo "Delete with: nats kv del <bucket> --force"
