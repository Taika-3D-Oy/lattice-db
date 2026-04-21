#!/usr/bin/env bash
# integration_partitioned.sh — Partitioning integration tests for lattice-db.
#
# Exercises partition isolation across all operations. The lattice-db instance
# MUST be running with LDB_PARTITIONED=1 for these tests.
#
# Requires: nats CLI, jq, base64
#
# Usage:
#   bash tests/integration_partitioned.sh                          # plain, localhost:4222
#   NATS_URL=host:port bash tests/integration_partitioned.sh       # custom address
#   bash tests/integration_partitioned.sh --tls                    # mTLS with certs from K8s secret

set -euo pipefail

NATS_EXTRA_ARGS=""
NATS_URL_SET="${NATS_URL:-}"

# Support --tls flag: extract certs from K8s secret into temp files.
if [[ "${1:-}" == "--tls" ]] || [[ "${NATS_TLS:-}" == "1" ]]; then
  CERT_DIR=$(mktemp -d)
  trap "rm -rf $CERT_DIR" EXIT

  kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.ca\.crt}' | base64 -d > "$CERT_DIR/ca.crt"
  kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.tls\.crt}' | base64 -d > "$CERT_DIR/tls.crt"
  kubectl get secret wasmcloud-data-tls -o jsonpath='{.data.tls\.key}' | base64 -d > "$CERT_DIR/tls.key"

  NATS_EXTRA_ARGS="--tlsca $CERT_DIR/ca.crt --tlscert $CERT_DIR/tls.crt --tlskey $CERT_DIR/tls.key"
  NATS_URL_SET="${NATS_URL_SET:-nats:4222}"
  echo "TLS: using mTLS certs from wasmcloud-data-tls secret (server: ${NATS_URL_SET})"
fi

NATS_URL="${NATS_URL_SET:-localhost:4222}"

NATS="nats -s nats://${NATS_URL} --timeout 5s ${NATS_EXTRA_ARGS}"
TABLE="mt_$(date +%s)"  # shared table name — partitions should NOT see each other

PART_A="acme"
PART_B="globex"

PASS=0
FAIL=0
TOTAL=0

# ── Helpers ──────────────────────────────────────────────────

b64() { echo -n "$1" | base64; }

req() {
  local subject="$1" payload="$2"
  $NATS req "$subject" "$payload" --raw 2>/dev/null
}

# Convenience: inject _partition into a request
req_t() {
  local partition="$1" subject="$2" payload="$3"
  # Inject _partition field into the JSON payload
  local patched
  patched=$(echo "$payload" | jq --arg t "$partition" '. + {"_partition": $t}' -c)
  req "$subject" "$patched"
}

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if [[ "$expected" == "$actual" ]]; then
    echo "  ✓ $desc"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $desc"
    echo "    expected: $expected"
    echo "    actual:   $actual"
    FAIL=$((FAIL + 1))
  fi
}

assert_contains() {
  local desc="$1" needle="$2" haystack="$3"
  TOTAL=$((TOTAL + 1))
  if echo "$haystack" | grep -q "$needle"; then
    echo "  ✓ $desc"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $desc"
    echo "    expected to contain: $needle"
    echo "    actual: $haystack"
    FAIL=$((FAIL + 1))
  fi
}

assert_not_contains() {
  local desc="$1" needle="$2" haystack="$3"
  TOTAL=$((TOTAL + 1))
  if echo "$haystack" | grep -q "$needle"; then
    echo "  ✗ $desc"
    echo "    should NOT contain: $needle"
    echo "    actual: $haystack"
    FAIL=$((FAIL + 1))
  else
    echo "  ✓ $desc"
    PASS=$((PASS + 1))
  fi
}

assert_json() {
  local desc="$1" jq_expr="$2" expected="$3" json="$4"
  local actual
  actual=$(echo "$json" | jq -r "$jq_expr" 2>/dev/null || echo "JQ_ERROR")
  assert_eq "$desc" "$expected" "$actual"
}

# ── Connectivity check ──────────────────────────────────────

echo "=== lattice-db partitioning integration tests ==="
echo "  NATS: ${NATS_URL}"
echo "  Table: ${TABLE}"
echo "  Partitions: ${PART_A}, ${PART_B}"
echo ""

# Quick NATS connectivity check
if ! $NATS pub _ping "" &>/dev/null; then
  echo "ERROR: Cannot connect to NATS at ${NATS_URL}"
  exit 1
fi

# ── 1. PARTITION REQUIRED ──────────────────────────────────────

echo "--- PARTITION FIELD REQUIRED ---"

R=$(req ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}")
assert_contains "put without _partition → error" "_partition field required" "$R"

R=$(req ldb.get "{\"table\":\"$TABLE\",\"key\":\"x\"}")
assert_contains "get without _partition → error" "_partition field required" "$R"

R=$(req ldb.delete "{\"table\":\"$TABLE\",\"key\":\"x\"}")
assert_contains "delete without _partition → error" "_partition field required" "$R"

R=$(req ldb.exists "{\"table\":\"$TABLE\",\"key\":\"x\"}")
assert_contains "exists without _partition → error" "_partition field required" "$R"

R=$(req ldb.keys "{\"table\":\"$TABLE\"}")
assert_contains "keys without _partition → error" "_partition field required" "$R"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[]}")
assert_contains "scan without _partition → error" "_partition field required" "$R"

R=$(req ldb.count "{\"table\":\"$TABLE\",\"filters\":[]}")
assert_contains "count without _partition → error" "_partition field required" "$R"

R=$(req ldb.txn "{\"ops\":[{\"op\":\"put\",\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}]}")
assert_contains "txn without _partition → error" "_partition field required" "$R"

echo ""

# ── 2. PARTITION ID VALIDATION ─────────────────────────────────

echo "--- PARTITION ID VALIDATION ---"

R=$(req_t "" ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}")
assert_contains "empty partition → error" "invalid partition" "$R"

R=$(req_t "a/b" ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}")
assert_contains "partition with slash → error" "invalid partition" "$R"

R=$(req_t "a.b" ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}")
assert_contains "partition with dot → error" "invalid partition" "$R"

R=$(req_t "a b" ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}")
assert_contains "partition with space → error" "invalid partition" "$R"

# 65 chars should fail
LONG_ID=$(printf 'a%.0s' {1..65})
R=$(req_t "$LONG_ID" ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 'test')\"}")
assert_contains "partition too long → error" "invalid partition" "$R"

# Valid partition names
R=$(req_t "valid-partition_01" ldb.put "{\"table\":\"$TABLE\",\"key\":\"x\",\"value\":\"$(b64 '{"v":"ok"}')\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.revision' &>/dev/null; then
  echo "  ✓ valid partition name accepted"
  PASS=$((PASS + 1))
else
  echo "  ✗ valid partition name accepted"
  echo "    response: $R"
  FAIL=$((FAIL + 1))
fi

echo ""

# ── 3. BASIC PUT/GET ISOLATION ───────────────────────────────

echo "--- PUT/GET ISOLATION ---"

ALICE='{"name":"Alice","age":"30","city":"Helsinki"}'
ALICE_B64=$(b64 "$ALICE")

BOB='{"name":"Bob","age":"25","city":"Tampere"}'
BOB_B64=$(b64 "$BOB")

# Partition A puts alice
R=$(req_t "$PART_A" ldb.put "{\"table\":\"$TABLE\",\"key\":\"alice\",\"value\":\"$ALICE_B64\"}")
assert_json "partition A: put alice → revision" '.revision' "1" "$R"

# Partition B puts bob (same table name, should be siloed)
R=$(req_t "$PART_B" ldb.put "{\"table\":\"$TABLE\",\"key\":\"bob\",\"value\":\"$BOB_B64\"}")
assert_json "partition B: put bob → revision" '.revision' "1" "$R"

# Partition A should NOT see bob
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
assert_contains "partition A: get bob → not found" "not found" "$R"

# Partition B should NOT see alice
R=$(req_t "$PART_B" ldb.get "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_contains "partition B: get alice → not found" "not found" "$R"

# Partition A should see alice
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_json "partition A: get alice → key" '.key' "alice" "$R"
GOT_VALUE=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null || echo "DECODE_ERROR")
assert_contains "partition A: alice has correct name" '"name":"Alice"' "$GOT_VALUE"

# Partition B should see bob
R=$(req_t "$PART_B" ldb.get "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
assert_json "partition B: get bob → key" '.key' "bob" "$R"

echo ""

# ── 4. EXISTS ISOLATION ──────────────────────────────────────

echo "--- EXISTS ISOLATION ---"

R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_json "partition A: alice exists" ".exists" "true" "$R"

R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
assert_json "partition A: bob not exists" ".exists" "false" "$R"

R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
assert_json "partition B: bob exists" ".exists" "true" "$R"

R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_json "partition B: alice not exists" ".exists" "false" "$R"

echo ""

# ── 5. KEYS ISOLATION ───────────────────────────────────────

echo "--- KEYS ISOLATION ---"

# Add more data to partition A
R=$(req_t "$PART_A" ldb.put "{\"table\":\"$TABLE\",\"key\":\"carol\",\"value\":\"$(b64 '{"name":"Carol"}')\"}")

R=$(req_t "$PART_A" ldb.keys "{\"table\":\"$TABLE\"}")
A_KEYS=$(echo "$R" | jq '.keys | length')
assert_eq "partition A: keys count = 2" "2" "$A_KEYS"
assert_not_contains "partition A: keys don't include bob" '"bob"' "$R"

R=$(req_t "$PART_B" ldb.keys "{\"table\":\"$TABLE\"}")
B_KEYS=$(echo "$R" | jq '.keys | length')
assert_eq "partition B: keys count = 1" "1" "$B_KEYS"
assert_not_contains "partition B: keys don't include alice" '"alice"' "$R"

echo ""

# ── 6. SCAN ISOLATION ───────────────────────────────────────

echo "--- SCAN ISOLATION ---"

R=$(req_t "$PART_A" ldb.scan "{\"table\":\"$TABLE\",\"filters\":[]}")
SCAN_A=$(echo "$R" | jq '.total_count')
assert_eq "partition A: scan count = 2" "2" "$SCAN_A"

R=$(req_t "$PART_B" ldb.scan "{\"table\":\"$TABLE\",\"filters\":[]}")
SCAN_B=$(echo "$R" | jq '.total_count')
assert_eq "partition B: scan count = 1" "1" "$SCAN_B"

# Filtered scan
R=$(req_t "$PART_A" ldb.scan "{\"table\":\"$TABLE\",\"filters\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Helsinki\"}]}")
SCAN_FILTERED=$(echo "$R" | jq '.total_count')
assert_eq "partition A: scan city=Helsinki = 1" "1" "$SCAN_FILTERED"

# Partition B should get 0 for Helsinki filter (bob is in Tampere)
R=$(req_t "$PART_B" ldb.scan "{\"table\":\"$TABLE\",\"filters\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Helsinki\"}]}")
SCAN_FILTERED=$(echo "$R" | jq '.total_count')
assert_eq "partition B: scan city=Helsinki = 0" "0" "$SCAN_FILTERED"

echo ""

# ── 7. COUNT ISOLATION ──────────────────────────────────────

echo "--- COUNT ISOLATION ---"

R=$(req_t "$PART_A" ldb.count "{\"table\":\"$TABLE\",\"filters\":[]}")
assert_json "partition A: count = 2" ".count" "2" "$R"

R=$(req_t "$PART_B" ldb.count "{\"table\":\"$TABLE\",\"filters\":[]}")
assert_json "partition B: count = 1" ".count" "1" "$R"

echo ""

# ── 8. DELETE ISOLATION ──────────────────────────────────────

echo "--- DELETE ISOLATION ---"

# Partition B tries to delete partition A's key — should be a no-op (key doesn't exist in B's namespace)
R=$(req_t "$PART_B" ldb.delete "{\"table\":\"$TABLE\",\"key\":\"alice\"}")

# Partition A's alice should still exist
R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_json "partition A: alice survives B's delete" ".exists" "true" "$R"

# Partition A deletes carol
R=$(req_t "$PART_A" ldb.delete "{\"table\":\"$TABLE\",\"key\":\"carol\"}")
R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"carol\"}")
assert_json "partition A: carol deleted" ".exists" "false" "$R"

# Partition B's data unaffected
R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
assert_json "partition B: bob still exists after A deletes" ".exists" "true" "$R"

echo ""

# ── 9. CREATE ISOLATION ─────────────────────────────────────

echo "--- CREATE ISOLATION ---"

# Both partitions create the same key name — should succeed independently
R=$(req_t "$PART_A" ldb.create "{\"table\":\"$TABLE\",\"key\":\"shared_key\",\"value\":\"$(b64 '{"owner":"A"}')\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.revision' &>/dev/null; then
  echo "  ✓ partition A: create shared_key"
  PASS=$((PASS + 1))
else
  echo "  ✗ partition A: create shared_key"
  echo "    response: $R"
  FAIL=$((FAIL + 1))
fi

R=$(req_t "$PART_B" ldb.create "{\"table\":\"$TABLE\",\"key\":\"shared_key\",\"value\":\"$(b64 '{"owner":"B"}')\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.revision' &>/dev/null; then
  echo "  ✓ partition B: create same shared_key (no conflict)"
  PASS=$((PASS + 1))
else
  echo "  ✗ partition B: create same shared_key (no conflict)"
  echo "    response: $R"
  FAIL=$((FAIL + 1))
fi

# Verify each partition sees their own value
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$TABLE\",\"key\":\"shared_key\"}")
GOT=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition A: shared_key owned by A" '"owner":"A"' "$GOT"

R=$(req_t "$PART_B" ldb.get "{\"table\":\"$TABLE\",\"key\":\"shared_key\"}")
GOT=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition B: shared_key owned by B" '"owner":"B"' "$GOT"

echo ""

# ── 10. CAS ISOLATION ───────────────────────────────────────

echo "--- CAS ISOLATION ---"

# Get partition A's revision for shared_key
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$TABLE\",\"key\":\"shared_key\"}")
A_REV=$(echo "$R" | jq -r '.revision')

# Partition A CAS with correct revision
R=$(req_t "$PART_A" ldb.cas "{\"table\":\"$TABLE\",\"key\":\"shared_key\",\"value\":\"$(b64 '{"owner":"A","v":2}')\",\"revision\":$A_REV}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.revision' &>/dev/null; then
  echo "  ✓ partition A: CAS shared_key succeeds"
  PASS=$((PASS + 1))
else
  echo "  ✗ partition A: CAS shared_key succeeds"
  echo "    response: $R"
  FAIL=$((FAIL + 1))
fi

# Partition B's shared_key should be unaffected
R=$(req_t "$PART_B" ldb.get "{\"table\":\"$TABLE\",\"key\":\"shared_key\"}")
GOT=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition B: shared_key still owned by B after A's CAS" '"owner":"B"' "$GOT"

echo ""

# ── 11. BATCH GET ISOLATION ─────────────────────────────────

echo "--- BATCH GET ISOLATION ---"

R=$(req_t "$PART_A" ldb.batch.get "{\"table\":\"$TABLE\",\"keys\":[\"alice\",\"bob\",\"shared_key\"]}")
# alice and shared_key should exist, bob should not
A_ALICE=$(echo "$R" | jq -r '.results[] | select(.key == "alice") | .value')
A_BOB_ERR=$(echo "$R" | jq -r '.results[] | select(.key == "bob") | .error')
A_SHARED=$(echo "$R" | jq -r '.results[] | select(.key == "shared_key") | .value')

assert_eq "partition A: batch.get alice has value" "1" "$([ -n "$A_ALICE" ] && [ "$A_ALICE" != "null" ] && echo 1 || echo 0)"
assert_eq "partition A: batch.get bob → not found" "not found" "$A_BOB_ERR"
assert_eq "partition A: batch.get shared_key has value" "1" "$([ -n "$A_SHARED" ] && [ "$A_SHARED" != "null" ] && echo 1 || echo 0)"

echo ""

# ── 12. BATCH PUT ISOLATION ─────────────────────────────────

echo "--- BATCH PUT ISOLATION ---"

R=$(req_t "$PART_A" ldb.batch.put "{\"table\":\"$TABLE\",\"entries\":[
  {\"key\":\"bp1\",\"value\":\"$(b64 '{"v":"A1"}')\"},
  {\"key\":\"bp2\",\"value\":\"$(b64 '{"v":"A2"}')\"}
]}")
RESULT_COUNT=$(echo "$R" | jq '.results | length')
assert_eq "partition A: batch.put 2 entries" "2" "$RESULT_COUNT"

# Partition B should not see these
R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"bp1\"}")
assert_json "partition B: bp1 not exists" ".exists" "false" "$R"

R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TABLE\",\"key\":\"bp2\"}")
assert_json "partition B: bp2 not exists" ".exists" "false" "$R"

echo ""

# ── 13. INDEX ISOLATION ─────────────────────────────────────

echo "--- INDEX ISOLATION ---"

# Partition A creates an index
R=$(req_t "$PART_A" ldb.index.create "{\"table\":\"$TABLE\",\"field\":\"city\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ partition A: create index on city"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ partition A: create index on city"
  PASS=$((PASS + 1))
fi

# Partition A should see 1 index
R=$(req_t "$PART_A" ldb.index.list "{\"table\":\"$TABLE\"}")
A_IDX=$(echo "$R" | jq '.indexes | length')
assert_eq "partition A: index count = 1" "1" "$A_IDX"

# Partition B should see 0 indexes (different namespace)
R=$(req_t "$PART_B" ldb.index.list "{\"table\":\"$TABLE\"}")
B_IDX=$(echo "$R" | jq '.indexes | length')
assert_eq "partition B: index count = 0" "0" "$B_IDX"

# Partition B creates their own index
R=$(req_t "$PART_B" ldb.index.create "{\"table\":\"$TABLE\",\"field\":\"name\"}")
R=$(req_t "$PART_B" ldb.index.list "{\"table\":\"$TABLE\"}")
B_IDX=$(echo "$R" | jq '.indexes | length')
assert_eq "partition B: index count = 1" "1" "$B_IDX"

# Partition A's indexes should still be just 1 (city)
R=$(req_t "$PART_A" ldb.index.list "{\"table\":\"$TABLE\"}")
A_IDX=$(echo "$R" | jq '.indexes | length')
assert_eq "partition A: still 1 index after B's create" "1" "$A_IDX"

echo ""

# ── 14. SCHEMA ISOLATION ────────────────────────────────────

echo "--- SCHEMA ISOLATION ---"

# Partition A sets a schema
SCHEMA_A='{"fields":{"name":{"type":"string","required":true},"age":{"type":"number"}}}'
R=$(req_t "$PART_A" ldb.schema.set "{\"table\":\"$TABLE\",\"schema\":$SCHEMA_A}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ partition A: set schema"
  echo "    response: $R"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ partition A: set schema"
  PASS=$((PASS + 1))
fi

# Partition A's put should now validate
R=$(req_t "$PART_A" ldb.put "{\"table\":\"$TABLE\",\"key\":\"schema_test\",\"value\":\"$(b64 '{"age":25}')\"}")
assert_contains "partition A: missing required name → error" "missing required field" "$R"

# Partition B should NOT have a schema — any value should work
R=$(req_t "$PART_B" ldb.put "{\"table\":\"$TABLE\",\"key\":\"no_schema\",\"value\":\"$(b64 '{"random":"stuff"}')\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ partition B: put without schema accepts anything"
  echo "    response: $R"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ partition B: put without schema accepts anything"
  PASS=$((PASS + 1))
fi

# Partition A: get schema
R=$(req_t "$PART_A" ldb.schema.get "{\"table\":\"$TABLE\"}")
SCHEMA_FIELDS=$(echo "$R" | jq '.schema.fields | keys | length')
assert_eq "partition A: schema has 2 fields" "2" "$SCHEMA_FIELDS"

# Partition B: get schema should be null
R=$(req_t "$PART_B" ldb.schema.get "{\"table\":\"$TABLE\"}")
SCHEMA_VAL=$(echo "$R" | jq -r '.schema')
assert_eq "partition B: no schema" "null" "$SCHEMA_VAL"

# Clean up schema for further tests
req_t "$PART_A" ldb.schema.delete "{\"table\":\"$TABLE\"}" >/dev/null

echo ""

# ── 15. AGGREGATION ISOLATION ────────────────────────────────

echo "--- AGGREGATION ISOLATION ---"

AGG_TABLE="mt_agg_$(date +%s)"

req_t "$PART_A" ldb.put "{\"table\":\"$AGG_TABLE\",\"key\":\"s1\",\"value\":\"$(b64 '{"amount":"100","region":"north"}')\"}" >/dev/null
req_t "$PART_A" ldb.put "{\"table\":\"$AGG_TABLE\",\"key\":\"s2\",\"value\":\"$(b64 '{"amount":"200","region":"south"}')\"}" >/dev/null

req_t "$PART_B" ldb.put "{\"table\":\"$AGG_TABLE\",\"key\":\"s1\",\"value\":\"$(b64 '{"amount":"999","region":"east"}')\"}" >/dev/null

# Partition A: count
R=$(req_t "$PART_A" ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"count\"}]}")
COUNT_A=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "partition A: agg count = 2" "2" "$COUNT_A"

# Partition A: sum
R=$(req_t "$PART_A" ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"sum\",\"field\":\"amount\"}]}")
SUM_A=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "partition A: agg sum = 300" "300" "${SUM_A%.0}"

# Partition B: count
R=$(req_t "$PART_B" ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"count\"}]}")
COUNT_B=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "partition B: agg count = 1" "1" "$COUNT_B"

# Partition B: sum (should only see their 999)
R=$(req_t "$PART_B" ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"sum\",\"field\":\"amount\"}]}")
SUM_B=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "partition B: agg sum = 999" "999" "${SUM_B%.0}"

echo ""

# ── 16. TRANSACTION ISOLATION ────────────────────────────────

echo "--- TRANSACTION ISOLATION ---"

TXN_TABLE="mt_txn_$(date +%s)"

# Partition A: multi-key transaction
R=$(req_t "$PART_A" ldb.txn "{\"ops\":[
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"tx1\",\"value\":\"$(b64 '{"owner":"A"}')\"},
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"tx2\",\"value\":\"$(b64 '{"owner":"A"}')\"}
]}")
TXN_OK=$(echo "$R" | jq -r '.ok')
assert_eq "partition A: txn put tx1+tx2" "true" "$TXN_OK"

# Partition B: transaction with same key names
R=$(req_t "$PART_B" ldb.txn "{\"ops\":[
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"tx1\",\"value\":\"$(b64 '{"owner":"B"}')\"},
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"tx3\",\"value\":\"$(b64 '{"owner":"B"}')\"}
]}")
TXN_OK=$(echo "$R" | jq -r '.ok')
assert_eq "partition B: txn put tx1+tx3" "true" "$TXN_OK"

# Verify isolation: partition A's tx1 should still be owned by A
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$TXN_TABLE\",\"key\":\"tx1\"}")
GOT=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition A: txn tx1 owned by A" '"owner":"A"' "$GOT"

# Partition B's tx1 should be owned by B
R=$(req_t "$PART_B" ldb.get "{\"table\":\"$TXN_TABLE\",\"key\":\"tx1\"}")
GOT=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition B: txn tx1 owned by B" '"owner":"B"' "$GOT"

# Partition A should NOT see tx3
R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TXN_TABLE\",\"key\":\"tx3\"}")
assert_json "partition A: tx3 not visible" ".exists" "false" "$R"

# Partition B should NOT see tx2
R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TXN_TABLE\",\"key\":\"tx2\"}")
assert_json "partition B: tx2 not visible" ".exists" "false" "$R"

# Keys isolation in txn table
R=$(req_t "$PART_A" ldb.keys "{\"table\":\"$TXN_TABLE\"}")
A_TXN_KEYS=$(echo "$R" | jq '.keys | length')
assert_eq "partition A: txn table has 2 keys" "2" "$A_TXN_KEYS"

R=$(req_t "$PART_B" ldb.keys "{\"table\":\"$TXN_TABLE\"}")
B_TXN_KEYS=$(echo "$R" | jq '.keys | length')
assert_eq "partition B: txn table has 2 keys" "2" "$B_TXN_KEYS"

echo ""

# ── 17. TRANSACTION CROSS-TABLE ISOLATION ────────────────────

echo "--- TRANSACTION CROSS-TABLE ---"

# Transaction with ops across multiple tables — all should be prefixed
CROSS_TABLE_1="mt_cross1_$(date +%s)"
CROSS_TABLE_2="mt_cross2_$(date +%s)"

R=$(req_t "$PART_A" ldb.txn "{\"ops\":[
  {\"op\":\"put\",\"table\":\"$CROSS_TABLE_1\",\"key\":\"c1\",\"value\":\"$(b64 '{"v":"cross1"}')\"},
  {\"op\":\"put\",\"table\":\"$CROSS_TABLE_2\",\"key\":\"c2\",\"value\":\"$(b64 '{"v":"cross2"}')\"}
]}")
TXN_OK=$(echo "$R" | jq -r '.ok')
assert_eq "partition A: cross-table txn succeeds" "true" "$TXN_OK"

# Partition B should not see either
R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$CROSS_TABLE_1\",\"key\":\"c1\"}")
assert_json "partition B: cross1.c1 not visible" ".exists" "false" "$R"

R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$CROSS_TABLE_2\",\"key\":\"c2\"}")
assert_json "partition B: cross2.c2 not visible" ".exists" "false" "$R"

echo ""

# ── 18. TRANSACTION ROLLBACK ISOLATION ───────────────────────

echo "--- TRANSACTION ROLLBACK ISOLATION ---"

ROLLBACK_TABLE="mt_rollback_$(date +%s)"

# Partition A creates a key
R=$(req_t "$PART_A" ldb.create "{\"table\":\"$ROLLBACK_TABLE\",\"key\":\"existing\",\"value\":\"$(b64 '{"v":"original"}')\"}")

# Partition A: txn that should rollback (second op create-conflicts)
R=$(req_t "$PART_A" ldb.txn "{\"ops\":[
  {\"op\":\"put\",\"table\":\"$ROLLBACK_TABLE\",\"key\":\"new_key\",\"value\":\"$(b64 '{"v":"new"}')\"},
  {\"op\":\"create\",\"table\":\"$ROLLBACK_TABLE\",\"key\":\"existing\",\"value\":\"$(b64 '{"v":"conflict"}')\"}
]}")
assert_contains "partition A: txn rollback on conflict" "error" "$R"

# new_key should NOT exist (rolled back)
R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$ROLLBACK_TABLE\",\"key\":\"new_key\"}")
assert_json "partition A: new_key rolled back" ".exists" "false" "$R"

# existing should still have original value
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$ROLLBACK_TABLE\",\"key\":\"existing\"}")
GOT=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition A: existing unchanged after rollback" '"v":"original"' "$GOT"

echo ""

# ── 19. TTL ISOLATION ────────────────────────────────────────

echo "--- TTL ISOLATION ---"

TTL_TABLE="mt_ttl_$(date +%s)"

# Partition A: key with 2s TTL
R=$(req_t "$PART_A" ldb.put "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\",\"value\":\"$(b64 '{"v":"temp"}')\",\"ttl_seconds\":2}")
assert_json "partition A: put with TTL" '.revision' "1" "$R"

# Partition B: permanent key with same name
R=$(req_t "$PART_B" ldb.put "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\",\"value\":\"$(b64 '{"v":"permanent"}')\"}")

# Both should exist immediately
R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "partition A: ttl key exists immediately" ".exists" "true" "$R"
R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "partition B: permanent key exists" ".exists" "true" "$R"

echo "  (waiting 3s for TTL expiry...)"
sleep 3

# Partition A's key should be expired
R=$(req_t "$PART_A" ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "partition A: ttl key expired" ".exists" "false" "$R"

# Partition B's key should still exist (no TTL, different namespace)
R=$(req_t "$PART_B" ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "partition B: permanent key survives A's TTL" ".exists" "true" "$R"

echo ""

# ── 20. WATCH EVENTS ISOLATION ───────────────────────────────

echo "--- WATCH EVENTS ISOLATION ---"

WATCH_TABLE="mt_watch_$(date +%s)"
# In partitioned mode, events go to ldb-events.{partition}_{table}.{key}
# Partition A subscribes to their namespace
$NATS sub "ldb-events.${PART_A}_${WATCH_TABLE}.>" --count 1 --raw > /tmp/ldb_mt_events_a.txt 2>/dev/null &
WATCH_A_PID=$!
# Partition B subscribes to their namespace
$NATS sub "ldb-events.${PART_B}_${WATCH_TABLE}.>" --count 1 --raw > /tmp/ldb_mt_events_b.txt 2>/dev/null &
WATCH_B_PID=$!
sleep 0.5

# Only partition A makes a change
req_t "$PART_A" ldb.put "{\"table\":\"$WATCH_TABLE\",\"key\":\"w1\",\"value\":\"$(b64 '{"v":"1"}')\"}" >/dev/null

sleep 1

# Partition A should have received the event
kill $WATCH_A_PID 2>/dev/null || true
wait $WATCH_A_PID 2>/dev/null || true

TOTAL=$((TOTAL + 1))
if grep -q '"op":"put"' /tmp/ldb_mt_events_a.txt 2>/dev/null; then
  echo "  ✓ partition A: received own put event"
  PASS=$((PASS + 1))
else
  echo "  ✗ partition A: received own put event"
  echo "    events: $(cat /tmp/ldb_mt_events_a.txt 2>/dev/null || echo 'none')"
  FAIL=$((FAIL + 1))
fi

# Partition B should NOT have received any event (timeout / empty)
kill $WATCH_B_PID 2>/dev/null || true
wait $WATCH_B_PID 2>/dev/null || true

TOTAL=$((TOTAL + 1))
if [[ ! -s /tmp/ldb_mt_events_b.txt ]] || ! grep -q '"op"' /tmp/ldb_mt_events_b.txt 2>/dev/null; then
  echo "  ✓ partition B: no events from A's writes"
  PASS=$((PASS + 1))
else
  echo "  ✗ partition B: no events from A's writes"
  echo "    events: $(cat /tmp/ldb_mt_events_b.txt 2>/dev/null || echo 'none')"
  FAIL=$((FAIL + 1))
fi

rm -f /tmp/ldb_mt_events_a.txt /tmp/ldb_mt_events_b.txt

echo ""

# ── 21. KEY PREFIX SCAN ISOLATION ────────────────────────────

echo "--- KEY PREFIX SCAN ISOLATION ---"

KP_TABLE="mt_prefix_$(date +%s)"

req_t "$PART_A" ldb.put "{\"table\":\"$KP_TABLE\",\"key\":\"user:alice\",\"value\":\"$(b64 '{"role":"admin"}')\"}" >/dev/null
req_t "$PART_A" ldb.put "{\"table\":\"$KP_TABLE\",\"key\":\"user:carol\",\"value\":\"$(b64 '{"role":"user"}')\"}" >/dev/null
req_t "$PART_B" ldb.put "{\"table\":\"$KP_TABLE\",\"key\":\"user:bob\",\"value\":\"$(b64 '{"role":"admin"}')\"}" >/dev/null

R=$(req_t "$PART_A" ldb.scan "{\"table\":\"$KP_TABLE\",\"filters\":[],\"key_prefix\":\"user:\"}")
assert_eq "partition A: key_prefix user: → 2" "2" "$(echo "$R" | jq '.total_count')"
assert_not_contains "partition A: no bob in prefix scan" '"bob"' "$(echo "$R" | jq -c '.rows[].key')"

R=$(req_t "$PART_B" ldb.scan "{\"table\":\"$KP_TABLE\",\"filters\":[],\"key_prefix\":\"user:\"}")
assert_eq "partition B: key_prefix user: → 1" "1" "$(echo "$R" | jq '.total_count')"

echo ""

# ── 22. COMPOUND INDEX ISOLATION ─────────────────────────────

echo "--- COMPOUND INDEX ISOLATION ---"

CI_TABLE="mt_compound_$(date +%s)"

req_t "$PART_A" ldb.put "{\"table\":\"$CI_TABLE\",\"key\":\"e1\",\"value\":\"$(b64 '{"dept":"eng","level":"senior"}')\"}" >/dev/null
req_t "$PART_B" ldb.put "{\"table\":\"$CI_TABLE\",\"key\":\"e1\",\"value\":\"$(b64 '{"dept":"sales","level":"junior"}')\"}" >/dev/null

R=$(req_t "$PART_A" ldb.index.create "{\"table\":\"$CI_TABLE\",\"fields\":[\"dept\",\"level\"]}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ partition A: create compound index"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ partition A: create compound index"
  PASS=$((PASS + 1))
fi

# Partition B should not have the index
R=$(req_t "$PART_B" ldb.index.list "{\"table\":\"$CI_TABLE\"}")
B_CI=$(echo "$R" | jq '.indexes | length')
assert_eq "partition B: no compound indexes" "0" "$B_CI"

echo ""

# ── 23. SAME TABLE DIFFERENT PARTITION DATA INTEGRITY ──────────

echo "--- DATA INTEGRITY CROSS-CHECK ---"

INTEGRITY_TABLE="mt_integrity_$(date +%s)"

# Write 10 unique keys per partition
for i in $(seq 1 10); do
  VAL_A=$(printf '{"n":"%s","t":"A"}' "$i")
  VAL_B=$(printf '{"n":"%s","t":"B"}' "$((i+100))")
  req_t "$PART_A" ldb.put "{\"table\":\"$INTEGRITY_TABLE\",\"key\":\"k$i\",\"value\":\"$(b64 "$VAL_A")\"}" >/dev/null
  req_t "$PART_B" ldb.put "{\"table\":\"$INTEGRITY_TABLE\",\"key\":\"k$i\",\"value\":\"$(b64 "$VAL_B")\"}" >/dev/null
done

# Verify counts
R=$(req_t "$PART_A" ldb.count "{\"table\":\"$INTEGRITY_TABLE\",\"filters\":[]}")
assert_json "partition A: 10 keys" ".count" "10" "$R"

R=$(req_t "$PART_B" ldb.count "{\"table\":\"$INTEGRITY_TABLE\",\"filters\":[]}")
assert_json "partition B: 10 keys" ".count" "10" "$R"

# Spot check: k5 should have different values
R=$(req_t "$PART_A" ldb.get "{\"table\":\"$INTEGRITY_TABLE\",\"key\":\"k5\"}")
GOT_A=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition A: k5 has t=A" '"t":"A"' "$GOT_A"

R=$(req_t "$PART_B" ldb.get "{\"table\":\"$INTEGRITY_TABLE\",\"key\":\"k5\"}")
GOT_B=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null)
assert_contains "partition B: k5 has t=B" '"t":"B"' "$GOT_B"

# Aggregation: sum should be different
R=$(req_t "$PART_A" ldb.aggregate "{\"table\":\"$INTEGRITY_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"sum\",\"field\":\"n\"}]}")
SUM_A=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "partition A: sum n = 55" "55" "${SUM_A%.0}"

R=$(req_t "$PART_B" ldb.aggregate "{\"table\":\"$INTEGRITY_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"sum\",\"field\":\"n\"}]}")
SUM_B=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "partition B: sum n = 1055" "1055" "${SUM_B%.0}"

echo ""

# ── S-02: PARTITIONED CHANGE-EVENT SUBJECT SCOPING ───────────

echo "--- S-02: CHANGE-EVENT SUBJECT NAMESPACING ---"

EVT_TABLE="evt_$(date +%s)"

# Subscribe ONLY to partition A's event subtree.
$NATS sub "ldb-events.${PART_A}.>" --count 1 --raw > /tmp/ldb_part_a_events.txt 2>/dev/null &
SUB_A_PID=$!

# Subscribe ONLY to partition B's event subtree.
$NATS sub "ldb-events.${PART_B}.>" --count 1 --raw > /tmp/ldb_part_b_events.txt 2>/dev/null &
SUB_B_PID=$!

sleep 0.5

# Partition A writes; only subscriber A should receive it.
req_t "$PART_A" ldb.put "{\"table\":\"$EVT_TABLE\",\"key\":\"a1\",\"value\":\"$(b64 '{"v":"A"}')\"}" >/dev/null

# Partition B writes; only subscriber B should receive it.
req_t "$PART_B" ldb.put "{\"table\":\"$EVT_TABLE\",\"key\":\"b1\",\"value\":\"$(b64 '{"v":"B"}')\"}" >/dev/null

sleep 1
kill $SUB_A_PID $SUB_B_PID 2>/dev/null || true
wait $SUB_A_PID $SUB_B_PID 2>/dev/null || true

A_EVENTS=$(cat /tmp/ldb_part_a_events.txt 2>/dev/null || echo "")
B_EVENTS=$(cat /tmp/ldb_part_b_events.txt 2>/dev/null || echo "")

# Subscriber A should see its own write.
TOTAL=$((TOTAL + 1))
if echo "$A_EVENTS" | grep -q '"key":"a1"'; then
  echo "  ✓ S-02: partition A subscriber received its own event"
  PASS=$((PASS + 1))
else
  echo "  ✗ S-02: partition A subscriber received its own event"
  echo "    events: $A_EVENTS"
  FAIL=$((FAIL + 1))
fi

# Subscriber B should see its own write.
TOTAL=$((TOTAL + 1))
if echo "$B_EVENTS" | grep -q '"key":"b1"'; then
  echo "  ✓ S-02: partition B subscriber received its own event"
  PASS=$((PASS + 1))
else
  echo "  ✗ S-02: partition B subscriber received its own event"
  FAIL=$((FAIL + 1))
fi

# Cross-partition leakage check: A must NOT see B's event and vice versa.
TOTAL=$((TOTAL + 1))
if echo "$A_EVENTS" | grep -q '"key":"b1"'; then
  echo "  ✗ S-02: partition A leaked partition B's event"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ S-02: partition A did NOT see partition B's event"
  PASS=$((PASS + 1))
fi

TOTAL=$((TOTAL + 1))
if echo "$B_EVENTS" | grep -q '"key":"a1"'; then
  echo "  ✗ S-02: partition B leaked partition A's event"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ S-02: partition B did NOT see partition A's event"
  PASS=$((PASS + 1))
fi

rm -f /tmp/ldb_part_a_events.txt /tmp/ldb_part_b_events.txt

echo ""

# ── Summary ──────────────────────────────────────────────────

echo "==================================="
echo "  Results: ${PASS} passed, ${FAIL} failed, ${TOTAL} total"
echo "==================================="

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
