#!/usr/bin/env bash
# integration.sh — Integration tests for lattice-db storage service.
#
# Exercises all 13 NATS operations against a running lattice-db instance.
# Requires: nats CLI, jq, base64
#
# Usage:
#   bash tests/integration.sh                          # plain, localhost:4222
#   NATS_URL=host:port bash tests/integration.sh       # custom address
#   bash tests/integration.sh --tls                    # mTLS with certs from K8s secret

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
  # Server cert is valid for "nats" not "localhost"; use nats:4222 (requires /etc/hosts 127.0.0.1 nats)
  NATS_URL_SET="${NATS_URL_SET:-nats:4222}"
  echo "TLS: using mTLS certs from wasmcloud-data-tls secret (server: ${NATS_URL_SET})"
fi

NATS_URL="${NATS_URL_SET:-localhost:4222}"

NATS="nats -s nats://${NATS_URL} --timeout 5s ${NATS_EXTRA_ARGS}"
TABLE="test_$(date +%s)"  # unique table per run to avoid collisions

PASS=0
FAIL=0
TOTAL=0

# ── Helpers ──────────────────────────────────────────────────

b64() { echo -n "$1" | base64; }

req() {
  local subject="$1" payload="$2"
  $NATS req "$subject" "$payload" --raw 2>/dev/null
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

assert_json() {
  local desc="$1" jq_expr="$2" expected="$3" json="$4"
  local actual
  actual=$(echo "$json" | jq -r "$jq_expr" 2>/dev/null || echo "JQ_ERROR")
  assert_eq "$desc" "$expected" "$actual"
}

# ── Connectivity check ──────────────────────────────────────

echo "=== lattice-db integration tests ==="
echo "  NATS: ${NATS_URL}"
echo "  Table: ${TABLE}"
echo ""

# Quick NATS connectivity check
if ! $NATS pub _ping "" &>/dev/null; then
  echo "ERROR: Cannot connect to NATS at ${NATS_URL}"
  exit 1
fi

# ── 1. PUT ───────────────────────────────────────────────────

echo "--- PUT ---"

ALICE='{"name":"Alice","age":"30","city":"Helsinki"}'
ALICE_B64=$(b64 "$ALICE")

R=$(req ldb.put "{\"table\":\"$TABLE\",\"key\":\"alice\",\"value\":\"$ALICE_B64\"}")
assert_json "put alice returns revision" ".revision" "1" "$R"

BOB='{"name":"Bob","age":"25","city":"Tampere"}'
BOB_B64=$(b64 "$BOB")

R=$(req ldb.put "{\"table\":\"$TABLE\",\"key\":\"bob\",\"value\":\"$BOB_B64\"}")
BOB_REV=$(echo "$R" | jq -r '.revision')
assert_eq "put bob returns revision > 0" "1" "$([ "$BOB_REV" -gt 0 ] && echo 1 || echo 0)"

CAROL='{"name":"Carol","age":"35","city":"Helsinki"}'
CAROL_B64=$(b64 "$CAROL")

R=$(req ldb.put "{\"table\":\"$TABLE\",\"key\":\"carol\",\"value\":\"$CAROL_B64\"}")
CAROL_REV=$(echo "$R" | jq -r '.revision')
assert_eq "put carol returns revision > 0" "1" "$([ "$CAROL_REV" -gt 0 ] && echo 1 || echo 0)"

echo ""

# ── 2. GET ───────────────────────────────────────────────────

echo "--- GET ---"

R=$(req ldb.get "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_json "get alice key" ".key" "alice" "$R"
ALICE_REV=$(echo "$R" | jq -r '.revision')
assert_eq "get alice revision > 0" "1" "$([ "$ALICE_REV" -gt 0 ] && echo 1 || echo 0)"

# Decode the value and verify
GOT_VALUE=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null || echo "DECODE_ERROR")
assert_contains "get alice value has name" '"name":"Alice"' "$GOT_VALUE"

echo ""

# ── 3. GET (not found) ──────────────────────────────────────

echo "--- GET (not found) ---"

R=$(req ldb.get "{\"table\":\"$TABLE\",\"key\":\"nonexistent\"}")
assert_contains "get nonexistent returns error" "not found" "$R"

echo ""

# ── 4. EXISTS ────────────────────────────────────────────────

echo "--- EXISTS ---"

R=$(req ldb.exists "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
assert_json "exists alice → true" ".exists" "true" "$R"

R=$(req ldb.exists "{\"table\":\"$TABLE\",\"key\":\"nobody\"}")
assert_json "exists nobody → false" ".exists" "false" "$R"

echo ""

# ── 5. KEYS ─────────────────────────────────────────────────

echo "--- KEYS ---"

R=$(req ldb.keys "{\"table\":\"$TABLE\"}")
KEY_COUNT=$(echo "$R" | jq '.keys | length')
assert_eq "keys returns 3 keys" "3" "$KEY_COUNT"

# Keys should be sorted
FIRST_KEY=$(echo "$R" | jq -r '.keys[0]')
assert_eq "keys sorted: first is alice" "alice" "$FIRST_KEY"

echo ""

# ── 6. PUT (overwrite) ──────────────────────────────────────

echo "--- PUT (overwrite) ---"

ALICE2='{"name":"Alice","age":"31","city":"Espoo"}'
ALICE2_B64=$(b64 "$ALICE2")

R=$(req ldb.put "{\"table\":\"$TABLE\",\"key\":\"alice\",\"value\":\"$ALICE2_B64\"}")
REV2=$(echo "$R" | jq -r '.revision')
assert_eq "overwrite alice returns revision > 0" "1" "$([ "$REV2" -gt 0 ] && echo 1 || echo 0)"

# Verify the value changed
R=$(req ldb.get "{\"table\":\"$TABLE\",\"key\":\"alice\"}")
GOT_VALUE=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null || echo "DECODE_ERROR")
assert_contains "overwritten alice has Espoo" '"city":"Espoo"' "$GOT_VALUE"

echo ""

# ── 7. DELETE ────────────────────────────────────────────────

echo "--- DELETE ---"

R=$(req ldb.delete "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
# Should not be an error
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ delete bob"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ delete bob"
  PASS=$((PASS + 1))
fi

# Verify bob is gone
R=$(req ldb.exists "{\"table\":\"$TABLE\",\"key\":\"bob\"}")
assert_json "bob no longer exists" ".exists" "false" "$R"

R=$(req ldb.keys "{\"table\":\"$TABLE\"}")
KEY_COUNT=$(echo "$R" | jq '.keys | length')
assert_eq "keys now has 2" "2" "$KEY_COUNT"

echo ""

# ── 8. CREATE (key must not exist) ──────────────────────────

echo "--- CREATE ---"

DAVE='{"name":"Dave","age":"28","city":"Turku"}'
DAVE_B64=$(b64 "$DAVE")

R=$(req ldb.create "{\"table\":\"$TABLE\",\"key\":\"dave\",\"value\":\"$DAVE_B64\"}")
DAVE_CREATE_REV=$(echo "$R" | jq -r '.revision')
assert_eq "create dave returns revision > 0" "1" "$([ "$DAVE_CREATE_REV" -gt 0 ] && echo 1 || echo 0)"

# Creating same key again should fail
R=$(req ldb.create "{\"table\":\"$TABLE\",\"key\":\"dave\",\"value\":\"$DAVE_B64\"}")
assert_contains "create dave again → error" "error" "$R"

echo ""

# ── 9. CAS (compare-and-swap) ───────────────────────────────

echo "--- CAS ---"

# Get dave's current revision
R=$(req ldb.get "{\"table\":\"$TABLE\",\"key\":\"dave\"}")
DAVE_REV=$(echo "$R" | jq -r '.revision')

DAVE2='{"name":"Dave","age":"29","city":"Turku"}'
DAVE2_B64=$(b64 "$DAVE2")

# CAS with correct revision should succeed
R=$(req ldb.cas "{\"table\":\"$TABLE\",\"key\":\"dave\",\"value\":\"$DAVE2_B64\",\"revision\":$DAVE_REV}")
# CAS should return a revision (any number > 0 means success)
CAS_OK=$(echo "$R" | jq -r 'if .revision then "ok" else "fail" end')
assert_eq "cas dave with correct rev succeeds" "ok" "$CAS_OK"
# Just check it has a revision field (value will be > DAVE_REV)
CAS_REV=$(echo "$R" | jq -r '.revision')
TOTAL=$((TOTAL + 1))
if [[ "$CAS_REV" =~ ^[0-9]+$ ]] && [[ "$CAS_REV" -gt 0 ]]; then
  echo "  ✓ cas returns new revision ($CAS_REV)"
  PASS=$((PASS + 1))
else
  echo "  ✗ cas returns new revision"
  echo "    got: $CAS_REV"
  FAIL=$((FAIL + 1))
fi

# CAS with stale revision should fail
R=$(req ldb.cas "{\"table\":\"$TABLE\",\"key\":\"dave\",\"value\":\"$DAVE2_B64\",\"revision\":$DAVE_REV}")
assert_contains "cas dave with stale rev → error" "error" "$R"

echo ""

# ── 10. INDEX CREATE / LIST / DROP ──────────────────────────

echo "--- INDEX ---"

# Create index on "city"
R=$(req ldb.index.create "{\"table\":\"$TABLE\",\"field\":\"city\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ create index on city"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ create index on city"
  PASS=$((PASS + 1))
fi

# Create index on "name"
req ldb.index.create "{\"table\":\"$TABLE\",\"field\":\"name\"}" >/dev/null

# List indexes
R=$(req ldb.index.list "{\"table\":\"$TABLE\"}")
IDX_COUNT=$(echo "$R" | jq '.indexes | length')
assert_eq "index list has 2 indexes" "2" "$IDX_COUNT"

# Drop the name index
req ldb.index.drop "{\"table\":\"$TABLE\",\"field\":\"name\"}" >/dev/null

R=$(req ldb.index.list "{\"table\":\"$TABLE\"}")
IDX_COUNT=$(echo "$R" | jq '.indexes | length')
assert_eq "after drop, 1 index remains" "1" "$IDX_COUNT"

echo ""

# ── 11. SCAN (full) ─────────────────────────────────────────

echo "--- SCAN ---"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[]}")
SCAN_COUNT=$(echo "$R" | jq '.rows | length')
SCAN_TOTAL=$(echo "$R" | jq '.total_count')
assert_eq "scan all returns 3 rows" "3" "$SCAN_COUNT"
assert_eq "scan all total_count is 3" "3" "$SCAN_TOTAL"

echo ""

# ── 12. SCAN (filtered — eq) ────────────────────────────────

echo "--- SCAN (filtered) ---"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Espoo\"}]}")
SCAN_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "scan city=Espoo returns 1" "1" "$SCAN_COUNT"

MATCHED_KEY=$(echo "$R" | jq -r '.rows[0].key')
assert_eq "scan city=Espoo → alice" "alice" "$MATCHED_KEY"

echo ""

# ── 13. SCAN (prefix filter) ────────────────────────────────

echo "--- SCAN (prefix) ---"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[{\"field\":\"city\",\"op\":\"prefix\",\"value\":\"Tu\"}]}")
SCAN_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "scan city prefix 'Tu' returns 1 (Turku)" "1" "$SCAN_COUNT"

echo ""

# ── 14. SCAN (ordered + paginated) ──────────────────────────

echo "--- SCAN (sorted + paginated) ---"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[],\"order_by\":{\"field\":\"name\",\"order\":\"asc\"},\"limit\":2,\"offset\":0}")
PAGE1_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "page 1 has 2 rows" "2" "$PAGE1_COUNT"
assert_eq "total_count still 3" "3" "$(echo "$R" | jq '.total_count')"

FIRST=$(echo "$R" | jq -r '.rows[0].key')
assert_eq "sorted asc, first is alice" "alice" "$FIRST"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[],\"order_by\":{\"field\":\"name\",\"order\":\"asc\"},\"limit\":2,\"offset\":2}")
PAGE2_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "page 2 has 1 row" "1" "$PAGE2_COUNT"

echo ""

# ── 15. SCAN (desc order) ───────────────────────────────────

echo "--- SCAN (desc order) ---"

R=$(req ldb.scan "{\"table\":\"$TABLE\",\"filters\":[],\"order_by\":{\"field\":\"name\",\"order\":\"desc\"},\"limit\":1}")
FIRST=$(echo "$R" | jq -r '.rows[0].key')
assert_eq "sorted desc, first is dave" "dave" "$FIRST"

echo ""

# ── 16. COUNT ────────────────────────────────────────────────

echo "--- COUNT ---"

R=$(req ldb.count "{\"table\":\"$TABLE\",\"filters\":[]}")
assert_json "count all → 3" ".count" "3" "$R"

R=$(req ldb.count "{\"table\":\"$TABLE\",\"filters\":[{\"field\":\"city\",\"op\":\"eq\",\"value\":\"Espoo\"}]}")
assert_json "count city=Espoo → 1" ".count" "1" "$R"

R=$(req ldb.count "{\"table\":\"$TABLE\",\"filters\":[{\"field\":\"city\",\"op\":\"neq\",\"value\":\"Espoo\"}]}")
assert_json "count city!=Espoo → 2" ".count" "2" "$R"

echo ""

# ── 17. UNKNOWN OP ───────────────────────────────────────────

echo "--- ERROR PATHS ---"

R=$(req ldb.foobar '{"table":"x","key":"y"}')
assert_contains "unknown op returns error" "unknown operation" "$R"

R=$(req ldb.get 'not json')
assert_contains "malformed JSON returns error" "invalid request" "$R"

echo ""

# ── 18. SEPARATE TABLE ISOLATION ─────────────────────────────

echo "--- TABLE ISOLATION ---"

TABLE2="test_other_$(date +%s)"
R=$(req ldb.keys "{\"table\":\"$TABLE2\"}")
KEY_COUNT=$(echo "$R" | jq '.keys | length')
assert_eq "different table has 0 keys" "0" "$KEY_COUNT"

echo ""

# ── 19. TRANSACTIONS ────────────────────────────────────────

echo "--- TRANSACTIONS (basic) ---"

TXN_TABLE="txn_test_$(date +%s)"

# Multi-key put transaction
R=$(req ldb.txn "{\"ops\":[
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"x\",\"value\":\"$(b64 '{"v":"X"}')\"} ,
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"y\",\"value\":\"$(b64 '{"v":"Y"}')\"}
]}")
TXN_OK=$(echo "$R" | jq -r '.ok')
assert_eq "txn put x+y succeeds" "true" "$TXN_OK"
TXN_RESULTS=$(echo "$R" | jq '.results | length')
assert_eq "txn returns 2 results" "2" "$TXN_RESULTS"

# Verify both keys exist
R=$(req ldb.get "{\"table\":\"$TXN_TABLE\",\"key\":\"x\"}")
assert_json "txn: x exists after commit" ".key" "x" "$R"
R=$(req ldb.get "{\"table\":\"$TXN_TABLE\",\"key\":\"y\"}")
assert_json "txn: y exists after commit" ".key" "y" "$R"

echo ""

echo "--- TRANSACTIONS (create + delete) ---"

# Mix of create and delete in one transaction
R=$(req ldb.txn "{\"ops\":[
  {\"op\":\"create\",\"table\":\"$TXN_TABLE\",\"key\":\"z\",\"value\":\"$(b64 '{"v":"Z"}')\"},
  {\"op\":\"delete\",\"table\":\"$TXN_TABLE\",\"key\":\"x\"}
]}")
TXN_OK=$(echo "$R" | jq -r '.ok')
assert_eq "txn create z + delete x succeeds" "true" "$TXN_OK"

# z should exist, x should not
R=$(req ldb.exists "{\"table\":\"$TXN_TABLE\",\"key\":\"z\"}")
assert_json "txn: z exists" ".exists" "true" "$R"
R=$(req ldb.exists "{\"table\":\"$TXN_TABLE\",\"key\":\"x\"}")
assert_json "txn: x deleted" ".exists" "false" "$R"

echo ""

echo "--- TRANSACTIONS (rollback on conflict) ---"

# Create a key, then try a txn that creates it again — should fail and rollback
R=$(req ldb.txn "{\"ops\":[
  {\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"a\",\"value\":\"$(b64 '{"v":"A"}')\"},
  {\"op\":\"create\",\"table\":\"$TXN_TABLE\",\"key\":\"z\",\"value\":\"$(b64 '{"v":"Z2"}')\"}
]}")
assert_contains "txn fails when create conflicts" "error" "$R"

# Key 'a' should NOT exist because the txn was rolled back
R=$(req ldb.exists "{\"table\":\"$TXN_TABLE\",\"key\":\"a\"}")
assert_json "txn: a rolled back (does not exist)" ".exists" "false" "$R"

# Key 'z' should still have original value
R=$(req ldb.get "{\"table\":\"$TXN_TABLE\",\"key\":\"z\"}")
GOT_Z=$(echo "$R" | jq -r '.value' | base64 -d 2>/dev/null || echo "DECODE_ERROR")
assert_contains "txn: z unchanged after rollback" '"v":"Z"' "$GOT_Z"

echo ""

echo "--- TRANSACTIONS (validation) ---"

# Empty transaction
R=$(req ldb.txn '{"ops":[]}')
assert_contains "txn empty ops → error" "error" "$R"

# Bad op type
R=$(req ldb.txn "{\"ops\":[{\"op\":\"drop_table\",\"table\":\"$TXN_TABLE\",\"key\":\"a\"}]}")
assert_contains "txn bad op type → error" "error" "$R"

# Put without value
R=$(req ldb.txn "{\"ops\":[{\"op\":\"put\",\"table\":\"$TXN_TABLE\",\"key\":\"a\"}]}")
assert_contains "txn put without value → error" "error" "$R"

echo ""

# ── 20. TTL / EXPIRY ────────────────────────────────────────

echo "--- TTL / EXPIRY ---"

TTL_TABLE="ttl_test_$(date +%s)"

# Put with TTL=2 seconds
R=$(req ldb.put "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\",\"value\":\"$(b64 '{"v":"temp"}')\",\"ttl_seconds\":2}")
assert_json "put with TTL returns revision" ".revision" "1" "$R"

# Key should exist immediately
R=$(req ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "ttl: exists immediately" ".exists" "true" "$R"

# Get should work immediately
R=$(req ldb.get "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "ttl: get works immediately" ".key" "ephemeral" "$R"

# Put a non-TTL key for comparison
R=$(req ldb.put "{\"table\":\"$TTL_TABLE\",\"key\":\"permanent\",\"value\":\"$(b64 '{"v":"stays"}')\"}")
assert_json "put permanent key" ".revision" "1" "$([ "$(echo "$R" | jq -r '.revision')" -gt 0 ] && echo '{"revision":1}' || echo '{"revision":0}')"

# Wait for TTL to expire
echo "  (waiting 3s for TTL expiry...)"
sleep 3

# Key should be expired now
R=$(req ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_json "ttl: expired after 3s" ".exists" "false" "$R"

R=$(req ldb.get "{\"table\":\"$TTL_TABLE\",\"key\":\"ephemeral\"}")
assert_contains "ttl: get expired → not found" "not found" "$R"

# Permanent key should still exist
R=$(req ldb.exists "{\"table\":\"$TTL_TABLE\",\"key\":\"permanent\"}")
assert_json "ttl: permanent key still exists" ".exists" "true" "$R"

# Keys should not include expired entries
R=$(req ldb.keys "{\"table\":\"$TTL_TABLE\"}")
KEY_COUNT=$(echo "$R" | jq '.keys | length')
assert_eq "ttl: keys excludes expired" "1" "$KEY_COUNT"

# Count should not include expired
R=$(req ldb.count "{\"table\":\"$TTL_TABLE\",\"filters\":[]}")
assert_json "ttl: count excludes expired" ".count" "1" "$R"

echo ""

# ── 21. BATCH GET ────────────────────────────────────────────

echo "--- BATCH GET ---"

BATCH_TABLE="batch_test_$(date +%s)"

# Seed some data
req ldb.put "{\"table\":\"$BATCH_TABLE\",\"key\":\"b1\",\"value\":\"$(b64 '{"n":"one"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$BATCH_TABLE\",\"key\":\"b2\",\"value\":\"$(b64 '{"n":"two"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$BATCH_TABLE\",\"key\":\"b3\",\"value\":\"$(b64 '{"n":"three"}')\"}" >/dev/null

R=$(req ldb.batch.get "{\"table\":\"$BATCH_TABLE\",\"keys\":[\"b1\",\"b2\",\"missing\"]}")
RESULT_COUNT=$(echo "$R" | jq '.results | length')
assert_eq "batch.get returns 3 results" "3" "$RESULT_COUNT"

# b1 should have value
B1_VAL=$(echo "$R" | jq -r '.results[0].value' | base64 -d 2>/dev/null)
assert_contains "batch.get b1 has value" '"n":"one"' "$B1_VAL"

# missing should have error
MISSING_ERR=$(echo "$R" | jq -r '.results[2].error')
assert_eq "batch.get missing → error" "not found" "$MISSING_ERR"

echo ""

# ── 22. BATCH PUT ────────────────────────────────────────────

echo "--- BATCH PUT ---"

R=$(req ldb.batch.put "{\"table\":\"$BATCH_TABLE\",\"entries\":[
  {\"key\":\"b4\",\"value\":\"$(b64 '{"n":"four"}')\"},
  {\"key\":\"b5\",\"value\":\"$(b64 '{"n":"five"}')\"}
]}")
RESULT_COUNT=$(echo "$R" | jq '.results | length')
assert_eq "batch.put returns 2 results" "2" "$RESULT_COUNT"

B4_REV=$(echo "$R" | jq -r '.results[0].revision')
assert_eq "batch.put b4 has revision > 0" "1" "$([ "$B4_REV" -gt 0 ] && echo 1 || echo 0)"

# Verify keys exist
R=$(req ldb.exists "{\"table\":\"$BATCH_TABLE\",\"key\":\"b4\"}")
assert_json "batch.put b4 exists" ".exists" "true" "$R"
R=$(req ldb.exists "{\"table\":\"$BATCH_TABLE\",\"key\":\"b5\"}")
assert_json "batch.put b5 exists" ".exists" "true" "$R"

echo ""

# ── 23. KEY PREFIX SCAN ─────────────────────────────────────

echo "--- KEY PREFIX SCAN ---"

PREFIX_TABLE="prefix_test_$(date +%s)"
req ldb.put "{\"table\":\"$PREFIX_TABLE\",\"key\":\"user:alice\",\"value\":\"$(b64 '{"role":"admin"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$PREFIX_TABLE\",\"key\":\"user:bob\",\"value\":\"$(b64 '{"role":"user"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$PREFIX_TABLE\",\"key\":\"order:001\",\"value\":\"$(b64 '{"total":"50"}')\"}" >/dev/null

R=$(req ldb.scan "{\"table\":\"$PREFIX_TABLE\",\"filters\":[],\"key_prefix\":\"user:\"}")
SCAN_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "key_prefix 'user:' returns 2" "2" "$SCAN_COUNT"
TOTAL=$(echo "$R" | jq '.total_count')
assert_eq "key_prefix total_count 2" "2" "$TOTAL"

R=$(req ldb.scan "{\"table\":\"$PREFIX_TABLE\",\"filters\":[],\"key_prefix\":\"order:\"}")
SCAN_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "key_prefix 'order:' returns 1" "1" "$SCAN_COUNT"

echo ""

# ── 24. COMPOUND INDEXES ────────────────────────────────────

echo "--- COMPOUND INDEXES ---"

CI_TABLE="compound_test_$(date +%s)"
req ldb.put "{\"table\":\"$CI_TABLE\",\"key\":\"e1\",\"value\":\"$(b64 '{"dept":"eng","level":"senior"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$CI_TABLE\",\"key\":\"e2\",\"value\":\"$(b64 '{"dept":"eng","level":"junior"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$CI_TABLE\",\"key\":\"e3\",\"value\":\"$(b64 '{"dept":"sales","level":"senior"}')\"}" >/dev/null

# Create compound index
R=$(req ldb.index.create "{\"table\":\"$CI_TABLE\",\"fields\":[\"dept\",\"level\"]}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ create compound index"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ create compound index dept+level"
  PASS=$((PASS + 1))
fi

# List should show compound index
R=$(req ldb.index.list "{\"table\":\"$CI_TABLE\"}")
assert_contains "compound index listed" "dept+level" "$R"

# Drop compound index
req ldb.index.drop "{\"table\":\"$CI_TABLE\",\"field\":\"dept+level\"}" >/dev/null
R=$(req ldb.index.list "{\"table\":\"$CI_TABLE\"}")
IDX_COUNT=$(echo "$R" | jq '.indexes | length')
assert_eq "compound index dropped" "0" "$IDX_COUNT"

echo ""

# ── 25. AGGREGATION ─────────────────────────────────────────

echo "--- AGGREGATION ---"

AGG_TABLE="agg_test_$(date +%s)"
req ldb.put "{\"table\":\"$AGG_TABLE\",\"key\":\"s1\",\"value\":\"$(b64 '{"product":"A","amount":"100","region":"north"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$AGG_TABLE\",\"key\":\"s2\",\"value\":\"$(b64 '{"product":"B","amount":"200","region":"south"}')\"}" >/dev/null
req ldb.put "{\"table\":\"$AGG_TABLE\",\"key\":\"s3\",\"value\":\"$(b64 '{"product":"A","amount":"150","region":"south"}')\"}" >/dev/null

# Count all
R=$(req ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"count\"}]}")
COUNT_VAL=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "agg count all = 3" "3" "$COUNT_VAL"

# Sum amount
R=$(req ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"sum\",\"field\":\"amount\"}]}")
SUM_VAL=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "agg sum amount = 450" "450" "${SUM_VAL%.0}"

# Avg amount
R=$(req ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"avg\",\"field\":\"amount\"}]}")
AVG_VAL=$(echo "$R" | jq '.groups[0].results[0].value')
assert_eq "agg avg amount = 150" "150" "${AVG_VAL%.0}"

# Min/Max
R=$(req ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"ops\":[{\"fn\":\"min\",\"field\":\"amount\"},{\"fn\":\"max\",\"field\":\"amount\"}]}")
MIN_VAL=$(echo "$R" | jq -r '.groups[0].results[0].value')
MAX_VAL=$(echo "$R" | jq -r '.groups[0].results[1].value')
assert_eq "agg min amount = 100" "100" "$MIN_VAL"
assert_eq "agg max amount = 200" "200" "$MAX_VAL"

# Group by region
R=$(req ldb.aggregate "{\"table\":\"$AGG_TABLE\",\"filters\":[],\"group_by\":\"region\",\"ops\":[{\"fn\":\"count\"},{\"fn\":\"sum\",\"field\":\"amount\"}]}")
GROUP_COUNT=$(echo "$R" | jq '.groups | length')
assert_eq "agg group_by region → 2 groups" "2" "$GROUP_COUNT"

# North group: 1 record, sum=100
NORTH=$(echo "$R" | jq '.groups[] | select(.key == "north")')
NORTH_COUNT=$(echo "$NORTH" | jq '.results[0].value')
NORTH_SUM=$(echo "$NORTH" | jq '.results[1].value')
assert_eq "agg north count = 1" "1" "$NORTH_COUNT"
assert_eq "agg north sum = 100" "100" "${NORTH_SUM%.0}"

# South group: 2 records, sum=350
SOUTH=$(echo "$R" | jq '.groups[] | select(.key == "south")')
SOUTH_COUNT=$(echo "$SOUTH" | jq '.results[0].value')
SOUTH_SUM=$(echo "$SOUTH" | jq '.results[1].value')
assert_eq "agg south count = 2" "2" "$SOUTH_COUNT"
assert_eq "agg south sum = 350" "350" "${SOUTH_SUM%.0}"

echo ""

# ── 26. SCHEMA VALIDATION ───────────────────────────────────

echo "--- SCHEMA VALIDATION ---"

SCHEMA_TABLE="schema_test_$(date +%s)"

# Set schema
SCHEMA='{"fields":{"name":{"type":"string","required":true},"age":{"type":"number"},"active":{"type":"boolean"}}}'
R=$(req ldb.schema.set "{\"table\":\"$SCHEMA_TABLE\",\"schema\":$SCHEMA}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ set schema"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ set schema"
  PASS=$((PASS + 1))
fi

# Get schema
R=$(req ldb.schema.get "{\"table\":\"$SCHEMA_TABLE\"}")
SCHEMA_FIELDS=$(echo "$R" | jq '.schema.fields | keys | length')
assert_eq "get schema has 3 fields" "3" "$SCHEMA_FIELDS"

# Valid put (matches schema)
R=$(req ldb.put "{\"table\":\"$SCHEMA_TABLE\",\"key\":\"valid\",\"value\":\"$(b64 '{"name":"Alice","age":30,"active":true}')\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ valid put accepted"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ valid put accepted"
  PASS=$((PASS + 1))
fi

# Invalid put: missing required field "name"
R=$(req ldb.put "{\"table\":\"$SCHEMA_TABLE\",\"key\":\"bad1\",\"value\":\"$(b64 '{"age":25}')\"}")
assert_contains "schema: missing required field → error" "missing required field" "$R"

# Invalid put: wrong type for "age" (string instead of number)
R=$(req ldb.put "{\"table\":\"$SCHEMA_TABLE\",\"key\":\"bad2\",\"value\":\"$(b64 '{"name":"Bob","age":"old"}')\"}")
assert_contains "schema: wrong type → error" "expected type" "$R"

# Delete schema
R=$(req ldb.schema.delete "{\"table\":\"$SCHEMA_TABLE\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ delete schema"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ delete schema"
  PASS=$((PASS + 1))
fi

# After schema removed, any value should work
R=$(req ldb.put "{\"table\":\"$SCHEMA_TABLE\",\"key\":\"anything\",\"value\":\"$(b64 '{"random":"stuff"}')\"}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ put after schema delete accepted"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ put after schema delete accepted"
  PASS=$((PASS + 1))
fi

echo ""

# ── 27. WATCH / CHANGE EVENTS ───────────────────────────────

echo "--- WATCH / CHANGE EVENTS ---"

WATCH_TABLE="watch_test_$(date +%s)"
WATCH_INBOX="_INBOX.watch.$(date +%s)"

# Subscribe to change events in background, collect 2 events
$NATS sub "ldb-events.$WATCH_TABLE.>" --count 2 --raw > /tmp/ldb_watch_events.txt 2>/dev/null &
WATCH_PID=$!
sleep 0.5

# Trigger changes
req ldb.put "{\"table\":\"$WATCH_TABLE\",\"key\":\"w1\",\"value\":\"$(b64 '{"v":"1"}')\"}" >/dev/null
req ldb.delete "{\"table\":\"$WATCH_TABLE\",\"key\":\"w1\"}" >/dev/null

# Wait for subscriber to collect events
sleep 1
kill $WATCH_PID 2>/dev/null || true
wait $WATCH_PID 2>/dev/null || true

# Check events were received
TOTAL=$((TOTAL + 1))
if grep -q '"op":"put"' /tmp/ldb_watch_events.txt 2>/dev/null; then
  echo "  ✓ watch: received put event"
  PASS=$((PASS + 1))
else
  echo "  ✗ watch: received put event"
  echo "    events: $(cat /tmp/ldb_watch_events.txt 2>/dev/null || echo 'none')"
  FAIL=$((FAIL + 1))
fi

TOTAL=$((TOTAL + 1))
if grep -q '"op":"delete"' /tmp/ldb_watch_events.txt 2>/dev/null; then
  echo "  ✓ watch: received delete event"
  PASS=$((PASS + 1))
else
  echo "  ✗ watch: received delete event"
  FAIL=$((FAIL + 1))
fi

rm -f /tmp/ldb_watch_events.txt

echo ""

# ── Summary ──────────────────────────────────────────────────

echo "==================================="
echo "  Results: ${PASS} passed, ${FAIL} failed, ${TOTAL} total"
echo "==================================="

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
