#!/usr/bin/env bash
# integration_sql.sh — Integration tests for lattice-sql.
#
# Exercises the SQL frontend against a running lattice-db + lattice-sql stack.
# Sends {"sql":"..."} requests to ldb.sql.query via NATS and validates responses.
#
# Requires: nats CLI, jq
# Services : storage-service + lattice-sql must be running and subscribed
#
# Usage:
#   bash tests/integration_sql.sh                          # plain, localhost:4222
#   NATS_URL=host:port bash tests/integration_sql.sh       # custom address
#   bash tests/integration_sql.sh --tls                    # mTLS with certs from K8s secret

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

# Unique suffix per run so repeated runs don't collide in the catalog.
TS=$(date +%s)
USERS="sqlu${TS}"    # user table for this run
ORDERS="sqlo${TS}"  # orders table for this run
SALES="sqls${TS}"   # sales table for aggregation tests

PASS=0
FAIL=0
TOTAL=0

# ── Helpers ──────────────────────────────────────────────────

req() {
  local subject="$1" payload="$2"
  $NATS req "$subject" "$payload" --raw 2>/dev/null
}

# Send an SQL statement to ldb.sql.query and return the raw response.
sql() {
  local query="$1"
  local payload
  payload=$(jq -n --arg sql "$query" '{"sql": $sql}')
  req ldb.sql.query "$payload"
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

# Find column index by name from a query result JSON.
col_idx() {
  local col="$1" json="$2"
  echo "$json" | jq --arg col "$col" '.columns | index($col)'
}

# Get a cell by column name from a specific row.
cell() {
  local col="$1" row_idx="$2" json="$3"
  local idx
  idx=$(col_idx "$col" "$json")
  echo "$json" | jq -r --argjson r "$row_idx" --argjson c "$idx" '.rows[$r][$c]'
}

# ── Connectivity checks ──────────────────────────────────────

echo "=== lattice-sql integration tests ==="
echo "  NATS:   ${NATS_URL}"
echo "  Tables: ${USERS}, ${ORDERS}, ${SALES}"
echo ""

# 1. Basic NATS connectivity.
if ! $NATS pub _ping "" &>/dev/null; then
  echo "ERROR: Cannot connect to NATS at ${NATS_URL}"
  exit 1
fi

# 2. lattice-sql connectivity check — send a trivial DDL and expect any response.
PING_SQL="CREATE TABLE IF NOT EXISTS _sql_ping_${TS} (id TEXT PRIMARY KEY)"
PING_R=$(sql "$PING_SQL" 2>/dev/null || echo "")
if [[ -z "$PING_R" ]]; then
  echo "ERROR: No response from lattice-sql on ldb.sql.query"
  echo "       Make sure lattice-sql is running and subscribed to ldb.sql.>"
  exit 1
fi

echo "  lattice-sql: OK"
echo ""

# ── 1. DDL: CREATE TABLE ─────────────────────────────────────

echo "--- DDL: CREATE TABLE ---"

R=$(sql "CREATE TABLE ${USERS} (id TEXT PRIMARY KEY, name TEXT NOT NULL, age INTEGER, city TEXT)")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ CREATE TABLE ${USERS}"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ CREATE TABLE ${USERS}"
  PASS=$((PASS + 1))
fi

R=$(sql "CREATE TABLE ${ORDERS} (id TEXT PRIMARY KEY, user_id TEXT, amount INTEGER)")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ CREATE TABLE ${ORDERS}"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ CREATE TABLE ${ORDERS}"
  PASS=$((PASS + 1))
fi

R=$(sql "CREATE TABLE ${SALES} (id TEXT PRIMARY KEY, region TEXT, amount INTEGER)")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ CREATE TABLE ${SALES}"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ CREATE TABLE ${SALES}"
  PASS=$((PASS + 1))
fi

# CREATE TABLE IF NOT EXISTS — should succeed even if table already exists.
R=$(sql "CREATE TABLE IF NOT EXISTS ${USERS} (id TEXT PRIMARY KEY, name TEXT NOT NULL)")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ CREATE TABLE IF NOT EXISTS (duplicate)"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ CREATE TABLE IF NOT EXISTS (duplicate) → no error"
  PASS=$((PASS + 1))
fi

echo ""

# ── 2. DML: INSERT ───────────────────────────────────────────

echo "--- DML: INSERT ---"

R=$(sql "INSERT INTO ${USERS} (id, name, age, city) VALUES ('alice', 'Alice', 30, 'Helsinki')")
assert_json "insert alice → affected_rows=1" ".affected_rows" "1" "$R"

R=$(sql "INSERT INTO ${USERS} (id, name, age, city) VALUES ('bob', 'Bob', 25, 'Tampere')")
assert_json "insert bob → affected_rows=1" ".affected_rows" "1" "$R"

R=$(sql "INSERT INTO ${USERS} (id, name, age, city) VALUES ('carol', 'Carol', 35, 'Helsinki')")
assert_json "insert carol → affected_rows=1" ".affected_rows" "1" "$R"

# Duplicate PK should fail (executor uses ldb.create which is insert-if-not-exists).
R=$(sql "INSERT INTO ${USERS} (id, name, age, city) VALUES ('alice', 'Alice2', 99, 'Oulu')")
assert_contains "duplicate insert → error" "error" "$R"

# Insert into orders (for JOIN tests later).
sql "INSERT INTO ${ORDERS} (id, user_id, amount) VALUES ('o1', 'alice', 100)" > /dev/null
sql "INSERT INTO ${ORDERS} (id, user_id, amount) VALUES ('o2', 'alice', 250)" > /dev/null
sql "INSERT INTO ${ORDERS} (id, user_id, amount) VALUES ('o3', 'carol', 150)" > /dev/null

# Insert into sales (for aggregation tests).
sql "INSERT INTO ${SALES} (id, region, amount) VALUES ('s1', 'north', 100)" > /dev/null
sql "INSERT INTO ${SALES} (id, region, amount) VALUES ('s2', 'south', 200)" > /dev/null
sql "INSERT INTO ${SALES} (id, region, amount) VALUES ('s3', 'south', 150)" > /dev/null
sql "INSERT INTO ${SALES} (id, region, amount) VALUES ('s4', 'north', 300)" > /dev/null

echo ""

# ── 3. SELECT (basic) ────────────────────────────────────────

echo "--- SELECT (basic) ---"

R=$(sql "SELECT * FROM ${USERS}")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "SELECT * returns 3 rows" "3" "$ROW_COUNT"

COL_COUNT=$(echo "$R" | jq '.columns | length')
assert_eq "SELECT * has 4 columns" "4" "$COL_COUNT"

assert_contains "columns include 'id'"   "id"   "$(echo "$R" | jq -r '.columns[]')"
assert_contains "columns include 'name'" "name" "$(echo "$R" | jq -r '.columns[]')"
assert_contains "columns include 'age'"  "age"  "$(echo "$R" | jq -r '.columns[]')"
assert_contains "columns include 'city'" "city" "$(echo "$R" | jq -r '.columns[]')"

# Rows are sorted by key (alice < bob < carol).
FIRST_ID=$(cell "id" 0 "$R")
assert_eq "first row id is alice (sorted)" "alice" "$FIRST_ID"

echo ""

# ── 4. SELECT (specific columns) ─────────────────────────────

echo "--- SELECT (specific columns) ---"

R=$(sql "SELECT id, name FROM ${USERS}")
COL_COUNT=$(echo "$R" | jq '.columns | length')
assert_eq "SELECT id,name has 2 columns" "2" "$COL_COUNT"

ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "SELECT id,name has 3 rows" "3" "$ROW_COUNT"

# First column should be 'id' and second 'name'.
FIRST_COL=$(echo "$R" | jq -r '.columns[0]')
assert_eq "first column is 'id'" "id" "$FIRST_COL"

echo ""

# ── 5. SELECT (filtered — eq) ────────────────────────────────

echo "--- SELECT (filtered: eq) ---"

R=$(sql "SELECT * FROM ${USERS} WHERE city = 'Helsinki'")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "WHERE city='Helsinki' returns 2 rows" "2" "$ROW_COUNT"

R=$(sql "SELECT * FROM ${USERS} WHERE id = 'alice'")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "WHERE id='alice' returns 1 row" "1" "$ROW_COUNT"
NAME_VAL=$(cell "name" 0 "$R")
assert_eq "WHERE id='alice' → name is Alice" "Alice" "$NAME_VAL"

echo ""

# ── 6. SELECT (filtered — comparison) ────────────────────────

echo "--- SELECT (filtered: comparison) ---"

# age > 28 → alice (30) and carol (35)
R=$(sql "SELECT * FROM ${USERS} WHERE age > 28")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "WHERE age > 28 returns 2 rows" "2" "$ROW_COUNT"

# age < 30 → bob (25)
R=$(sql "SELECT * FROM ${USERS} WHERE age < 30")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "WHERE age < 30 returns 1 row" "1" "$ROW_COUNT"
ID_VAL=$(cell "id" 0 "$R")
assert_eq "WHERE age < 30 → bob" "bob" "$ID_VAL"

# age >= 30 → alice, carol
R=$(sql "SELECT * FROM ${USERS} WHERE age >= 30")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "WHERE age >= 30 returns 2 rows" "2" "$ROW_COUNT"

echo ""

# ── 7. SELECT (AND filter) ───────────────────────────────────

echo "--- SELECT (AND filter) ---"

# city = 'Helsinki' AND age > 30 → carol (35)
R=$(sql "SELECT * FROM ${USERS} WHERE city = 'Helsinki' AND age > 30")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "city='Helsinki' AND age>30 returns 1 row" "1" "$ROW_COUNT"
ID_VAL=$(cell "id" 0 "$R")
assert_eq "city='Helsinki' AND age>30 → carol" "carol" "$ID_VAL"

echo ""

# ── 8. SELECT (sorted + paginated) ───────────────────────────

echo "--- SELECT (sorted + paginated) ---"

R=$(sql "SELECT * FROM ${USERS} ORDER BY name ASC LIMIT 2 OFFSET 0")
PAGE1_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "ORDER BY name ASC LIMIT 2 page 1 has 2 rows" "2" "$PAGE1_COUNT"

# Alice < Bob < Carol → first should be alice, second bob.
FIRST_NAME=$(cell "name" 0 "$R")
assert_eq "sorted asc, first is Alice" "Alice" "$FIRST_NAME"
SECOND_NAME=$(cell "name" 1 "$R")
assert_eq "sorted asc, second is Bob" "Bob" "$SECOND_NAME"

R=$(sql "SELECT * FROM ${USERS} ORDER BY name ASC LIMIT 2 OFFSET 2")
PAGE2_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "page 2 has 1 row" "1" "$PAGE2_COUNT"
LAST_NAME=$(cell "name" 0 "$R")
assert_eq "page 2 is Carol" "Carol" "$LAST_NAME"

# DESC order
R=$(sql "SELECT * FROM ${USERS} ORDER BY name DESC LIMIT 1")
FIRST_DESC=$(cell "name" 0 "$R")
assert_eq "ORDER BY name DESC, first is Carol" "Carol" "$FIRST_DESC"

echo ""

# ── 9. SELECT (aggregate) ────────────────────────────────────

echo "--- SELECT (aggregate) ---"

R=$(sql "SELECT COUNT(*) FROM ${USERS}")
ROWS=$(echo "$R" | jq '.rows | length')
assert_eq "COUNT(*) returns 1 row" "1" "$ROWS"
COUNT_VAL=$(echo "$R" | jq -r '.rows[0][0]')
assert_eq "COUNT(*) = 3" "3" "$COUNT_VAL"

R=$(sql "SELECT SUM(age) FROM ${USERS}")
SUM_VAL=$(echo "$R" | jq -r '.rows[0][0]')
assert_eq "SUM(age) = 90" "90" "${SUM_VAL%.0}"

R=$(sql "SELECT AVG(age) FROM ${USERS}")
AVG_VAL=$(echo "$R" | jq -r '.rows[0][0]')
assert_eq "AVG(age) = 30" "30" "${AVG_VAL%.0}"

R=$(sql "SELECT MIN(age), MAX(age) FROM ${USERS}")
MIN_VAL=$(echo "$R" | jq -r '.rows[0][0]')
MAX_VAL=$(echo "$R" | jq -r '.rows[0][1]')
assert_eq "MIN(age) = 25" "25" "$MIN_VAL"
assert_eq "MAX(age) = 35" "35" "$MAX_VAL"

echo ""

# ── 10. SELECT (GROUP BY) ────────────────────────────────────

echo "--- SELECT (GROUP BY) ---"

# Sales: north has s1(100) + s4(300) = 2 rows, sum=400
#        south has s2(200) + s3(150) = 2 rows, sum=350
R=$(sql "SELECT region, COUNT(*) FROM ${SALES} GROUP BY region")
GROUP_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "GROUP BY region returns 2 groups" "2" "$GROUP_COUNT"

# Find north group and check count.
NORTH_ROW=$(echo "$R" | jq -r '[.rows[] | select(.[0] == "north")] | .[0]')
NORTH_COUNT=$(echo "$NORTH_ROW" | jq '.[1]')
assert_eq "north group count = 2" "2" "$NORTH_COUNT"

SOUTH_ROW=$(echo "$R" | jq -r '[.rows[] | select(.[0] == "south")] | .[0]')
SOUTH_COUNT=$(echo "$SOUTH_ROW" | jq '.[1]')
assert_eq "south group count = 2" "2" "$SOUTH_COUNT"

# Aggregated SUM per region.
R=$(sql "SELECT region, SUM(amount) FROM ${SALES} GROUP BY region")
NORTH_ROW=$(echo "$R" | jq -r '[.rows[] | select(.[0] == "north")] | .[0]')
NORTH_SUM=$(echo "$NORTH_ROW" | jq '.[1]')
assert_eq "north SUM(amount) = 400" "400" "${NORTH_SUM%.0}"

SOUTH_ROW=$(echo "$R" | jq -r '[.rows[] | select(.[0] == "south")] | .[0]')
SOUTH_SUM=$(echo "$SOUTH_ROW" | jq '.[1]')
assert_eq "south SUM(amount) = 350" "350" "${SOUTH_SUM%.0}"

echo ""

# ── 11. JOIN: INNER JOIN ─────────────────────────────────────

echo "--- INNER JOIN ---"

# alice has 2 orders (o1, o2), carol has 1 (o3), bob has none.
# INNER JOIN → only rows with matching orders (alice×2, carol×1 = 3 rows).
R=$(sql "SELECT ${USERS}.id, ${USERS}.name, ${ORDERS}.amount FROM ${USERS} INNER JOIN ${ORDERS} ON ${USERS}.id = ${ORDERS}.user_id")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "INNER JOIN returns 3 rows (alice×2, carol×1)" "3" "$ROW_COUNT"

echo ""

# ── 12. JOIN: LEFT JOIN ──────────────────────────────────────

echo "--- LEFT JOIN ---"

# LEFT JOIN → all users, null amount for bob.
R=$(sql "SELECT ${USERS}.id, ${USERS}.name, ${ORDERS}.amount FROM ${USERS} LEFT JOIN ${ORDERS} ON ${USERS}.id = ${ORDERS}.user_id")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
# alice×2 + bob×1(null) + carol×1 = 4 rows
assert_eq "LEFT JOIN returns 4 rows" "4" "$ROW_COUNT"

echo ""

# ── 13. UPDATE ───────────────────────────────────────────────

echo "--- UPDATE ---"

# Single-key UPDATE (WHERE pk = value → fast path via ldb.cas).
R=$(sql "UPDATE ${USERS} SET city = 'Espoo', age = 31 WHERE id = 'alice'")
assert_json "UPDATE alice → affected_rows=1" ".affected_rows" "1" "$R"

# Verify the update took effect.
R=$(sql "SELECT * FROM ${USERS} WHERE id = 'alice'")
CITY_VAL=$(cell "city" 0 "$R")
assert_eq "alice city updated to Espoo" "Espoo" "$CITY_VAL"
AGE_VAL=$(cell "age" 0 "$R")
assert_eq "alice age updated to 31" "31" "$AGE_VAL"

# Multi-row UPDATE (scan path).
R=$(sql "UPDATE ${USERS} SET city = 'Vantaa' WHERE city = 'Helsinki'")
ROWS_UPD=$(echo "$R" | jq -r '.affected_rows')
assert_eq "UPDATE WHERE city='Helsinki' → affected_rows > 0" "1" "$([ "$ROWS_UPD" -gt 0 ] && echo 1 || echo 0)"

echo ""

# ── 14. DELETE ───────────────────────────────────────────────

echo "--- DELETE ---"

# Single-key DELETE.
R=$(sql "DELETE FROM ${USERS} WHERE id = 'bob'")
assert_json "DELETE bob → affected_rows=1" ".affected_rows" "1" "$R"

# Verify bob is gone.
R=$(sql "SELECT * FROM ${USERS}")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "SELECT * after delete returns 2 rows" "2" "$ROW_COUNT"

# Multi-row DELETE.
R=$(sql "DELETE FROM ${SALES} WHERE region = 'south'")
ROWS_DEL=$(echo "$R" | jq -r '.affected_rows')
assert_eq "DELETE WHERE region='south' → affected_rows=2" "2" "$ROWS_DEL"

R=$(sql "SELECT * FROM ${SALES}")
ROW_COUNT=$(echo "$R" | jq '.rows | length')
assert_eq "SALES table has 2 rows (north only)" "2" "$ROW_COUNT"

echo ""

# ── 15. DDL: CREATE INDEX ─────────────────────────────────────

echo "--- DDL: CREATE INDEX ---"

R=$(sql "CREATE INDEX ON ${USERS} (name)")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ CREATE INDEX ON ${USERS} (name)"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ CREATE INDEX ON ${USERS} (name)"
  PASS=$((PASS + 1))
fi

R=$(sql "CREATE INDEX ON ${USERS} (city)")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ CREATE INDEX ON ${USERS} (city)"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ CREATE INDEX ON ${USERS} (city)"
  PASS=$((PASS + 1))
fi

# DROP INDEX.
R=$(sql "DROP INDEX name")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ DROP INDEX name"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ DROP INDEX name"
  PASS=$((PASS + 1))
fi

echo ""

# ── 16. DDL: DROP TABLE ──────────────────────────────────────

echo "--- DDL: DROP TABLE ---"

R=$(sql "DROP TABLE ${ORDERS}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ DROP TABLE ${ORDERS}"
  echo "    error: $(echo "$R" | jq -r '.error')"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ DROP TABLE ${ORDERS}"
  PASS=$((PASS + 1))
fi

R=$(sql "DROP TABLE ${SALES}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ DROP TABLE ${SALES}"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ DROP TABLE ${SALES}"
  PASS=$((PASS + 1))
fi

# DROP TABLE IF EXISTS on nonexistent table should succeed.
R=$(sql "DROP TABLE IF EXISTS _no_such_table_${TS}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ DROP TABLE IF EXISTS (nonexistent) → error"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ DROP TABLE IF EXISTS (nonexistent) → no error"
  PASS=$((PASS + 1))
fi

# DROP TABLE on nonexistent table without IF EXISTS should error.
R=$(sql "DROP TABLE _no_such_table_${TS}")
assert_contains "DROP TABLE (nonexistent, no IF EXISTS) → error" "error" "$R"

# Verify the dropped table is gone from the catalog.
R=$(sql "SELECT * FROM ${ORDERS}")
assert_contains "SELECT from dropped table → error" "error" "$R"

echo ""

# ── 17. ERROR PATHS ──────────────────────────────────────────

echo "--- ERROR PATHS ---"

# Malformed JSON to the NATS subject.
R=$(req ldb.sql.query 'not json at all')
assert_contains "malformed JSON → error" "error" "$R"

# Valid JSON but no 'sql' field.
R=$(req ldb.sql.query '{"query":"SELECT 1"}')
assert_contains "missing 'sql' field → error" "error" "$R"

# Completely invalid SQL.
R=$(sql "THIS IS NOT SQL")
assert_contains "invalid SQL → parse error" "error" "$R"

# SELECT from table not in catalog.
R=$(sql "SELECT * FROM _nonexistent_table_${TS}")
assert_contains "SELECT nonexistent table → error" "error" "$R"

# INSERT without providing the primary key value.
R=$(sql "INSERT INTO ${USERS} (name, age) VALUES ('Dave', 40)")
assert_contains "INSERT without PK → error" "error" "$R"

# Empty SQL string.
R=$(sql "")
assert_contains "empty SQL → error" "error" "$R"

echo ""

# ── 18. CLEANUP: DROP remaining tables ───────────────────────

echo "--- CLEANUP ---"

R=$(sql "DROP TABLE ${USERS}")
TOTAL=$((TOTAL + 1))
if echo "$R" | jq -e '.error' &>/dev/null; then
  echo "  ✗ DROP TABLE ${USERS}"
  FAIL=$((FAIL + 1))
else
  echo "  ✓ DROP TABLE ${USERS}"
  PASS=$((PASS + 1))
fi

sql "DROP TABLE IF EXISTS _sql_ping_${TS}" > /dev/null

echo ""

# ── Summary ──────────────────────────────────────────────────

echo "==================================="
echo "  Results: ${PASS} passed, ${FAIL} failed, ${TOTAL} total"
echo "==================================="

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
