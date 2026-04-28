# lattice-sql-client

Typed Rust SDK for [lattice-sql](https://github.com/Taika-3D-Oy/lattice-db) — the SQL frontend for [lattice-db](https://github.com/Taika-3D-Oy/lattice-db).

Sends SQL strings to the `ldb.sql.query` NATS subject (where `ldb` is your `LDB_INSTANCE`) and maps the responses back to strongly-typed Rust values. Sits above `lattice-db-client` in the stack: you write SQL, not raw key-value operations.

## Requirements

- **Target**: `wasm32-wasip3` (Component Model async I/O)
- **Toolchain**: Rust nightly with `-Zbuild-std`
- **Runtime**: [wasmCloud](https://wasmcloud.com) ≥ 2.0, or [Wasmtime](https://wasmtime.dev) ≥ 45
- **Services**: A running `storage-service` __and__ a running `lattice-sql` instance, both connected to NATS

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
lattice-sql-client = "1.0"
nats-wasip3 = "0.7"
```

```rust
use nats_wasip3::client::{Client, ConnectConfig};
use lattice_sql_client::LatticeSql;

let client = Client::connect(ConnectConfig::default()).await?;
// Optional: auth token (required when server sets LDB_AUTH_TOKEN)
let db = LatticeSql::new(client)
    .with_auth("my-secret-token");

// DDL — define a table
db.ddl("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, age INTEGER)").await?;

// DML — write data
db.exec("INSERT INTO users (id, name, age) VALUES ('alice', 'Alice', 30)").await?;
db.exec("UPDATE users SET age = 31 WHERE id = 'alice'").await?;

// Query — read data
let result = db.query("SELECT * FROM users WHERE age >= 30 ORDER BY name ASC").await?;
println!("{:?}", result.columns);  // ["id", "name", "age"]

// Cell access by column name
let name = result.cell(0, "name").unwrap();  // "Alice"

// Aggregate
let agg = db.query("SELECT COUNT(*), SUM(age) FROM users").await?;
let count = agg.rows[0][0].as_i64().unwrap();  // 1

// Deserialize rows into a struct
#[derive(serde::Deserialize)]
struct User { id: String, name: String, age: i64 }
let users: Vec<User> = result.deserialize_rows()?;

// Cleanup
db.ddl("DROP TABLE users").await?;
```

## Supported SQL

| Statement | Example |
|---|---|
| `CREATE TABLE` | `CREATE TABLE t (id TEXT PRIMARY KEY, x INTEGER)` |
| `CREATE TABLE IF NOT EXISTS` | `CREATE TABLE IF NOT EXISTS t (...)` |
| `DROP TABLE` | `DROP TABLE t` |
| `DROP TABLE IF EXISTS` | `DROP TABLE IF EXISTS t` |
| `CREATE INDEX` | `CREATE INDEX ON t (field)` |
| `DROP INDEX` | `DROP INDEX field` |
| `INSERT INTO` | `INSERT INTO t (id, x) VALUES ('k', 42)` |
| `UPDATE` | `UPDATE t SET x = 99 WHERE id = 'k'` |
| `DELETE` | `DELETE FROM t WHERE x < 10` |
| `SELECT *` | `SELECT * FROM t` |
| `SELECT cols` | `SELECT id, name FROM t` |
| `WHERE` | `WHERE age > 25 AND city = 'Helsinki'` |
| `ORDER BY` | `ORDER BY name ASC` |
| `LIMIT / OFFSET` | `LIMIT 10 OFFSET 20` |
| Aggregates | `SELECT COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x) FROM t` |
| `GROUP BY` | `SELECT region, COUNT(*) FROM t GROUP BY region` |
| `INNER JOIN` | `SELECT a.id, b.val FROM a INNER JOIN b ON a.id = b.fk` |
| `LEFT JOIN` | `SELECT a.id, b.val FROM a LEFT JOIN b ON a.id = b.fk` |

## Column types

| SQL type | JSON representation |
|---|---|
| `TEXT` / `VARCHAR` | `string` |
| `INTEGER` / `INT` | `number` (integer) |
| `REAL` / `FLOAT` | `number` (float) |
| `BOOLEAN` | `boolean` |

## API

### `LatticeSql`

```rust
impl LatticeSql {
    fn new(client: Client) -> Self
    fn with_timeout(client: Client, timeout: Duration) -> Self
    fn with_auth(self, token: impl Into<String>) -> Self

    // Typed shortcuts
    async fn query(&self, sql: &str) -> Result<QueryResult, Error>
    async fn exec(&self, sql: &str) -> Result<u64, Error>
    async fn ddl(&self, sql: &str) -> Result<(), Error>

    // Auto-detect
    async fn sql(&self, sql: &str) -> Result<SqlResult, Error>
}
```

### `QueryResult`

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

impl QueryResult {
    fn row_count(&self) -> usize
    fn col_index(&self, name: &str) -> Option<usize>
    fn cell(&self, row: usize, col: &str) -> Option<&serde_json::Value>
    fn deserialize_rows<T: DeserializeOwned>(&self) -> Result<Vec<T>, String>
}
```

### `SqlResult` (auto-detected)

```rust
pub enum SqlResult {
    Query(QueryResult),
    Exec { affected_rows: u64 },
}
```

### `Error`

```rust
pub enum Error {
    Nats(nats_wasi::Error),  // transport error
    Db(String),              // SQL / application error from lattice-sql
    Json(String),            // serialisation error
    WrongResultType(String), // typed helper got wrong response shape
}
```

## Building

```sh
cd lattice-sql-client
cargo build --target wasm32-wasip3 -Zbuild-std=std,panic_abort
```

## Running the smoke test

```sh
# Start dependencies
nats-server -js -p 14222 &
wasmtime run --inherit-network --env NATS_URL=127.0.0.1:14222 \
  target/wasm32-wasip3/release/storage-service.wasm &
wasmtime run -S p3=y -S inherit-network=y -W component-model=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  ../target/wasm32-wasip3/release/lattice-sql.wasm &

# Build and run
cargo build --target wasm32-wasip3 --example smoke_test
wasmtime run -S p3=y -S inherit-network=y -W component-model=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  target/wasm32-wasip3/debug/examples/smoke_test.wasm
```

## Integration tests

A bash integration test for the SQL frontend is also available at the workspace level:

```sh
bash tests/integration_sql.sh
# With custom NATS:
NATS_URL=host:4222 bash tests/integration_sql.sh
# With mTLS:
bash tests/integration_sql.sh --tls
```

## Architecture

```
Your code
    │  SQL string
    ▼
LatticeSql (this crate)
    │  {"sql":"..."} over NATS → ldb.sql.query
    ▼
lattice-sql (SQL frontend component)
    │  ldb.get / ldb.scan / ldb.put / …
    ▼
storage-service (lattice-db)
    │  JetStream KV
    ▼
NATS Server
```

## License

Apache-2.0
