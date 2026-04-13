# lattice-db-client

Typed Rust SDK for [lattice-db](https://github.com/Taika-3D-Oy/lattice-db) — a NATS-native distributed database for wasmCloud.

Wraps the NATS request/reply wire protocol so you can call `put`, `get`, `scan`, `transaction`, etc. with strongly-typed Rust methods instead of hand-rolling JSON payloads.

## Requirements

- **Target**: `wasm32-wasip3` (Component Model async I/O)
- **Toolchain**: Rust nightly with `-Zbuild-std`
- **Runtime**: [wasmCloud](https://wasmcloud.com) ≥ 2.0 with wasip3 support, or [Wasmtime](https://wasmtime.dev) ≥ 43
- **NATS**: A running NATS server with JetStream enabled

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
lattice-db-client = "0.1"
nats-wasip3 = "0.3"
```

```rust
use nats_wasip3::client::{Client, ConnectConfig};
use lattice_db_client::LatticeDb;

let client = Client::connect(ConnectConfig::default()).await?;
let db = LatticeDb::new(client);

// Store and retrieve JSON
db.put_json("users", "alice", &serde_json::json!({"name": "Alice"})).await?;
let user: serde_json::Value = db.get_json("users", "alice").await?;

// Scan with filters
use lattice_db_client::ScanQuery;
let results = db.scan("users", ScanQuery::new()
    .filter("age", "gte", "25")
    .order_by("name", "asc")
    .limit(10)
).await?;

// Atomic transactions
use lattice_db_client::TxnOp;
db.transaction(vec![
    TxnOp::put("accounts", "alice", b"{\"balance\":90}"),
    TxnOp::put("accounts", "bob",   b"{\"balance\":110}"),
]).await?;
```

## Operations

| Method | Description |
|--------|-------------|
| `put` / `put_json` | Store a key-value pair |
| `get` / `get_json` | Retrieve a value by key |
| `delete` | Remove a key |
| `exists` | Check if a key exists |
| `keys` | List all keys in a table |
| `create` | Insert only if key doesn't exist |
| `cas` | Compare-and-swap (optimistic concurrency) |
| `scan` | Query with filters, sorting, pagination |
| `count` | Count rows matching optional filters |
| `aggregate` | Sum, avg, min, max, group_by |
| `batch_get` / `batch_put` | Bulk operations |
| `transaction` | Atomic multi-operation transactions |
| `set_schema` / `get_schema` / `delete_schema` | JSON schema validation |
| `create_index` / `list_indexes` / `drop_index` | Secondary indexes |

## Building

```sh
cargo build --target wasm32-wasip3 -Zbuild-std=std,panic_abort
```

## License

Apache-2.0
