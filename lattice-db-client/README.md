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
lattice-db-client = "1.5"
nats-wasip3 = "0.8"
```

```rust
use nats_wasip3::client::{Client, ConnectConfig};
use lattice_db_client::LatticeDb;

let client = Client::connect(ConnectConfig::default()).await?;
// Optional: auth token (required when server sets LDB_AUTH_TOKEN)
//           instance  (must match LDB_INSTANCE on the server, default "ldb")
let db = LatticeDb::new(client)
    .with_auth("my-secret-token")
    .with_instance("acme");

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
| `put_with_ttl` | Store with an expiry (seconds) |
| `get` / `get_json` | Retrieve a value by key |
| `delete` | Remove a key |
| `exists` | Check if a key exists |
| `keys` | List all keys in a table |
| `create` / `create_json` | Insert only if key doesn't exist |
| `create_with_ttl` | Insert with an expiry (fails if key exists) |
| `cas` | Compare-and-swap (optimistic concurrency) |
| `cas_with_ttl` | Compare-and-swap with an expiry |
| `cas_delete` | CAS delete — only if revision matches |
| `purge` | Remove all revisions of a key |
| `purge_with_ttl` | Purge with an expiring tombstone |
| `purge_expect_revision` | CAS purge — only if revision matches |
| `get_revision` | Fetch entry at a specific revision (incl. tombstones) |
| `scan` | Query with filters, sorting, pagination |
| `count` | Count rows matching optional filters |
| `aggregate` | Sum, avg, min, max, group_by |
| `batch_get` / `batch_put` | Bulk operations |
| `transaction` | Atomic multi-operation transactions |
| `set_schema` / `get_schema` / `delete_schema` | JSON schema validation |
| `create_index` / `list_indexes` / `drop_index` | Secondary indexes |

## Authentication & instance

```rust
// Authenticate against a server with LDB_AUTH_TOKEN set:
let db = LatticeDb::new(client).with_auth("my-secret-token");

// Connect to a non-default instance (must match LDB_INSTANCE on the server):
let db = LatticeDb::new(client).with_instance("acme");

// Both together:
let db = LatticeDb::new(client)
    .with_auth("my-secret-token")
    .with_instance("acme");
```

The instance name (default `"ldb"`) must match the server's `LDB_INSTANCE` configuration. This drives all NATS subject prefixes for messaging and change events. If the server is configured with a separate `LDB_DATA_INSTANCE` for storage, the client still only needs to know the messaging `LDB_INSTANCE`. Each lattice-db deployment on the same cluster remains isolated by these prefixes.

## Building

```sh
cargo build --target wasm32-wasip3 -Zbuild-std=std,panic_abort
```

## License

Apache-2.0
