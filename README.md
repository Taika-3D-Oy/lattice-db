# lattice-db

A NATS-native distributed database built as a WebAssembly component for [wasmCloud](https://wasmcloud.com). It compiles to `wasm32-wasip3`, connects directly to NATS JetStream for persistence, and serves all operations over the NATS request/reply protocol.

## Architecture

```
clients ──NATS req/rep──▶ storage-service (Wasm component)
                                │
                          NATS JetStream KV
                          (one bucket per table)
```

**Crates:**

| Crate | Description |
|---|---|
| `storage-service` | The database service — in-memory cache, secondary indexes, persistence to NATS KV |
| [`lattice-db-client`](lattice-db-client/) | Typed Rust SDK — wraps the wire protocol with ergonomic async methods |
| [`nats-wasip3`](https://crates.io/crates/nats-wasip3) | NATS client for `wasm32-wasip3` — core protocol, JetStream, KV, TLS (published separately) |

All state lives in NATS JetStream KV buckets (one per table, named `ldb-{table}`). The service maintains an in-memory cache for fast reads and writes through to NATS KV for durability.

## Operations

All operations use NATS request/reply on `ldb.{op}` subjects. Request and response bodies are JSON. Values are base64-encoded.

### CRUD

| Subject | Request | Response |
|---|---|---|
| `ldb.get` | `{table, key}` | `{key, value, revision}` |
| `ldb.put` | `{table, key, value, ttl_seconds?}` | `{revision}` |
| `ldb.delete` | `{table, key}` | `{}` |
| `ldb.create` | `{table, key, value, ttl_seconds?}` | `{revision}` |
| `ldb.cas` | `{table, key, value, revision, ttl_seconds?}` | `{revision}` |
| `ldb.exists` | `{table, key}` | `{exists}` |
| `ldb.keys` | `{table, cursor?}` | `{keys, cursor?}` |

### Batch

| Subject | Request | Response |
|---|---|---|
| `ldb.batch.get` | `{table, keys: [...]}` | `{results: [{key, value?, revision?, error?}]}` |
| `ldb.batch.put` | `{table, entries: [{key, value, ttl_seconds?}]}` | `{results: [{key, revision}]}` |

### Query

| Subject | Request | Response |
|---|---|---|
| `ldb.scan` | `{table, filters, order_by?, limit?, offset?, key_prefix?}` | `{rows, total_count}` |
| `ldb.count` | `{table, filters}` | `{count}` |

**Filters** support `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `prefix` operators:

```json
{"field": "city", "op": "eq", "value": "Helsinki"}
```

### Indexes

| Subject | Request | Response |
|---|---|---|
| `ldb.index.create` | `{table, field}` or `{table, fields: ["a","b"]}` | `{}` |
| `ldb.index.drop` | `{table, field}` | `{}` |
| `ldb.index.list` | `{table}` | `{indexes}` |

Single-field and compound (multi-field) indexes are supported. Compound indexes are named `field1+field2`.

### Aggregation

| Subject | Request | Response |
|---|---|---|
| `ldb.aggregate` | `{table, filters, group_by?, ops: [{fn, field?}]}` | `{groups: [{key?, results}]}` |

Supported functions: `count`, `sum`, `avg`, `min`, `max`.

```json
{
  "table": "sales",
  "filters": [],
  "group_by": "region",
  "ops": [{"fn": "count"}, {"fn": "sum", "field": "amount"}]
}
```

### Transactions

| Subject | Request | Response |
|---|---|---|
| `ldb.txn` | `{ops: [{op, table, key, value?}]}` | `{ok, results}` |

Multi-key atomic writes backed by a write-ahead log (WAL) in JetStream. Supports `put`, `create`, `delete` operations. Max 64 ops per transaction. Automatic rollback on failure, crash recovery on startup.

### Schema Validation

| Subject | Request | Response |
|---|---|---|
| `ldb.schema.set` | `{table, schema}` | `{}` |
| `ldb.schema.get` | `{table}` | `{schema}` |
| `ldb.schema.delete` | `{table}` | `{}` |

Schema format:

```json
{
  "fields": {
    "name": {"type": "string", "required": true},
    "age": {"type": "number"},
    "active": {"type": "boolean"}
  }
}
```

When a schema is set, all `put`, `create`, `cas`, and `batch.put` operations validate against it.

### TTL / Expiry

Add `ttl_seconds` to any write operation (`put`, `create`, `cas`, `batch.put`):

```json
{"table": "sessions", "key": "abc", "value": "...", "ttl_seconds": 3600}
```

Expired keys are invisible to `get`, `exists`, `keys`, `scan`, and `count`.

### Watch / Change Events

Every mutation publishes a change event on `ldb-events.{table}.{key}`:

```json
{"op": "put", "table": "users", "key": "alice", "value": "...", "revision": 42}
```

Subscribe with the NATS CLI:

```bash
nats sub "ldb-events.users.>"      # all changes to the users table
nats sub "ldb-events.>"            # all changes across all tables
```

## Auth & Multi-Tenancy

Both are opt-in via environment variables. **Important:** This implementation provides organization/partitioning only, not cryptographic isolation.

**Auth token** — set `LDB_AUTH_TOKEN` and every request must include `"_auth": "<token>"` in the JSON body:

```bash
LDB_AUTH_TOKEN=my-secret
```

⚠️ **Security notes:**
- The token is a single shared secret across all clients.
- Tokens are sent in plaintext in the JSON message body (not in NATS headers).
- No per-client identity, rotation, or replay protection.
- Suitable only for internal networks with trusted clients.
- For production untrusted networks, use NATS NKey/JWT authentication instead.

**Partitioned mode** — set `LDB_PARTITIONED=1` and every request must include `"_partition": "<id>"`. The partition ID is transparently prefixed to table names:

```bash
LDB_PARTITIONED=1
```

⚠️ **Partition model:**
- Partitions are a **logical key-namespace convenience**, not a security boundary.
- Any client with the shared auth token can access any partition by changing the `_partition` field.
- For cryptographic isolation, use NATS account/permission-based separation, or run a separate lattice-db instance per security domain.
- Suitable for: operator-owned services where all clients are internal/trusted and partitioning is just a key namespace.
- Not suitable for: customer-facing SaaS without additional auth/RBAC layers.

## Build

Requires Rust nightly (for `wasm32-wasip3` build-std). The included `rust-toolchain.toml` and `.cargo/config.toml` handle all configuration:

```bash
cargo build --release
```

The output binary is at `target/wasm32-wasip3/release/storage_service.wasm`.

## Run Locally

### With wasmtime

```bash
wasmtime run -S inherit-network -S p3=y \
  --env NATS_URL=127.0.0.1:4222 \
  target/wasm32-wasip3/release/storage_service.wasm
```

Requires a running NATS server with JetStream enabled:

```bash
nats-server -js
```

### On Kubernetes (Kind)

The included deploy script sets up a complete local environment — Kind cluster, NATS with JetStream and mTLS, wasmCloud host, and the storage service:

```bash
# Full setup from scratch
bash deploy/deploy-local.sh

# Rebuild and redeploy the service only
bash deploy/deploy-local.sh rebuild

# Tear down everything
bash deploy/deploy-local.sh teardown
```

Prerequisites: `kind`, `kubectl`, `helm`, `docker`, `cargo`, `wash`

### Public Registry Deployment

If you want to include `lattice-db` directly in another wasmCloud setup, publish
`storage_service.wasm` to an OCI registry and use
`deploy/workloaddeployment-public.yaml` as the drop-in manifest.

```bash
kubectl apply -f deploy/workloaddeployment-public.yaml
```

Important: `lattice-db` is built for `wasm32-wasip3`. The target wasmCloud host
must have wasip3 enabled (for example, `wash host --wasip3`).

## Test

190 integration tests covering all operations:

```bash
# Against a plain local NATS server
bash tests/integration.sh

# Against the Kind cluster with mTLS
bash tests/integration.sh --tls
```

### Partition Tests

96 tests verifying partition prefix isolation across all operations. Requires the service to be running with `LDB_PARTITIONED=1`:

```bash
bash tests/integration_partitioned.sh
```

For instructions on configuring a Kubernetes testing cluster to support the latest `wasm32-wasip3` dependencies natively, please see the [Testing on Kubernetes Setup Guide](TESTING.md).

Requires: `nats` CLI, `jq`, `base64`

## Project Structure

```
lattice-db/
├── Cargo.toml              # workspace root
├── storage-service/        # the database service (wasm component)
│   └── src/
│       ├── main.rs         # entry point, NATS connection, WAL recovery
│       ├── handler.rs      # request dispatch for all operations
│       ├── state.rs        # in-memory cache, indexes, query engine, aggregation
│       ├── store.rs        # NATS KV persistence layer
│       └── txn.rs          # WAL-backed transactions
├── lattice-db-client/      # typed Rust SDK (published on crates.io)
│   └── src/
│       └── lib.rs          # LatticeDb struct with all typed methods
├── deploy/
│   ├── deploy-local.sh               # Kind cluster setup and deployment
│   └── workloaddeployment-public.yaml # public OCI deployment example
└── tests/
    ├── integration.sh      # 94 integration tests
    └── integration_partitioned.sh  # 96 partition prefix tests
```

## Roadmap

### Planned for v1.1.0
- Per-account NATS auth isolation
- Query performance metrics (latency histograms)
- Automatic index recommendation engine

### Planned for v2.0.0
- Distributed query execution
- Transparent sharding
- Time-series optimizations

## License

Apache-2.0
