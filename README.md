# lattice-db

A distributed database that lives entirely inside NATS.

The engine is a 663 KB `wasm32-wasip3` component. It connects to NATS, subscribes to `ldb.>` in a queue group, persists every table to its own JetStream KV bucket, and serves all CRUD, query, index, and transaction traffic over NATS request/reply.

```
clients ──NATS req/rep──▶ storage-service (Wasm component, 663 KB)
                                │
co-located ──TCP :4080──▶       │
  components                    │
                          NATS JetStream KV
                          (one bucket per table: ldb-{table})
```

- **JetStream KV as the storage layer.** Tables are buckets. No other persistent state.
- **Cross-replica cache coherence via KV watchers.** Every replica keeps a local in-memory cache and invalidates entries by subscribing to the bucket's change stream.
- **Multi-key transactions on top of a JetStream WAL stream.** A dedicated `ldb-txn` stream records PREPARE → apply → COMMIT/ABORT. Crash recovery walks the WAL on startup with a 30-second grace window and a per-txn lock bucket so multiple replicas can recover safely without stepping on each other.
- **Horizontal scaling via NATS queue groups.** All replicas join `ldb-workers`; NATS distributes requests across them. Add a replica → reads scale linearly.
- **Stateless components.** A replica can crash, restart, scale up, or scale down without any data migration. The state is in JetStream.

## Performance

Measured on a single Apple M-series machine, release build, loopback NATS 2.12.6, no Raft replication, 1 replica:

| Scenario | Throughput | p50 | p99 |
|---|---|---|---|
| Reads (cache hit, 64 concurrent) | **134,100 req/s** | 0.44 ms | 1.19 ms |
| Writes, 1 table (64 concurrent) | **9,684 req/s** | 5.50 ms | 23.74 ms |
| Writes, 8 tables (64 concurrent) | **18,580 req/s** | 2.68 ms | 12.14 ms |
| Transactions, 2-op (8 concurrent) | **499 txn/s** | 1.19 ms | 3.10 ms |

- Reads are served from each replica's in-memory cache; the cost is one NATS round-trip.
- Multi-table writes scale because each bucket has an independent JetStream stream leader.
- Transactions are capped by the single global WAL stream — adding replicas does not help, since they all serialize through it.

See [Running the benchmark](#running-the-benchmark) below.

## Operations

All requests are NATS request/reply on `ldb.{op}`. Bodies are JSON; binary values are base64-encoded.

| Subject | Body |
|---|---|
| `ldb.get` / `ldb.exists` / `ldb.keys` | `{table, key, consistency?}` / `{table, cursor?, consistency?}` |
| `ldb.put` / `ldb.create` | `{table, key, value, ttl_seconds?}` |
| `ldb.cas` | `{table, key, value, revision, ttl_seconds?}` |
| `ldb.delete` | `{table, key}` |
| `ldb.cas_delete` | `{table, key, revision}` |
| `ldb.purge` | `{table, key, revision?, ttl_seconds?}` |
| `ldb.get_revision` | `{table, key, revision, consistency?}` — includes delete/purge tombstones |
| `ldb.batch.get` / `ldb.batch.put` | `{table, keys: [...], consistency?}` / `{table, entries: [...]}` |
| `ldb.scan` / `ldb.count` | `{table, filters, order_by?, limit?, offset?, key_prefix?, consistency?}` |
| `ldb.aggregate` | `{table, filters, group_by?, ops: [{fn, field?}], consistency?}` (`count`/`sum`/`avg`/`min`/`max`) |
| `ldb.index.create` / `ldb.index.drop` / `ldb.index.list` | `{table, field}` or `{table, fields: [...]}` (compound) |
| `ldb.txn` | `{ops: [{op, table, key, value?}]}` — atomic, max 64 ops |
| `ldb.schema.set` / `ldb.schema.get` / `ldb.schema.delete` | `{table, schema}` |

Filters use `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `prefix`. Schemas validate `put` / `create` / `cas` / `batch.put`.

Every mutation also publishes a change event on `ldb-events.{table}.{key}`:

```bash
nats sub "ldb-events.users.>"
```

Session-consistent clients can carry a per-table watermark via:

```json
"consistency": { "min_revision": 123 }
```

Write responses may include:

```json
"session": { "revisions": { "users": 123 } }
```

This enables read-your-write behavior even when traffic hops across app and storage replicas.

When consistency is enabled, lattice-db derives catch-up freshness from KV stream sequence
watermarks, not just the highest surviving row revision. This avoids false stale errors when
the bucket has many delete/purge events (for example, high-churn expiring session keys).

## Instance isolation

> **Note:** Throughout this README, `ldb` is the **default instance name** — the prefix used in all subject names, KV bucket names, and the WAL stream. It is not hardcoded; every occurrence of `ldb` in the examples above (`ldb.get`, `ldb-users`, `ldb-txn`, …) becomes your chosen name when you set `LDB_INSTANCE`.

Deploy one `storage-service` per application. Set `LDB_INSTANCE` in each deployment's environment; all NATS subjects, KV buckets, and WAL resources are automatically namespaced.

| Env var | Effect |
|---|---|
| `LDB_INSTANCE=instancename` | NATS subject prefix for messaging (e.g. `instancename.get`, `instancename-events.>`) |
| `LDB_DATA_INSTANCE=instancename` | NATS KV bucket and WAL prefix (e.g. `instancename-users`, `instancename-txn`). Defaults to `LDB_INSTANCE`. |
| `LDB_AUTH_TOKEN=...` | Every request must include `"_auth": "<token>"` |
| `NATS_URL=...` | NATS address for messaging (req/rep subscriptions and events). **Optional** — if omitted, NATS request/reply is disabled. |
| `NATS_DATA_URL=...` | NATS address for storage (JetStream WAL and KV buckets). Falls back to `NATS_URL` if not set. **At least one of `NATS_URL` or `NATS_DATA_URL` must be set.** |
| `LDB_TCP_PORT=4080` | Enable localhost TCP listener on the given port. **Optional** — if omitted, TCP is disabled. |
| `LDB_CONSISTENCY_WATCHER_WAIT_STEPS` | Number of short watcher-poll attempts before forced table reload on consistency-gated reads. Default `2` (range `0..60`). |
| `LDB_CONSISTENCY_WATCHER_WAIT_STEP_SECS` | Seconds per watcher-poll attempt. Default `1` (range `0..30`). |

**Transport modes:** At least one of `NATS_URL` or `LDB_TCP_PORT` must be set. Both can be enabled simultaneously for hybrid deployments where some clients use NATS and co-located components use TCP.

`LDB_INSTANCE` defaults to `ldb`. Allowed characters: alphanumeric, `_`, `-`; max 64 chars.

This is **NATS-level isolation** — an application using instance `asd` cannot accidentally read or write `wasd` data because the subjects are different. For stricter security (separate credentials), give each deployment its own NATS account or NKey.

### Rust client

```rust
let db = LatticeDb::new(client)
    .with_instance("instancename")   // must match LDB_INSTANCE on the server
    .with_auth("secret");      // must match LDB_AUTH_TOKEN
```

## Build & run

Requires Rust nightly (the `rust-toolchain.toml` pins it). Output is `target/wasm32-wasip3/release/storage_service.wasm` (663 KB).

```bash
cargo build --release

# Run with wasmtime against a local NATS server
nats-server -js -p 14222 &
wasmtime run -S p3=y -S inherit-network=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  target/wasm32-wasip3/release/storage_service.wasm
```

For a full Kind + wasmCloud + mTLS local environment:

```bash
bash deploy/deploy-local.sh           # full setup
bash deploy/deploy-local.sh rebuild   # rebuild service only
bash deploy/deploy-local.sh teardown
```

Prerequisites: `kind`, `kubectl`, `helm`, `docker`, `cargo`, `wash`. The wasmCloud host must have wasip3 enabled (`wash host --wasip3`).

For a public-registry deployment, push `storage_service.wasm` to OCI and apply [`deploy/workloaddeployment-public.yaml`](deploy/workloaddeployment-public.yaml).

## Co-located TCP service (wasmCloud)

When running on wasmCloud v2, you can deploy `storage-service` as a **co-located service** inside a `WorkloadDeployment`. Components in the same workload connect over localhost TCP (`127.0.0.1:4080`) instead of going through NATS request/reply — eliminating a network hop and giving sub-millisecond latency for reads.

```
┌─ WorkloadDeployment ──────────────────────────────┐
│                                                    │
│  component-a ──TCP──▶ storage-service ──▶ NATS KV │
│  component-b ──TCP──┘       :4080                  │
│                                                    │
└────────────────────────────────────────────────────┘
```

### Wire protocol

The TCP protocol is identical to the NATS JSON bodies, wrapped in a length-prefixed frame:

```
┌──────────────┬──────────────────────────────────┐
│ 4 bytes (BE) │          JSON payload             │
│  body length │  (same as NATS, with _op field)   │
└──────────────┴──────────────────────────────────┘
```

The JSON body must include an `_op` field corresponding to the NATS subject suffix:

```json
{"_op": "get", "table": "users", "key": "user-123"}
{"_op": "put", "table": "users", "key": "user-123", "value": {"name": "Alice"}}
{"_op": "scan", "table": "sessions", "filters": {"user_id": {"eq": "user-123"}}}
```

### Configuration

| Env var | Default | Description |
|---|---|---|
| `LDB_TCP_PORT` | `4080` | Port to listen on (localhost only) |

The TCP listener runs alongside the NATS subscription loop. All existing NATS-based operations (`get`, `put`, `cas`, `scan`, `txn`, etc.) work over TCP with the same JSON format — just add `"_op": "..."` to specify the operation.

### Deployment example (WorkloadDeployment)

```yaml
apiVersion: runtime.wasmcloud.dev/v1alpha1
kind: WorkloadDeployment
metadata:
  name: my-app
spec:
  lattice: default
  service:
    name: storage-service
    image: ghcr.io/your-org/lattice-db/storage-service:latest
    imagePullPolicy: Always
    config:
      - name: storage-service-config  # ConfigMap with NATS_URL, LDB_INSTANCE, etc.
  components:
    - name: my-component
      image: ghcr.io/your-org/my-component:latest
      replicas: 1
```

The service runs as a sidecar — one long-lived process that all component instances in the workload connect to.

### Client example (Rust, wasm32-wasip3)

```rust
use wasi::sockets::types::{IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, TcpSocket};

async fn ldb_request(op: &str, payload: &serde_json::Value) -> Result<serde_json::Value, String> {
    let mut payload = payload.clone();
    payload.as_object_mut().unwrap()
        .insert("_op".into(), serde_json::Value::String(op.into()));
    let body = serde_json::to_vec(&payload).unwrap();

    // Connect to co-located storage-service
    let socket = TcpSocket::create(IpAddressFamily::Ipv4).unwrap();
    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress { port: 4080, address: (127, 0, 0, 1) });
    socket.connect(addr).await.unwrap();

    let (mut rx, _rx_done) = socket.receive();
    let (mut tx, tx_rx) = wit_stream::new::<u8>();
    let _send = socket.send(tx_rx);

    // Write: [4-byte BE length][JSON body]
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    tx.write_all(frame).await;

    // Read response: [4-byte BE length][JSON body]
    let mut buf = Vec::new();
    while buf.len() < 4 {
        let (status, data) = rx.read(Vec::with_capacity(4096)).await;
        match status {
            StreamResult::Complete(0) => return Err("eof".into()),
            StreamResult::Complete(n) => buf.extend_from_slice(&data[..n]),
            _ => return Err("read error".into()),
        }
    }
    let resp_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.drain(..4);
    while buf.len() < resp_len {
        let (status, data) = rx.read(Vec::with_capacity(4096)).await;
        match status {
            StreamResult::Complete(0) => return Err("eof".into()),
            StreamResult::Complete(n) => buf.extend_from_slice(&data[..n]),
            _ => return Err("read error".into()),
        }
    }

    serde_json::from_slice(&buf[..resp_len]).map_err(|e| e.to_string())
}
```

## Test

```bash
bash tests/integration.sh             # 190 tests, plain local NATS
bash tests/integration.sh --tls       # against the Kind cluster with mTLS
```

Requires `nats` CLI, `jq`, `base64`. See [TESTING.md](TESTING.md) for Kubernetes setup.

## Running the benchmark

```bash
cargo build --target wasm32-wasip3 --release --example bench -p lattice-db-client

nats-server -js -p 14222 &
wasmtime run -S p3=y -S inherit-network=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  target/wasm32-wasip3/release/storage_service.wasm &

wasmtime run -S p3=y -S inherit-network=y -W component-model-async=y \
  --env NATS_URL=127.0.0.1:14222 \
  --env BENCH_DURATION_SECS=10 \
  --env BENCH_CONCURRENCY=64 \
  --env BENCH_TXN_CONCURRENCY=8 \
  target/wasm32-wasip3/release/examples/bench.wasm
```

Tunables: `BENCH_DURATION_SECS` (10), `BENCH_CONCURRENCY` (64), `BENCH_TXN_CONCURRENCY` (8), `BENCH_MSG_SIZE` (256), `BENCH_TABLES` (8), `BENCH_TXN_OPS` (2).

## Project layout

```
storage-service/    # the database (wasm component)
  src/main.rs       #   NATS connection, queue subscription, watchers, WAL recovery
  src/tcp_server.rs #   localhost TCP listener for co-located component access
  src/handler.rs    #   request dispatch for all {instance}.* operations
  src/state.rs      #   in-memory cache, indexes, query engine, aggregation
  src/store.rs      #   NATS KV persistence
  src/txn.rs        #   WAL-backed transactions
lattice-db-client/  # typed Rust SDK (published on crates.io)
  examples/bench.rs #   the benchmark used above
deploy/             # Kind + wasmCloud local environment
tests/              # integration test suites
```

## Crates

| Crate | Description |
|---|---|
| `storage-service` | The database service |
| [`lattice-db-client`](lattice-db-client/) | Typed Rust SDK |
| [`nats-wasip3`](https://crates.io/crates/nats-wasip3) | NATS client for `wasm32-wasip3` (published separately) |

## License

Apache-2.0
