# Changelog

## [1.4.0] - 2026-04-28

### Added

- **Multi-cluster NATS support**: Support for separate NATS clusters for messaging and data storage via `NATS_DATA_URL`.
  - `NATS_URL` is now used for request/reply subscriptions and change events.
  - `NATS_DATA_URL` (new) is used for JetStream WAL and KV storage.
- **Multi-instance isolation**: Support for separate instance names for messaging and data storage via `LDB_DATA_INSTANCE`.
  - `LDB_INSTANCE` is used for NATS subject prefixes and event publishing.
  - `LDB_DATA_INSTANCE` (new) is used for NATS KV bucket names and JetStream WAL.
  - Both default to the messaging counterparts if not provided, maintaining backward compatibility.

### Changed

- **`storage-service/src/main.rs`**: Updated to establish separate NATS connections and use separate instance prefixes when configured.
- **`deploy/`**: Updated deployment examples to include `NATS_DATA_URL` and `LDB_DATA_INSTANCE`.
- **Version Bumps**: Bumped all workspace crates (`lattice-db-client`, `lattice-sql`, `lattice-sql-client`) to `1.4.0` for project-wide consistency.

## [1.2.1] - 2026-04-23

### Fixes & cleanup

- **`lattice-sql-client`**: `LatticeSql` struct was missing the `instance` field and the old `SUBJECT` const was still present, causing a compile error. Both fixed.
- **`storage-service/src/tests.rs`**: Removed two obsolete partition tests (`test_partition_prefix_distinct`, `test_unpartitioned_vs_partitioned_config`) left over from the removed `_partition` scheme.
- **`lattice-sql/src/planner.rs`**: Clarified a TODO comment from "filter partitioning" to "filter scoping for JOIN queries".
- **`lattice-db-client/README.md`**: Updated quick-start snippet and auth section to use `with_instance` / `LDB_INSTANCE`; removed all `with_partition` / `LDB_PARTITIONED` references.

## [1.2.0] - 2026-04-23

### Breaking change — `LDB_PARTITIONED` and `_partition` removed

The old per-request `_partition` field and `LDB_PARTITIONED=1` server flag have
been replaced by a single **instance prefix** configured once at deploy time.

**Before:**
- One shared `storage-service` deployment, `LDB_PARTITIONED=1`
- Every request payload carried `"_partition": "<id>"` to namespace its tables
- KV buckets, WAL stream, and NATS subjects all shared the hardcoded `ldb-` prefix

**After:**
- One `storage-service` deployment per logical application, each with its own `LDB_INSTANCE=<id>`
- No per-request field — callers send plain `{"table": "users", ...}` as always
- Every NATS resource (subjects, KV buckets, WAL stream, queue group) is automatically scoped to the instance

This trades a per-request runtime convention for a per-deployment configuration, giving
full NATS-level isolation between apps sharing the same cluster. Each instance also gets
its own 4 GiB Wasm memory budget, which was previously shared across all tenants.

#### Migration

| Before | After |
|---|---|
| `LDB_PARTITIONED=1` + `_partition: "orders"` on every call | `LDB_INSTANCE=orders` in the deployment env |
| `nats sub "ldb-events.orders.users.>"` | `nats sub "orders-events.users.>"` |
| `ldb-orders-users` KV bucket (with partition prefix in name) | `orders-users` KV bucket |

`LDB_INSTANCE` defaults to `ldb` so **existing single-deployment setups work
unchanged** without any env-var change.

#### What changed

- **`storage-service/src/main.rs`** — reads `LDB_INSTANCE` (validates: alphanumeric, `_`, `-`, max 64 chars); subscribes to `{instance}.>` with queue group `{instance}-workers`
- **`storage-service/src/store.rs`** — `Store` carries the instance prefix; KV buckets named `{instance}-{table}`
- **`storage-service/src/txn.rs`** — WAL stream `{instance}-txn`, WAL subject `_{instance}.txn.wal`, recovery-lock bucket `{instance}-_recovery-locks`; `ensure_wal_stream`, `recover`, `execute` all accept `instance: &str`
- **`storage-service/src/handler.rs`** — `Config.partitioned: bool` → `Config.instance: String`; `apply_partition_prefix` removed; `publish_change` now emits `{instance}-events.{table}.{key}`
- **`lattice-db-client/src/lib.rs`** — `with_partition()` removed; `with_instance()` added (default `"ldb"`); all NATS subjects derived from instance via `subj()` helper; `_partition` field no longer injected
- **`lattice-sql/src/main.rs`** — reads `LDB_INSTANCE`; subscribes to `{instance}.sql.>`; passes instance to `LatticeDb` client
- **`lattice-sql-client/src/lib.rs`** — `SUBJECT` const removed; `with_instance()` added; `subject()` method returns `{instance}.sql.query`
- **`tests/integration_partitioned.sh`** — deleted (tested the removed `_partition` scheme)

## [1.1.0] - 2026-04-22

Focus: distributed correctness when scaling the storage-service workload up
and down. None of the wire-protocol APIs changed.

### Distributed Correctness Fixes

#### Indexes survive reboots and propagate to new replicas
- **Issue**: Index definitions were persisted to the `_indexes` KV bucket but
  never reloaded on startup, so indexes were silently lost on restart and
  invisible to scaled-up replicas. Queries silently degraded to full scans.
- **Fix**: `main.rs` now loads `_indexes` at startup and rebuilds in-memory
  index structures via `create_index` / `create_compound_index`. A KV watcher
  keeps every replica in sync when an index is created or dropped on any peer.
- **Files**: `storage-service/src/main.rs`

#### TTL is uniformly visible across all replicas
- **Issue**: Originating replica hid expired keys via a local monotonic clock,
  but peers still served them until the NATS-side `Operation::Delete` arrived.
  This created a window of stale-but-visible reads on peers.
- **Fix**: Removed the local `expires_at_ms` / `clock_ms` / `reap_expired`
  machinery entirely. Per-message TTL is enforced solely by the NATS server
  (`Nats-TTL` header, `allow_msg_ttl: true`); KV watchers receive
  `Operation::Delete` simultaneously on every replica, so expiry is uniform
  by construction.
- **Files**: `storage-service/src/state.rs`, `storage-service/src/handler.rs`,
  `storage-service/src/main.rs`

#### CAS retries see the winning revision immediately
- **Issue**: On `cas` revision-mismatch, the client's next `get` could return
  a stale cached row until the KV watcher delivered the conflicting write.
- **Fix**: On any CAS failure, `handle_cas` proactively does `kv.get(&key)`
  and refreshes the local cache so the client's next read returns the
  authoritative current value/revision.
- **Files**: `storage-service/src/handler.rs`

#### Watchers survive NATS hiccups
- **Issue**: Per-table KV watchers exited on disconnect with `watching = false`
  but `loaded = true`. `ensure_loaded` therefore never re-spawned them, so the
  cache silently diverged from peers until the replica restarted. The schema
  and index watchers in `main.rs` similarly exited and never reconnected.
- **Fix**:
  - Per-table watcher now sets both `loaded = false` and `watching = false`
    on disconnect so the next request triggers a full reload + fresh watcher.
  - Schema and index watchers in `main.rs` wrapped in self-respawning loops
    with 5s backoff that track the last-seen revision across reconnects.
- **Files**: `storage-service/src/handler.rs`, `storage-service/src/main.rs`

#### WAL recovery is cross-replica safe
- **Issue**: A newly started replica could roll back PREPARE records that
  belonged to a *live* peer's in-flight transaction, corrupting committed
  state. The ad-hoc `purge_stream` could also race against in-flight COMMITs.
- **Fix**:
  - WAL records carry a per-process random `node_id`
    (`wasi:random/get_random_u64`).
  - Recovery skips PREPAREs younger than 30 s on **server-side timestamp**
    (new in `nats-wasip3` 0.8.0's `StreamMessage.time`), unless the record
    was authored by this very process.
  - Per-txn recovery lock via `kv.create` on a new `ldb-_recovery-locks`
    bucket (TTL 300 s); only the winning replica runs rollback, crashed
    claimers free up after the lock TTL.
  - Removed the ad-hoc `purge_stream` — relies on the existing
    `max_msgs: 10_000` stream bound.
- **Files**: `storage-service/src/txn.rs`, `storage-service/src/main.rs`

#### Tables loaded by recovery now also get watchers
- **Issue**: `txn::recover` pre-loaded tables before the dispatcher ran,
  marking `loaded = true`. `handler::ensure_loaded` keyed watcher spawn off
  `needs_load`, so those tables never got a KV watcher and missed all peer
  writes.
- **Fix**: `ensure_loaded` now tracks `loaded` and `watching` independently
  and spawns a watcher whenever one is missing, even if data is already
  cached.
- **Files**: `storage-service/src/handler.rs`

### Dependencies

- Bumped `nats-wasip3` 0.7 → 0.8 across all four workspace crates for the
  new `StreamMessage.time` field used by WAL recovery grace logic.

## [1.0.0] - 2024

### ✨ Major Features

#### Durable Per-Message TTL
- **Issue**: TTL was not persisted across pod restarts, losing expiration metadata
- **Solution**: Upgraded to nats-wasip3 0.5.0 with native JetStream per-message TTL
- **Changes**:
  - All write operations (`put`, `create`, `cas`, `batch.put`) now support `ttl_seconds` parameter
  - Handler wires TTL through `put_with_ttl()`, `create_with_ttl()`, `update_with_ttl()` methods
  - KV buckets configured with `allow_msg_ttl: true` for durable server-side expiration
  - TTL now survives pod restarts via JetStream headers
- **Files Modified**: storage-service/src/handler.rs, storage-service/src/store.rs

#### Index Persistence
- **Issue**: Index definitions were lost on restart, requiring manual recreation
- **Solution**: Persist indexes to `_indexes` KV bucket with cross-replica sync
- **Changes**:
  - New `handle_index_create()` and `handle_index_drop()` handlers persist/delete index definitions as JSON
  - Index definitions stored in format: `table.index_name` → `{table, name, fields}`
  - Indexes automatically loaded at startup from `_indexes` KV bucket
  - Index watcher syncs changes across replicas in real-time
- **Files Modified**: storage-service/src/handler.rs, storage-service/src/main.rs (planned)

#### Compound Index Support
- **Issue**: Compound indexes were not consulted during query planning, forcing full table scans
- **Solution**: Enhanced `index_scan()` to try compound indexes before single-field indexes
- **Changes**:
  - `index_scan()` now checks if multiple filters can be satisfied by a compound index
  - Supports Eq comparisons on all fields of compound index
  - Remaining filters (if any) applied post-index as row filters
  - Falls back to single-field indexes if compound index not available
- **Files Modified**: storage-service/src/state.rs

#### Structured JSON Logging
- **Issue**: Unstructured `eprintln!` output difficult to parse and filter in production
- **Solution**: Created structured JSON logging module with escape handling
- **Changes**:
  - New `log.rs` module with `Log` builder pattern
  - Provides `log_info!()`, `log_warn!()`, `log_error!()` macros
  - Proper JSON string escaping (quotes, newlines, tabs, backslashes)
  - Integrated into startup sequence (TLS, NATS connection, config)
- **Files Modified**: storage-service/src/log.rs (new), storage-service/src/main.rs

#### Transaction ID Collision Mitigation
- **Issue**: 32-bit counter in transaction IDs could collide across replicas during high concurrency
- **Solution**: Upgraded to 64-bit atomic counter with nanosecond timestamp prefix
- **Changes**:
  - Transaction ID format: `{timestamp_hex}-{counter_16x}`
  - 64-bit counter dramatically reduces collision probability
  - Idempotent rollback semantics handle rare collisions gracefully
- **Files Modified**: storage-service/src/txn.rs

#### WAL Back-Pressure Configuration
- **Issue**: WAL stream could silently drop PREPARE records under high load with `discard: Old`
- **Solution**: Upgraded nats-wasip3 0.5.0 already configured with `discard: New`
- **Changes**:
  - WAL now uses `DiscardPolicy::New` for back-pressure instead of silent drops
  - Guarantees message ordering and prevents loss of in-flight transactions
- **Files Modified**: storage-service/src/txn.rs (already configured in 0.5.0)

#### Partition Model Documentation
- **Issue**: "Multi-tenant" terminology implied a security boundary that does not exist
- **Solution**: Renamed to "partitioned" to reflect that this is a logical key-namespace, not isolation
- **Changes**:
  - Renamed `_tenant` request field to `_partition`
  - Renamed `LDB_MULTI_TENANT` env var to `LDB_PARTITIONED`
  - Renamed `apply_tenant_prefix` → `apply_partition_prefix`, `multi_tenant: bool` → `partitioned: bool`
  - Renamed `tests/integration_multitenant.sh` → `tests/integration_partitioned.sh`
  - README clarifies partitions are not a security boundary
  - Recommended NATS account/permission-based isolation for production scenarios
- **Files Modified**: storage-service/src/{handler.rs,main.rs,tests.rs}, README.md, tests/

### 📝 Quality Improvements

#### Comprehensive Unit Test Suite
- **Coverage**: 29 unit tests covering critical functionality
- **Test Categories**:
  - Filter matching: Eq, Neq, Gt, Gte, Lt, Lte, Prefix comparisons
  - Multiple filter combinations: all-match and fail-on-one scenarios
  - Partition prefix: distinctness validation and prefix logic
  - JSON logging: quote, newline, tab escape handling
  - TTL handling: zero/normal/large value conversions
  - Transaction IDs: format validation and collision probability analysis
  - Compound indexes: key format distinctness and JSON serialization
  - Index persistence: KV storage format and JSON round-trip
  - WAL configuration: discard policy validation
- **Results**: All 29 tests passing
- **Files Added**: storage-service/src/tests.rs

#### Version Management
- **Changes**: All packages bumped to 1.0.0

### 🔧 Dependency Updates
- **nats-wasip3**: Upgraded from 0.3 to 0.7
  - 0.5: Added native per-message TTL support, improved `DiscardPolicy` options, better async/await patterns
  - 0.7: `kv::Operation` is now `#[non_exhaustive]` (handled with wildcard arm in watcher)

### 🎯 Features

- ✅ Durable TTL via JetStream headers (survives restarts)
- ✅ Persistent index definitions with cross-replica sync
- ✅ Compound index query optimization
- ✅ Structured JSON logging for observability
- ✅ Reduced transaction ID collision risk
- ✅ WAL back-pressure prevents silent drops
- ✅ Clear partition model documentation (no implied security boundary)
- ✅ Comprehensive test coverage (29 tests)
- ✅ Clean WASM release builds

### 📋 Breaking Changes
- **Wire protocol**: `_tenant` request field renamed to `_partition`. Clients must update.
- **Environment variable**: `LDB_MULTI_TENANT` renamed to `LDB_PARTITIONED`.
- Otherwise backward compatible with v0.3.0.

### 🔍 Known Limitations
- Multi-tenancy is logical partitioning only; use NATS accounts for cryptographic isolation
- Per-message TTL requires JetStream 2.9+; falls back to NATS KV limits if older
- Transaction ID collisions are theoretically possible but astronomically unlikely

### 📖 Migration Guide
No migration required from v0.3.0. Simply upgrade binaries and restart services:
1. Build WASM modules with cargo build --target wasm32-wasip3 --release
2. Deploy new storage-service.wasm
3. Existing data in KV buckets remains compatible
4. TTL will apply only to new records; existing records unaffected

### 🙏 Contributors
- TTL architecture: JetStream per-message headers
- Index persistence: Inspired by schema loading pattern
- Logging infrastructure: Structured JSON for production deployments
