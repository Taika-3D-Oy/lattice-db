//! WAL-based transactions.
//!
//! Provides multi-key atomic writes using a write-ahead log stored in a
//! NATS JetStream stream (`ldb-txn`).
//!
//! ## Protocol
//!
//! ```text
//! ldb.txn  {ops: [{op, table, key, value?}], ...}  → {ok: true, results: [...]}
//! ```
//!
//! ## WAL lifecycle
//!
//! 1. **PREPARE** — snapshot before-state for each key, write WAL record
//! 2. **Apply** — execute each op via CAS (put, delete, create)
//! 3. **COMMIT** — mark WAL record as committed
//! 4. On failure: **rollback** each applied op, mark WAL as ABORT
//!
//! ## Recovery
//!
//! On startup, scan the WAL stream for any PREPARE record without a matching
//! COMMIT or ABORT. For each, roll back any ops whose revision matches what
//! the transaction wrote (idempotent).

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::cell::Cell;

use nats_wasi::client::secs;
use nats_wasi::jetstream::{JetStream, StreamConfig};
use nats_wasi::kv::{KeyValue, KvConfig};

use crate::state::SharedState;
use crate::store::SharedStore;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

const WAL_STREAM: &str = "ldb-txn";
const WAL_SUBJECT: &str = "_ldb.txn.wal";
/// KV bucket used to coordinate which replica recovers a given txn id.
const RECOVERY_LOCK_BUCKET: &str = "ldb-_recovery-locks";
/// PREPARE records younger than this are considered possibly-in-flight on a
/// peer and are NOT rolled back by another replica's recovery scan.
const RECOVERY_GRACE_SECS: i64 = 30;
/// Recovery-lock entries auto-expire after this many seconds, so a replica
/// that crashes mid-rollback doesn't block recovery forever.
const RECOVERY_LOCK_TTL_SECS: u64 = 300;

// ── Per-process node identity ─────────────────────────────────

thread_local! {
    /// Random per-startup id. Used to (a) tag WAL records so a replica can
    /// instantly recognise its own previous-incarnation PREPAREs, and
    /// (b) own recovery locks.
    static NODE_ID: Cell<u64> = const { Cell::new(0) };
}

/// Initialise the node id with a fresh random u64. Call once at startup
/// before [`recover`].
pub fn init_node_id() {
    let id = wasip3::random::random::get_random_u64();
    NODE_ID.with(|c| c.set(id));
}

/// Get the current process's node id (0 if [`init_node_id`] was not called).
pub fn node_id() -> u64 {
    NODE_ID.with(|c| c.get())
}

// ── Wire types ─────────────────────────────────────────────────────

/// A single operation in a transaction request.
#[derive(Debug, Deserialize)]
pub struct TxnOp {
    pub op: String, // "put", "delete", "create"
    pub table: String,
    pub key: String,
    #[serde(default)]
    pub value: Option<String>, // base64, required for put/create
}

/// The transaction request from the client.
#[derive(Debug, Deserialize)]
pub struct TxnRequest {
    pub ops: Vec<TxnOp>,
}

/// Per-op result returned to the client on success.
#[derive(Debug, Serialize)]
pub struct TxnOpResult {
    pub op: String,
    pub table: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
}

/// The transaction response to the client.
#[derive(Debug, Serialize)]
pub struct TxnResponse {
    pub ok: bool,
    pub results: Vec<TxnOpResult>,
}

// ── WAL record ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    id: String,
    status: WalStatus,
    ops: Vec<WalOp>,
    /// Random per-startup id of the replica that authored this record.
    /// Defaults to 0 for backwards compatibility with records written before
    /// node-id stamping was added.
    #[serde(default)]
    node_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum WalStatus {
    Prepare,
    Commit,
    Abort,
}

/// An operation as stored in the WAL, including before-state for rollback.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalOp {
    op: String,
    table: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_b64: Option<String>,
    // Before-state for rollback
    #[serde(skip_serializing_if = "Option::is_none")]
    before_value: Option<String>, // base64, None if key didn't exist
    #[serde(skip_serializing_if = "Option::is_none")]
    before_revision: Option<u64>,
    // After-state (filled in as ops are applied)
    #[serde(skip_serializing_if = "Option::is_none")]
    applied_revision: Option<u64>,
}

// ── WAL stream setup ───────────────────────────────────────────────

/// Ensure the WAL stream exists. Called on startup.
pub async fn ensure_wal_stream(js: &JetStream) -> Result<(), nats_wasi::Error> {
    let config = StreamConfig {
        name: WAL_STREAM.to_string(),
        subjects: vec![WAL_SUBJECT.to_string()],
        retention: nats_wasi::jetstream::Retention::Limits,
        max_consumers: -1,
        max_msgs: 10_000,
        max_bytes: 10 * 1024 * 1024,
        max_msg_size: -1,
        max_msgs_per_subject: 0,
        storage: nats_wasi::jetstream::Storage::File,
        num_replicas: 1,
        // Use New so the stream back-pressures writers rather than silently
        // dropping in-flight PREPARE records when capacity is reached.
        discard: nats_wasi::jetstream::DiscardPolicy::New,
        max_age: None,
        duplicate_window: None,
        allow_direct: true,
        allow_rollup_hdrs: false,
        allow_msg_ttl: false,
    };
    js.create_stream(&config).await?;
    Ok(())
}

// ── Execute transaction ────────────────────────────────────────────

/// Execute a multi-key transaction with WAL protection.
pub async fn execute(
    js: &JetStream,
    state: &SharedState,
    store: &SharedStore,
    req: TxnRequest,
) -> Result<TxnResponse, String> {
    if req.ops.is_empty() {
        return Err("transaction has no operations".into());
    }
    if req.ops.len() > 64 {
        return Err("transaction exceeds 64 operations limit".into());
    }

    // Validate ops upfront.
    for op in &req.ops {
        match op.op.as_str() {
            "put" | "create" => {
                if op.value.is_none() {
                    return Err(format!("{} requires a value (key: {})", op.op, op.key));
                }
                // Validate base64
                B64.decode(op.value.as_ref().unwrap())
                    .map_err(|e| format!("base64 for {}: {e}", op.key))?;
            }
            "delete" => {}
            other => return Err(format!("unsupported txn op: {other}")),
        }
    }

    // 1. Snapshot before-state for every key.
    let mut wal_ops = Vec::with_capacity(req.ops.len());
    for op in &req.ops {
        ensure_loaded(&op.table, state, store).await?;

        let (before_value, before_revision) = {
            let s = state.borrow();
            match s.tables.get(&op.table).and_then(|t| t.data.get(&op.key)) {
                Some(row) => (Some(B64.encode(&row.value)), Some(row.revision)),
                None => (None, None),
            }
        };

        wal_ops.push(WalOp {
            op: op.op.clone(),
            table: op.table.clone(),
            key: op.key.clone(),
            value_b64: op.value.clone(),
            before_value,
            before_revision,
            applied_revision: None,
        });
    }

    // 2. Write WAL PREPARE record.
    let txn_id = generate_txn_id();
    let wal = WalRecord {
        id: txn_id.clone(),
        status: WalStatus::Prepare,
        ops: wal_ops.clone(),
        node_id: node_id(),
    };
    let wal_bytes = serde_json::to_vec(&wal).map_err(|e| format!("wal serialize: {e}"))?;
    js.publish(WAL_SUBJECT, &wal_bytes)
        .await
        .map_err(|e| format!("wal write: {e}"))?;

    // 3. Apply each op.
    let mut results = Vec::with_capacity(wal_ops.len());
    let mut applied_count = 0usize;

    for (i, wal_op) in wal_ops.iter_mut().enumerate() {
        match apply_op(state, store, wal_op).await {
            Ok(revision) => {
                wal_op.applied_revision = Some(revision);
                applied_count += 1;
                results.push(TxnOpResult {
                    op: wal_op.op.clone(),
                    table: wal_op.table.clone(),
                    key: wal_op.key.clone(),
                    revision: Some(revision),
                });
            }
            Err(e) => {
                // Apply failed — rollback everything we've done so far.
                let rollback_err = rollback(state, store, &wal_ops[..i]).await;
                // Write ABORT to WAL.
                let abort = WalRecord {
                    id: txn_id,
                    status: WalStatus::Abort,
                    ops: wal_ops,
                    node_id: node_id(),
                };
                let abort_bytes =
                    serde_json::to_vec(&abort).map_err(|e2| format!("wal abort: {e2}"))?;
                let _ = js.publish(WAL_SUBJECT, &abort_bytes).await;

                let mut msg = format!(
                    "txn failed at op {i} ({} {}/{}): {e}",
                    req.ops[i].op, req.ops[i].table, req.ops[i].key
                );
                if applied_count > 0 {
                    msg.push_str(&format!("; rolled back {applied_count} ops"));
                }
                if let Err(re) = rollback_err {
                    msg.push_str(&format!("; rollback error: {re}"));
                }
                return Err(msg);
            }
        }
    }

    // 4. All ops applied — write COMMIT to WAL.
    let commit = WalRecord {
        id: txn_id,
        status: WalStatus::Commit,
        ops: wal_ops,
        node_id: node_id(),
    };
    let commit_bytes = serde_json::to_vec(&commit).map_err(|e| format!("wal commit: {e}"))?;
    js.publish(WAL_SUBJECT, &commit_bytes)
        .await
        .map_err(|e| format!("wal commit write: {e}"))?;

    Ok(TxnResponse { ok: true, results })
}

// ── Recovery ───────────────────────────────────────────────────────

/// Open (or create) the recovery-lock KV bucket.
///
/// Bucket-wide TTL ensures that a replica which crashes mid-rollback does not
/// block recovery forever — stale locks expire and another replica can claim.
async fn open_recovery_lock_bucket(js: &JetStream) -> Result<KeyValue, nats_wasi::Error> {
    KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: RECOVERY_LOCK_BUCKET.to_string(),
            history: 1,
            ttl: Some(secs(RECOVERY_LOCK_TTL_SECS)),
            ..Default::default()
        },
    )
    .await
}

/// Parse an RFC 3339 timestamp (`YYYY-MM-DDTHH:MM:SS[.frac]Z`) into Unix
/// seconds. Returns `None` for any unexpected shape so callers fall back to
/// the conservative "don't recover" path rather than miscomputing age.
fn parse_rfc3339_unix_secs(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let mo: u32 = s.get(5..7)?.parse().ok()?;
    let d: u32 = s.get(8..10)?.parse().ok()?;
    let h: i64 = s.get(11..13)?.parse().ok()?;
    let mi: i64 = s.get(14..16)?.parse().ok()?;
    let se: i64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    // Howard Hinnant's days_from_civil algorithm.
    let y_adj = if mo <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let mp = if mo > 2 { mo as i64 - 3 } else { mo as i64 + 9 };
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(days * 86400 + h * 3600 + mi * 60 + se)
}

/// Try to atomically claim ownership of recovery for `txn_id`. Returns true
/// if this replica won the claim, false if another replica beat us to it.
async fn claim_recovery_lock(lock_kv: &KeyValue, txn_id: &str) -> bool {
    let owner = node_id().to_string();
    lock_kv.create(txn_id, owner.as_bytes()).await.is_ok()
}

/// Scan the WAL stream and roll back any incomplete (PREPARE-only) transactions.
/// Called once on startup before accepting requests.
///
/// Cross-replica safety:
/// - PREPAREs younger than `RECOVERY_GRACE_SECS` are skipped — they may still
///   be in flight on a peer.
/// - PREPAREs authored by this very process (matching `node_id`) are recovered
///   immediately, since they cannot still be in flight.
/// - Per-txn recovery locks (CAS create on `_recovery-locks` bucket) ensure
///   only one replica performs the rollback even if two scan concurrently.
pub async fn recover(
    js: &JetStream,
    state: &SharedState,
    store: &SharedStore,
) -> Result<u32, String> {
    // Get stream info to know the sequence range.
    let info = match js.stream_info(WAL_STREAM).await {
        Ok(info) => info,
        Err(nats_wasi::Error::JetStream { code: 404, .. }) => return Ok(0),
        Err(e) => return Err(format!("wal stream info: {e}")),
    };

    if info.state.messages == 0 {
        return Ok(0);
    }

    // Open the recovery-lock bucket up front. If we can't, skip recovery
    // entirely — better to leave PREPAREs in place for another replica than
    // race without coordination.
    let lock_kv = match open_recovery_lock_bucket(js).await {
        Ok(kv) => kv,
        Err(e) => {
            eprintln!("lattice-db: recovery lock bucket setup failed: {e} — skipping recovery");
            return Ok(0);
        }
    };

    // Scan all WAL records by sequence number, capturing each PREPARE's
    // server-side timestamp (RFC 3339) and authoring node_id.
    struct PrepareInfo {
        ops: Vec<WalOp>,
        time: Option<String>,
        author_node_id: u64,
    }
    let mut prepares: std::collections::HashMap<String, PrepareInfo> =
        std::collections::HashMap::new();
    let mut resolved: std::collections::HashSet<String> = std::collections::HashSet::new();

    let first = info.state.first_seq;
    let last = info.state.last_seq;

    for seq in first..=last {
        let msg = match js.stream_get_msg(WAL_STREAM, seq).await {
            Ok(Some(m)) => m,
            Ok(None) => continue,
            Err(e) => {
                eprintln!("lattice-db: wal recovery skip seq {seq}: {e}");
                continue;
            }
        };

        let record: WalRecord = match serde_json::from_slice(&msg.data) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("lattice-db: wal recovery skip seq {seq}: bad record: {e}");
                continue;
            }
        };

        match record.status {
            WalStatus::Prepare => {
                prepares.insert(
                    record.id,
                    PrepareInfo {
                        ops: record.ops,
                        time: msg.time,
                        author_node_id: record.node_id,
                    },
                );
            }
            WalStatus::Commit | WalStatus::Abort => {
                resolved.insert(record.id);
            }
        }
    }

    // Local wall-clock now, used as the reference point for the grace check
    // against server-side timestamps. Modest local clock skew (sub-minute)
    // is fine: the only failure mode is recovering slightly later than
    // optimal, never recovering an in-flight peer txn.
    let now_unix = wasip3::clocks::system_clock::now().seconds;
    let own_node = node_id();

    // Find unresolved PREPARE records eligible for rollback.
    let mut recovered = 0u32;
    for (id, info) in &prepares {
        if resolved.contains(id) {
            continue;
        }

        // Decide whether this PREPARE is safe to recover from this replica.
        let is_own_previous_incarnation =
            info.author_node_id != 0 && info.author_node_id == own_node;
        let aged_out = match info.time.as_deref().and_then(parse_rfc3339_unix_secs) {
            Some(t) => now_unix.saturating_sub(t) > RECOVERY_GRACE_SECS,
            // No server timestamp parseable — fall back to the conservative
            // "wait" path. The next recovery scan will pick it up.
            None => false,
        };
        if !is_own_previous_incarnation && !aged_out {
            eprintln!(
                "lattice-db: skipping recent PREPARE {id} (author=node:{:x}, may be in flight)",
                info.author_node_id
            );
            continue;
        }

        // Race-free claim: only the replica whose `kv.create` succeeds runs
        // rollback for this txn. Lock has bucket-wide TTL so a crashed claimer
        // cannot block recovery indefinitely.
        if !claim_recovery_lock(&lock_kv, id).await {
            eprintln!("lattice-db: PREPARE {id} already being recovered by another replica");
            continue;
        }

        eprintln!(
            "lattice-db: recovering incomplete txn {id} ({} ops, author=node:{:x})",
            info.ops.len(),
            info.author_node_id
        );

        for op in &info.ops {
            ensure_loaded(&op.table, state, store).await?;
        }

        if let Err(e) = rollback(state, store, &info.ops).await {
            eprintln!("lattice-db: recovery rollback error for {id}: {e}");
        }

        // Write ABORT so we don't process this again.
        let abort = WalRecord {
            id: id.clone(),
            status: WalStatus::Abort,
            ops: info.ops.clone(),
            node_id: own_node,
        };
        let abort_bytes = serde_json::to_vec(&abort).unwrap_or_default();
        let _ = js.publish(WAL_SUBJECT, &abort_bytes).await;

        recovered += 1;
    }

    Ok(recovered)
}

// ── Internal helpers ───────────────────────────────────────────────

/// Apply a single operation. Returns the new revision.
async fn apply_op(state: &SharedState, store: &SharedStore, wal_op: &WalOp) -> Result<u64, String> {
    let kv = crate::store::get_or_create_kv(store, &wal_op.table)
        .await
        .map_err(|e| format!("{e}"))?;

    match wal_op.op.as_str() {
        "put" => {
            let value = B64
                .decode(wal_op.value_b64.as_ref().unwrap())
                .map_err(|e| format!("base64: {e}"))?;
            let rev = kv
                .put(&wal_op.key, &value)
                .await
                .map_err(|e| format!("{e}"))?;
            state
                .borrow_mut()
                .table(&wal_op.table)
                .upsert(&wal_op.key, value, rev);
            Ok(rev)
        }
        "create" => {
            let value = B64
                .decode(wal_op.value_b64.as_ref().unwrap())
                .map_err(|e| format!("base64: {e}"))?;
            let rev = kv
                .create(&wal_op.key, &value)
                .await
                .map_err(|e| format!("{e}"))?;
            state
                .borrow_mut()
                .table(&wal_op.table)
                .upsert(&wal_op.key, value, rev);
            Ok(rev)
        }
        "delete" => {
            kv.delete(&wal_op.key).await.map_err(|e| format!("{e}"))?;
            state.borrow_mut().table(&wal_op.table).remove(&wal_op.key);
            Ok(0)
        }
        other => Err(format!("unsupported op: {other}")),
    }
}

/// Roll back applied ops in reverse order. Idempotent.
async fn rollback(state: &SharedState, store: &SharedStore, ops: &[WalOp]) -> Result<(), String> {
    for wal_op in ops.iter().rev() {
        let Some(applied_rev) = wal_op.applied_revision else {
            continue;
        };

        let current_rev = {
            let s = state.borrow();
            s.tables
                .get(&wal_op.table)
                .and_then(|t| t.data.get(&wal_op.key))
                .map(|r| r.revision)
        };

        let kv = crate::store::get_or_create_kv(store, &wal_op.table)
            .await
            .map_err(|e| format!("{e}"))?;

        match wal_op.op.as_str() {
            "put" | "create" => {
                if current_rev != Some(applied_rev) && applied_rev != 0 {
                    continue;
                }
                match (&wal_op.before_value, wal_op.before_revision) {
                    (Some(before_b64), Some(_before_rev)) => {
                        let old_value = B64
                            .decode(before_b64)
                            .map_err(|e| format!("rollback base64: {e}"))?;
                        if let Ok(rev) = kv.put(&wal_op.key, &old_value).await {
                            state.borrow_mut().table(&wal_op.table).upsert(
                                &wal_op.key,
                                old_value,
                                rev,
                            );
                        }
                    }
                    _ => {
                        let _ = kv.delete(&wal_op.key).await;
                        state.borrow_mut().table(&wal_op.table).remove(&wal_op.key);
                    }
                }
            }
            "delete" => {
                if let (Some(before_b64), Some(_)) = (&wal_op.before_value, wal_op.before_revision)
                {
                    let old_value = B64
                        .decode(before_b64)
                        .map_err(|e| format!("rollback base64: {e}"))?;
                    if let Ok(rev) = kv.put(&wal_op.key, &old_value).await {
                        state
                            .borrow_mut()
                            .table(&wal_op.table)
                            .upsert(&wal_op.key, old_value, rev);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Ensure a table's data is loaded from NATS KV into the in-memory cache.
async fn ensure_loaded(
    table: &str,
    state: &SharedState,
    store: &SharedStore,
) -> Result<(), String> {
    let needs_load = {
        let s = state.borrow();
        !s.tables.get(table).map_or(false, |t| t.loaded)
    };
    if needs_load {
        let kv = crate::store::get_or_create_kv(store, table)
            .await
            .map_err(|e| format!("load table: {e}"))?;
        let entries = kv
            .load_all()
            .await
            .map_err(|e| format!("load table: {e}"))?;
        let mut s = state.borrow_mut();
        let ts = s.table(table);
        for entry in entries {
            ts.upsert(&entry.key, entry.value, entry.revision);
        }
        ts.loaded = true;
    }
    Ok(())
}

/// Generate a unique transaction ID.
/// Format: {timestamp_in_hex}-{counter_in_hex}
/// The counter is extended to u64 to reduce single-replica collisions.
/// Across replicas, collisions are rare due to independent counter evolution,
/// and idempotent rollback semantics handle them gracefully.
fn generate_txn_id() -> String {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let ts = wasip3::clocks::monotonic_clock::now();
    let cnt = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{:x}-{:016x}", ts, cnt)
}
