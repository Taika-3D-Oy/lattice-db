//! Request dispatch and JSON protocol.
//!
//! ## NATS subject scheme
//!
//! ```text
//! ldb.get             {table, key}                     → {key, value, revision}
//! ldb.put             {table, key, value}              → {revision}
//! ldb.delete          {table, key}                     → {}
//! ldb.cas             {table, key, value, revision}    → {revision}
//! ldb.create          {table, key, value}              → {revision}
//! ldb.exists          {table, key}                     → {exists}
//! ldb.keys            {table, cursor?}                 → {keys, cursor?}
//! ldb.scan            {table, filters, order_by?, limit?, offset?} → {rows, total_count}
//! ldb.count           {table, filters}                 → {count}
//! ldb.index.create    {table, field}                   → {}
//! ldb.index.drop      {table, field}                   → {}
//! ldb.index.list      {table}                          → {indexes}
//! ldb.txn             {ops: [{op, table, key, value?}]} → {ok, results}
//! ```
//!
//! All values in request/response are base64-encoded when binary.
//! Errors are returned as `{"error": "description"}`.

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

use nats_wasi::client::{Client, Message, secs};
use nats_wasi::jetstream::JetStream;

use crate::state::{self, FieldFilter, SharedState};
use crate::store::SharedStore;
use crate::txn;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

// ── Request limits (S-04) ─────────────────────────────────────────────

/// Maximum decoded byte size of a value. Rejects oversized writes before they
/// reach NATS KV, preventing single-request memory exhaustion.
const MAX_VALUE_BYTES: usize = 1 * 1024 * 1024; // 1 MiB

/// Maximum byte length of a key.
const MAX_KEY_LEN: usize = 256;

/// Maximum byte length of a table name (after partition prefix is applied).
const MAX_TABLE_LEN: usize = 128;

/// Maximum number of entries in a batch.put request.
const MAX_BATCH_SIZE: usize = 256;

// ── Config ────────────────────────────────────────────────────────────

pub struct Config {
    pub auth_token: Option<String>,
    /// Partitioned mode: each request must include a `_partition` field whose
    /// value is prefixed onto the table name. NOTE: partitions are a logical
    /// key-namespace convenience, NOT a security boundary. Any caller with
    /// the shared auth token can read any partition by changing `_partition`.
    pub partitioned: bool,
}

pub type SharedConfig = Rc<Config>;

// ── Request types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct KeyReq {
    table: String,
    key: String,
}

#[derive(Deserialize)]
struct PutReq {
    table: String,
    key: String,
    value: String, // base64
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct CasReq {
    table: String,
    key: String,
    value: String, // base64
    revision: u64,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct TableReq {
    table: String,
}

#[derive(Deserialize)]
struct KeysReq {
    table: String,
    #[serde(default)]
    cursor: Option<u64>,
}

#[derive(Deserialize)]
struct FieldReq {
    table: String,
    field: String,
}

#[derive(Deserialize)]
struct ScanReq {
    table: String,
    #[serde(default)]
    filters: Vec<FieldFilter>,
    #[serde(default)]
    order_by: Option<SortByReq>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u32>,
    #[serde(default)]
    key_prefix: Option<String>,
}

#[derive(Deserialize)]
struct SortByReq {
    field: String,
    #[serde(default = "default_asc")]
    order: String,
}

fn default_asc() -> String {
    "asc".to_string()
}

#[derive(Deserialize)]
struct CountReq {
    table: String,
    #[serde(default)]
    filters: Vec<FieldFilter>,
}

#[derive(Deserialize)]
struct BatchGetReq {
    table: String,
    keys: Vec<String>,
}

#[derive(Deserialize)]
struct BatchPutEntry {
    key: String,
    value: String, // base64
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct BatchPutReq {
    table: String,
    entries: Vec<BatchPutEntry>,
}

#[derive(Deserialize)]
struct AggregateReq {
    table: String,
    #[serde(default)]
    filters: Vec<FieldFilter>,
    #[serde(default)]
    group_by: Option<String>,
    ops: Vec<AggOpReq>,
}

#[derive(Deserialize)]
struct AggOpReq {
    #[serde(rename = "fn")]
    fn_name: String,
    #[serde(default)]
    field: Option<String>,
}

#[derive(Deserialize)]
struct SchemaSetReq {
    table: String,
    schema: serde_json::Value,
}

#[derive(Deserialize)]
struct IndexCreateReq {
    table: String,
    /// Single field index.
    #[serde(default)]
    field: Option<String>,
    /// Compound index (multiple fields).
    #[serde(default)]
    fields: Option<Vec<String>>,
}

// ── Response types ─────────────────────────────────────────────────

#[derive(Serialize)]
struct RowResp {
    key: String,
    value: String, // base64
    revision: u64,
}

#[derive(Serialize)]
struct RevisionResp {
    revision: u64,
}

#[derive(Serialize)]
struct ExistsResp {
    exists: bool,
}

#[derive(Serialize)]
struct KeysResp {
    keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<u64>,
}

#[derive(Serialize)]
struct ScanResp {
    rows: Vec<RowResp>,
    total_count: u64,
}

#[derive(Serialize)]
struct CountResp {
    count: u64,
}

#[derive(Serialize)]
struct IndexesResp {
    indexes: Vec<String>,
}

#[derive(Serialize)]
struct ErrorResp {
    error: String,
}

#[derive(Serialize)]
struct EmptyResp {}

#[derive(Serialize)]
struct BatchGetResult {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct BatchGetResp {
    results: Vec<BatchGetResult>,
}

#[derive(Serialize)]
struct BatchPutResult {
    key: String,
    revision: u64,
}

#[derive(Serialize)]
struct BatchPutResp {
    results: Vec<BatchPutResult>,
}

#[derive(Serialize)]
struct AggregateResp {
    groups: Vec<state::AggGroup>,
}

#[derive(Serialize)]
struct SchemaResp {
    schema: serde_json::Value,
}

#[derive(Serialize)]
struct ChangeEvent {
    op: String,
    table: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
}

// ── Dispatch ───────────────────────────────────────────────────────

/// Handle an incoming request message. Replies via NATS to the reply-to subject.
pub async fn handle(client: &Client, js: &JetStream, config: &SharedConfig, state: &SharedState, store: &SharedStore, msg: Message) {
    let Some(reply_to) = msg.reply_to.as_deref() else {
        return; // no reply subject, nothing to respond to
    };

    // Auth check.
    if let Some(ref token) = config.auth_token {
        if let Err(e) = check_auth(&msg.payload, token) {
            let resp = serde_json::to_vec(&ErrorResp { error: e }).unwrap_or_default();
            let _ = client.publish(reply_to, &resp);
            return;
        }
    }

    // S-01: reject reserved (_-prefixed) table names before any prefix transformation.
    let op = msg.subject.strip_prefix("ldb.").unwrap_or(&msg.subject);
    if let Err(e) = check_no_reserved_tables(op, &msg.payload) {
        let resp = serde_json::to_vec(&ErrorResp { error: e }).unwrap_or_default();
        let _ = client.publish(reply_to, &resp);
        return;
    }

    // Partition prefix: rewrite table name in payload if partitioned mode is on.
    let payload = if config.partitioned {
        match apply_partition_prefix(&msg.payload) {
            Ok(p) => p,
            Err(e) => {
                let resp = serde_json::to_vec(&ErrorResp { error: e }).unwrap_or_default();
                let _ = client.publish(reply_to, &resp);
                return;
            }
        }
    } else {
        msg.payload
    };

    let partitioned = config.partitioned;
    let result = match op {
        "get" => handle_get(state, store, &payload).await,
        "put" => handle_put(client, state, store, &payload, partitioned).await,
        "delete" => handle_delete(client, state, store, &payload, partitioned).await,
        "cas" => handle_cas(client, state, store, &payload, partitioned).await,
        "create" => handle_create(client, state, store, &payload, partitioned).await,
        "exists" => handle_exists(state, store, &payload).await,
        "keys" => handle_keys(state, store, &payload).await,
        "scan" => handle_scan(state, store, &payload).await,
        "count" => handle_count(state, store, &payload).await,
        "index.create" => handle_index_create(state, store, &payload).await,
        "index.drop" => handle_index_drop(state, store, &payload).await,
        "index.list" => handle_index_list(state, &payload),
        "txn" => handle_txn(js, state, store, &payload).await,
        "batch.get" => handle_batch_get(state, store, &payload).await,
        "batch.put" => handle_batch_put(client, state, store, &payload, partitioned).await,
        "aggregate" => handle_aggregate(state, store, &payload).await,
        "schema.set" => handle_schema_set(state, store, &payload).await,
        "schema.get" => handle_schema_get(state, &payload),
        "schema.delete" => handle_schema_delete(state, store, &payload).await,
        _ => Err(format!("unknown operation: {op}")),
    };

    let resp_bytes = match result {
        Ok(json) => json,
        Err(e) => serde_json::to_vec(&ErrorResp { error: e }).unwrap_or_default(),
    };

    let _ = client.publish(reply_to, &resp_bytes);
}

// ── Ensure table is loaded ─────────────────────────────────────────

async fn ensure_loaded(table: &str, state: &SharedState, store: &SharedStore) -> Result<(), String> {
    let needs_load = {
        let s = state.borrow();
        !s.tables.get(table).map_or(false, |t| t.loaded)
    };
    if needs_load {
        let kv = crate::store::get_or_create_kv(store, table)
            .await
            .map_err(|e| format!("load table: {e}"))?;
        let entries = kv.load_all().await.map_err(|e| format!("load table: {e}"))?;

        // Find the highest revision we loaded so the watcher starts after it.
        let max_rev = entries.iter().map(|e| e.revision).max().unwrap_or(0);

        let mut s = state.borrow_mut();
        let ts = s.table(table);
        for entry in entries {
            ts.upsert(&entry.key, entry.value, entry.revision);
        }
        ts.loaded = true;

        // Spawn a background watcher if not already running.
        if !ts.watching {
            ts.watching = true;
            let watch_state = state.clone();
            let table_name = table.to_string();
            let kv_clone = kv.clone();
            wit_bindgen::spawn(async move {
                run_table_watcher(kv_clone, &table_name, &watch_state, max_rev).await;
            });
        }
    }
    Ok(())
}

/// Background loop that watches a KV bucket for changes from other replicas
/// and updates the local in-memory cache + indexes.
async fn run_table_watcher(kv: nats_wasi::kv::KeyValue, table: &str, state: &SharedState, start_after: u64) {
    let watcher = match kv.watch(start_after).await {
        Ok(w) => w,
        Err(e) => {
            eprintln!("lattice-db: watcher setup failed for {table}: {e}");
            state.borrow_mut().table(table).watching = false;
            return;
        }
    };
    eprintln!("lattice-db: watcher started for table {table} (after seq {start_after})");
    loop {
        let entry = match watcher.next().await {
            Ok(e) => e,
            Err(e) => {
                eprintln!("lattice-db: watcher error for {table}: {e}");
                break;
            }
        };

        let mut s = state.borrow_mut();
        let ts = s.table(table);

        match entry.operation {
            nats_wasi::kv::Operation::Put => {
                // Only apply if this is a newer revision than what we have.
                let dominated = ts.data.get(&entry.key)
                    .map_or(false, |r| r.revision >= entry.revision);
                if !dominated {
                    ts.upsert(&entry.key, entry.value, entry.revision);
                }
            }
            nats_wasi::kv::Operation::Delete | nats_wasi::kv::Operation::Purge => {
                // Remove from cache. Check revision to avoid deleting a newer write.
                let dominated = ts.data.get(&entry.key)
                    .map_or(false, |r| r.revision > entry.revision);
                if !dominated {
                    ts.remove(&entry.key);
                }
            }
            // `Operation` is `#[non_exhaustive]` since nats-wasip3 0.7; ignore unknown ops.
            _ => {}
        }
    }
    // If watcher disconnects, mark as not watching so it can be re-spawned.
    state.borrow_mut().table(table).watching = false;
    eprintln!("lattice-db: watcher stopped for table {table}");
}

// ── Operation handlers ─────────────────────────────────────────────

async fn handle_get(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: KeyReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table).ok_or("table not loaded")?;
    let row = table.data.get(&req.key).ok_or("not found")?;
    if row.is_expired() {
        return Err("not found".into());
    }

    ok_json(&RowResp {
        key: req.key,
        value: B64.encode(&row.value),
        revision: row.revision,
    })
}

async fn handle_put(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    partitioned: bool,
) -> Result<Vec<u8>, String> {
    let req: PutReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let revision = match req.ttl_seconds {
        Some(ttl) => kv.put_with_ttl(&req.key, &value, secs(ttl)).await.map_err(|e| format!("{e}"))?,
        None => kv.put(&req.key, &value).await.map_err(|e| format!("{e}"))?,
    };

    // Update cache + apply in-memory TTL for fast-path reads.
    {
        let mut s = state.borrow_mut();
        let ts = s.table(&req.table);
        ts.upsert(&req.key, value, revision);
        if let Some(ttl) = req.ttl_seconds {
            if let Some(row) = ts.data.get_mut(&req.key) {
                row.expires_at_ms = Some(state::clock_ms() + ttl * 1000);
            }
        }
    }

    publish_change(client, "put", &req.table, &req.key, Some(&req.value), Some(revision), partitioned);
    ok_json(&RevisionResp { revision })
}

async fn handle_delete(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    partitioned: bool,
) -> Result<Vec<u8>, String> {
    let req: KeyReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    kv.delete(&req.key).await.map_err(|e| format!("{e}"))?;

    // Update cache.
    state.borrow_mut().table(&req.table).remove(&req.key);

    publish_change(client, "delete", &req.table, &req.key, None, None, partitioned);
    ok_json(&EmptyResp {})
}

async fn handle_cas(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    partitioned: bool,
) -> Result<Vec<u8>, String> {
    let req: CasReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let revision = match req.ttl_seconds {
        Some(ttl) => kv.update_with_ttl(&req.key, &value, req.revision, secs(ttl)).await.map_err(|e| format!("{e}"))?,
        None => kv.update(&req.key, &value, req.revision).await.map_err(|e| format!("{e}"))?,
    };

    {
        let mut s = state.borrow_mut();
        let ts = s.table(&req.table);
        ts.upsert(&req.key, value, revision);
        if let Some(ttl) = req.ttl_seconds {
            if let Some(row) = ts.data.get_mut(&req.key) {
                row.expires_at_ms = Some(state::clock_ms() + ttl * 1000);
            }
        }
    }

    publish_change(client, "cas", &req.table, &req.key, Some(&req.value), Some(revision), partitioned);
    ok_json(&RevisionResp { revision })
}

async fn handle_create(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    partitioned: bool,
) -> Result<Vec<u8>, String> {
    let req: PutReq = parse_req(payload)?;
    let value = B64.decode(&req.value).map_err(|e| format!("base64: {e}"))?;
    validate_write_bounds(&req.table, &req.key, &value)?;
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            state::validate_schema(&value, schema)?;
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;
    let revision = match req.ttl_seconds {
        Some(ttl) => kv.create_with_ttl(&req.key, &value, secs(ttl)).await.map_err(|e| format!("{e}"))?,
        None => kv.create(&req.key, &value).await.map_err(|e| format!("{e}"))?,
    };

    {
        let mut s = state.borrow_mut();
        let ts = s.table(&req.table);
        ts.upsert(&req.key, value, revision);
        if let Some(ttl) = req.ttl_seconds {
            if let Some(row) = ts.data.get_mut(&req.key) {
                row.expires_at_ms = Some(state::clock_ms() + ttl * 1000);
            }
        }
    }

    publish_change(client, "create", &req.table, &req.key, Some(&req.value), Some(revision), partitioned);
    ok_json(&RevisionResp { revision })
}

async fn handle_exists(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: KeyReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let exists = {
        let s = state.borrow();
        s.tables.get(&req.table).map_or(false, |t| {
            t.data.get(&req.key).map_or(false, |row| !row.is_expired())
        })
    };

    ok_json(&ExistsResp { exists })
}

async fn handle_keys(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: KeysReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let mut keys: Vec<String> = table.map_or_else(Vec::new, |t| {
        t.data.iter()
            .filter(|(_, row)| !row.is_expired())
            .map(|(k, _)| k.clone())
            .collect()
    });
    keys.sort();

    let page_size = 100usize;
    let offset = req.cursor.unwrap_or(0) as usize;
    let page: Vec<String> = keys.into_iter().skip(offset).take(page_size).collect();
    let next_cursor = if page.len() == page_size {
        Some((offset + page_size) as u64)
    } else {
        None
    };

    ok_json(&KeysResp {
        keys: page,
        cursor: next_cursor,
    })
}

async fn handle_scan(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: ScanReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let Some(table) = table else {
        return ok_json(&ScanResp {
            rows: vec![],
            total_count: 0,
        });
    };

    // Try index scan, fall back to full scan.
    let matching_keys = match state::index_scan(table, &req.filters) {
        Some(keys) => keys,
        None => {
            table
                .data
                .iter()
                .filter(|(k, row)| {
                    !row.is_expired()
                        && req.key_prefix.as_ref().map_or(true, |pfx| k.starts_with(pfx.as_str()))
                        && state::matches_filters(&row.value, &req.filters)
                })
                .map(|(k, _)| k.clone())
                .collect()
        }
    };

    // Post-filter by key_prefix for index-scanned keys + filter expired.
    let matching_keys: Vec<String> = matching_keys
        .into_iter()
        .filter(|k| {
            let passes_prefix = req.key_prefix.as_ref().map_or(true, |pfx| k.starts_with(pfx.as_str()));
            let not_expired = table.data.get(k).map_or(false, |row| !row.is_expired());
            passes_prefix && not_expired
        })
        .collect();

    let total_count = matching_keys.len() as u64;

    // Sort if requested.
    let mut sorted_keys = matching_keys;
    if let Some(ref order_by) = req.order_by {
        sorted_keys.sort_by(|a, b| {
            let va = table
                .data
                .get(a)
                .and_then(|r| state::extract_json_field(&r.value, &order_by.field));
            let vb = table
                .data
                .get(b)
                .and_then(|r| state::extract_json_field(&r.value, &order_by.field));
            let cmp = va.as_deref().cmp(&vb.as_deref());
            if order_by.order == "desc" {
                cmp.reverse()
            } else {
                cmp
            }
        });
    }

    // Paginate.
    let offset = req.offset.unwrap_or(0) as usize;
    let limit = req.limit.unwrap_or(100) as usize;
    let page: Vec<RowResp> = sorted_keys
        .into_iter()
        .skip(offset)
        .take(limit)
        .filter_map(|k| {
            let row = table.data.get(&k)?;
            Some(RowResp {
                key: k,
                value: B64.encode(&row.value),
                revision: row.revision,
            })
        })
        .collect();

    ok_json(&ScanResp {
        rows: page,
        total_count,
    })
}

async fn handle_count(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: CountReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let count = match table {
        None => 0,
        Some(table) => match state::index_scan(table, &req.filters) {
            Some(keys) => keys
                .iter()
                .filter(|k| table.data.get(*k).map_or(false, |r| !r.is_expired()))
                .count() as u64,
            None => table
                .data
                .values()
                .filter(|row| !row.is_expired() && state::matches_filters(&row.value, &req.filters))
                .count() as u64,
        },
    };

    ok_json(&CountResp { count })
}

async fn handle_index_create(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: IndexCreateReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let index_name = if let Some(fields) = &req.fields {
        if fields.len() < 2 {
            return Err("compound index requires at least 2 fields".into());
        }
        state.borrow_mut().table(&req.table).create_compound_index(fields);
        fields.join("+")
    } else if let Some(ref field) = req.field {
        state.borrow_mut().table(&req.table).create_index(field);
        field.clone()
    } else {
        return Err("either 'field' or 'fields' is required".into());
    };

    // Persist index definition to _indexes bucket.
    let kv = crate::store::get_or_create_kv(store, "_indexes")
        .await
        .map_err(|e| format!("{e}"))?;
    let index_key = format!("{}.{}", req.table, index_name);
    let fields_list = if let Some(fields) = &req.fields {
        fields.clone()
    } else if let Some(field) = &req.field {
        vec![field.clone()]
    } else {
        return Err("no fields for index".into());
    };
    let index_def = serde_json::json!({
        "table": req.table,
        "name": index_name,
        "fields": fields_list,
    });
    let index_bytes = serde_json::to_vec(&index_def).map_err(|e| format!("{e}"))?;
    kv.put(&index_key, &index_bytes).await.map_err(|e| format!("{e}"))?;

    ok_json(&EmptyResp {})
}

async fn handle_index_drop(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: FieldReq = parse_req(payload)?;
    let mut s = state.borrow_mut();
    let t = s.table(&req.table);
    t.drop_index(&req.field);
    t.drop_compound_index(&req.field);
    drop(s); // explicitly drop to release borrow

    // Delete from _indexes bucket.
    let kv = crate::store::get_or_create_kv(store, "_indexes")
        .await
        .map_err(|e| format!("{e}"))?;
    let index_key = format!("{}.{}", req.table, req.field);
    let _ = kv.delete(&index_key).await;

    ok_json(&EmptyResp {})
}

fn handle_index_list(state: &SharedState, payload: &[u8]) -> Result<Vec<u8>, String> {
    let req: TableReq = parse_req(payload)?;
    let s = state.borrow();
    let mut indexes: Vec<String> = s
        .tables
        .get(&req.table)
        .map_or_else(Vec::new, |t| {
            let mut v: Vec<String> = t.indexes.keys().cloned().collect();
            v.extend(t.compound_indexes.keys().cloned());
            v
        });
    indexes.sort();
    ok_json(&IndexesResp { indexes })
}

async fn handle_txn(
    js: &JetStream,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: txn::TxnRequest = parse_req(payload)?;
    let resp = txn::execute(js, state, store, req).await?;
    ok_json(&resp)
}

// ── Batch operations ───────────────────────────────────────────────

async fn handle_batch_get(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: BatchGetReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let results: Vec<BatchGetResult> = req
        .keys
        .iter()
        .map(|key| {
            match table.and_then(|t| t.data.get(key)) {
                Some(row) if !row.is_expired() => BatchGetResult {
                    key: key.clone(),
                    value: Some(B64.encode(&row.value)),
                    revision: Some(row.revision),
                    error: None,
                },
                _ => BatchGetResult {
                    key: key.clone(),
                    value: None,
                    revision: None,
                    error: Some("not found".into()),
                },
            }
        })
        .collect();

    ok_json(&BatchGetResp { results })
}

async fn handle_batch_put(
    client: &Client,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
    partitioned: bool,
) -> Result<Vec<u8>, String> {
    let req: BatchPutReq = parse_req(payload)?;
    if req.entries.len() > MAX_BATCH_SIZE {
        return Err(format!("batch exceeds maximum of {MAX_BATCH_SIZE} entries"));
    }
    ensure_loaded(&req.table, state, store).await?;

    // Schema validation for all entries upfront.
    {
        let s = state.borrow();
        if let Some(schema) = s.tables.get(&req.table).and_then(|t| t.schema.as_ref()) {
            for entry in &req.entries {
                let value = B64.decode(&entry.value).map_err(|e| format!("base64 for {}: {e}", entry.key))?;
                state::validate_schema(&value, schema)?;
            }
        }
    }

    let kv = crate::store::get_or_create_kv(store, &req.table)
        .await
        .map_err(|e| format!("{e}"))?;

    let mut results = Vec::with_capacity(req.entries.len());
    for entry in &req.entries {
        let value = B64.decode(&entry.value).map_err(|e| format!("base64: {e}"))?;
        validate_write_bounds(&req.table, &entry.key, &value)?;
        let revision = match entry.ttl_seconds {
            Some(ttl) => kv.put_with_ttl(&entry.key, &value, secs(ttl)).await.map_err(|e| format!("{e}"))?,
            None => kv.put(&entry.key, &value).await.map_err(|e| format!("{e}"))?,
        };

        {
            let mut s = state.borrow_mut();
            let ts = s.table(&req.table);
            ts.upsert(&entry.key, value, revision);
            if let Some(ttl) = entry.ttl_seconds {
                if let Some(row) = ts.data.get_mut(&entry.key) {
                    row.expires_at_ms = Some(state::clock_ms() + ttl * 1000);
                }
            }
        }

        publish_change(client, "put", &req.table, &entry.key, Some(&entry.value), Some(revision), partitioned);
        results.push(BatchPutResult {
            key: entry.key.clone(),
            revision,
        });
    }

    ok_json(&BatchPutResp { results })
}

// ── Aggregation ────────────────────────────────────────────────────

async fn handle_aggregate(
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    let req: AggregateReq = parse_req(payload)?;
    ensure_loaded(&req.table, state, store).await?;

    let ops: Vec<state::AggOp> = req
        .ops
        .iter()
        .map(|o| state::AggOp {
            fn_name: o.fn_name.clone(),
            field: o.field.clone(),
        })
        .collect();

    let s = state.borrow();
    let table = s.tables.get(&req.table);
    let Some(table) = table else {
        return ok_json(&AggregateResp { groups: vec![] });
    };

    let groups = state::aggregate(table, &req.filters, req.group_by.as_deref(), &ops);
    ok_json(&AggregateResp { groups })
}

// ── Schema management ──────────────────────────────────────────────

async fn handle_schema_set(state: &SharedState, store: &SharedStore, payload: &[u8]) -> Result<Vec<u8>, String> {
    let req: SchemaSetReq = parse_req(payload)?;
    if let Some(fields) = req.schema.get("fields") {
        if !fields.is_object() {
            return Err("schema.fields must be an object".into());
        }
    }
    let kv = crate::store::get_or_create_kv(store, "_schemas")
        .await
        .map_err(|e| format!("{e}"))?;
    let schema_bytes = serde_json::to_vec(&req.schema).map_err(|e| format!("{e}"))?;
    kv.put(&req.table, &schema_bytes).await.map_err(|e| format!("{e}"))?;

    state.borrow_mut().table(&req.table).schema = Some(req.schema);
    ok_json(&EmptyResp {})
}

fn handle_schema_get(state: &SharedState, payload: &[u8]) -> Result<Vec<u8>, String> {
    let req: TableReq = parse_req(payload)?;
    let s = state.borrow();
    let schema = s
        .tables
        .get(&req.table)
        .and_then(|t| t.schema.clone())
        .unwrap_or(serde_json::Value::Null);
    ok_json(&SchemaResp { schema })
}

async fn handle_schema_delete(state: &SharedState, store: &SharedStore, payload: &[u8]) -> Result<Vec<u8>, String> {
    let req: TableReq = parse_req(payload)?;
    let kv = crate::store::get_or_create_kv(store, "_schemas")
        .await
        .map_err(|e| format!("{e}"))?;
    let _ = kv.delete(&req.table).await;

    state.borrow_mut().table(&req.table).schema = None;
    ok_json(&EmptyResp {})
}

// ── Helpers ────────────────────────────────────────────────────────

fn parse_req<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> Result<T, String> {
    serde_json::from_slice(payload).map_err(|e| format!("invalid request: {e}"))
}

fn ok_json<T: Serialize>(val: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(val).map_err(|e| format!("serialize: {e}"))
}

/// Publish a change event for watch/notification subscribers.
fn publish_change(
    client: &Client,
    op: &str,
    table: &str,
    key: &str,
    value: Option<&str>,
    revision: Option<u64>,
    partitioned: bool,
) {
    let event = ChangeEvent {
        op: op.to_string(),
        table: table.to_string(),
        key: key.to_string(),
        value: value.map(|v| v.to_string()),
        revision,
    };
    if let Ok(bytes) = serde_json::to_vec(&event) {
        // S-02: in partitioned mode, namespace the subject under the partition
        // so subscribers can scope to `ldb-events.{partition}.>` without
        // seeing events from other partitions.
        let subject = if partitioned {
            if let Some((partition, base_table)) = table.split_once('_') {
                format!("ldb-events.{partition}.{base_table}.{key}")
            } else {
                format!("ldb-events.{table}.{key}")
            }
        } else {
            format!("ldb-events.{table}.{key}")
        };
        let _ = client.publish(&subject, &bytes);
    }
}

/// S-01: reject requests targeting reserved (_-prefixed) table names.
///
/// Checked against the raw user-supplied payload, before any partition prefix
/// transformation, so `_indexes` and `_schemas` are always blocked.
pub(crate) fn check_no_reserved_tables(op: &str, payload: &[u8]) -> Result<(), String> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Ok(()); // malformed JSON is caught later by parse_req
    };
    if op == "txn" {
        if let Some(ops) = v.get("ops").and_then(|o| o.as_array()) {
            for entry in ops {
                if let Some(table) = entry.get("table").and_then(|t| t.as_str()) {
                    if table.starts_with('_') {
                        return Err(format!("table name '{table}' is reserved"));
                    }
                }
            }
        }
    } else if let Some(table) = v.get("table").and_then(|t| t.as_str()) {
        if table.starts_with('_') {
            return Err(format!("table name '{table}' is reserved"));
        }
    }
    Ok(())
}

/// S-04: validate key and decoded-value sizes before writing to NATS KV.
pub(crate) fn validate_write_bounds(table: &str, key: &str, value: &[u8]) -> Result<(), String> {
    if table.len() > MAX_TABLE_LEN {
        return Err(format!("table name exceeds maximum length of {MAX_TABLE_LEN}"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(format!("key exceeds maximum length of {MAX_KEY_LEN}"));
    }
    if value.len() > MAX_VALUE_BYTES {
        return Err(format!(
            "value exceeds maximum size of {} bytes",
            MAX_VALUE_BYTES
        ));
    }
    Ok(())
}

/// Check auth token in the request payload.
///
/// S-03: uses constant-time comparison to prevent timing-based token oracle attacks.
fn check_auth(payload: &[u8], expected: &str) -> Result<(), String> {
    let v: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "unauthorized".to_string())?;
    let provided = v.get("_auth").and_then(|v| v.as_str()).unwrap_or("");
    if !ct_eq(provided.as_bytes(), expected.as_bytes()) {
        return Err("unauthorized".into());
    }
    Ok(())
}

/// Constant-time byte-slice equality. Prevents timing-based token oracle attacks (S-03).
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    acc == 0
}

/// Apply partition prefix to the table name in the payload.
///
/// Partitions are a logical key-namespace convenience. They are NOT a security
/// boundary — any caller authenticated by the shared `LDB_AUTH_TOKEN` can read
/// or write any partition by changing the `_partition` field.
fn apply_partition_prefix(payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut v: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("invalid request: {e}"))?;
    let partition = v
        .get("_partition")
        .and_then(|v| v.as_str())
        .ok_or("_partition field required in partitioned mode")?
        .to_string();
    if partition.is_empty() || partition.len() > 64 {
        return Err("invalid partition ID".into());
    }
    if !partition.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        return Err("invalid partition ID: only alphanumeric, _, - allowed".into());
    }
    if let Some(table) = v.get("table").and_then(|t| t.as_str()).map(|t| t.to_string()) {
        v["table"] = serde_json::Value::String(format!("{partition}_{table}"));
    }
    // Also prefix table names inside transaction ops (ldb.txn).
    if let Some(ops) = v.get_mut("ops").and_then(|o| o.as_array_mut()) {
        for op in ops.iter_mut() {
            if let Some(table) = op.get("table").and_then(|t| t.as_str()).map(|t| t.to_string()) {
                op["table"] = serde_json::Value::String(format!("{partition}_{table}"));
            }
        }
    }
    serde_json::to_vec(&v).map_err(|e| format!("serialize: {e}"))
}
