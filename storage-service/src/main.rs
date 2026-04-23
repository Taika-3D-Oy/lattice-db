//! lattice-db storage service — main entry point (wasip3).
//!
//! Connects to NATS, subscribes to `ldb.>`, and dispatches incoming
//! requests to the handler. All state lives in-memory (backed by NATS KV).

mod handler;
mod log;
mod state;
mod store;
mod tests;
mod txn;

use nats_wasi::client::{Client, ConnectConfig};
use nats_wasi::jetstream::JetStream;
use std::rc::Rc;

// ── wasip3 command entry point ─────────────────────────────────────

wasip3::cli::command::export!(LatticeDb);

struct LatticeDb;

impl wasip3::exports::cli::run::Guest for LatticeDb {
    async fn run() -> Result<(), ()> {
        if let Err(e) = run().await {
            eprintln!("fatal: {e}");
        }
        Ok(())
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // NATS address: env var > argv > default.
    let nats_addr = std::env::var("NATS_URL")
        .ok()
        .or_else(|| std::env::args().nth(1))
        .unwrap_or_else(|| "127.0.0.1:4222".to_string());

    // TLS: enabled if NATS_TLS=1 is set.
    // Note: wasip3 uses host-side TLS via wasi:tls — no in-wasm crypto.
    // Custom CA certs are not yet supported by wasi:tls hosts.
    let use_tls = std::env::var("NATS_TLS").map_or(false, |v| v == "1" || v == "true");

    if use_tls {
        log_info!("TLS enabled (host-side wasi:tls)");
    }

    eprintln!("lattice-db: connecting to NATS at {nats_addr}");

    let client = Client::connect(ConnectConfig {
        address: nats_addr.to_string(),
        name: Some("lattice-db".to_string()),
        tls: use_tls,
        ..Default::default()
    })
    .await?;

    eprintln!(
        "lattice-db: connected to {} ({})",
        client.server_info().server_name,
        client.server_info().version,
    );

    // Instance name: drives all NATS subject and KV bucket prefixes so that
    // multiple lattice-db deployments sharing one NATS cluster stay isolated.
    let instance = {
        let raw = std::env::var("LDB_INSTANCE").unwrap_or_else(|_| "ldb".to_string());
        if raw.is_empty() || raw.len() > 64 {
            return Err("LDB_INSTANCE must be 1–64 characters".into());
        }
        if !raw.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            return Err("LDB_INSTANCE may only contain alphanumeric characters, _ and -".into());
        }
        eprintln!("lattice-db: instance = {raw}");
        raw
    };

    let shared_state = state::new_shared_state();
    let shared_store = store::new_shared_store(client.clone(), instance.clone());
    let js = JetStream::new(client.clone());

    // Auth config.
    let auth_token = std::env::var("LDB_AUTH_TOKEN").ok();
    if auth_token.is_some() {
        eprintln!("lattice-db: auth token required (_auth field)");
    }
    let config: handler::SharedConfig = Rc::new(handler::Config {
        auth_token,
        instance: instance.clone(),
    });

    // Set up WAL stream and recover incomplete transactions.
    txn::init_node_id();
    txn::ensure_wal_stream(&js, &instance)
        .await
        .map_err(|e| format!("wal stream setup: {e}"))?;

    let recovered = txn::recover(&js, &shared_state, &shared_store, &instance).await?;
    if recovered > 0 {
        eprintln!("lattice-db: recovered {recovered} incomplete transaction(s)");
    }

    // Load persisted schemas from KV.
    {
        let schema_kv = store::get_or_create_kv(&shared_store, "_schemas").await;
        if let Ok(kv) = schema_kv {
            let mut max_rev = 0u64;
            if let Ok(entries) = kv.load_all().await {
                let mut s = shared_state.borrow_mut();
                for entry in &entries {
                    if let Ok(schema) = serde_json::from_slice::<serde_json::Value>(&entry.value) {
                        s.table(&entry.key).schema = Some(schema);
                        eprintln!("lattice-db: loaded schema for table {}", entry.key);
                    }
                }
                max_rev = entries.iter().map(|e| e.revision).max().unwrap_or(0);
            }
            // Spawn schema watcher for cross-replica sync. Reconnects
            // automatically on disconnect so a NATS hiccup doesn't silently
            // stop schema propagation across replicas.
            let schema_state = shared_state.clone();
            let schema_kv_handle = kv.clone();
            wit_bindgen::spawn(async move {
                let mut since = max_rev;
                loop {
                    let watcher = match schema_kv_handle.watch(since).await {
                        Ok(w) => w,
                        Err(e) => {
                            eprintln!("lattice-db: schema watcher setup failed: {e} — retrying");
                            wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(5))
                                .await;
                            continue;
                        }
                    };
                    eprintln!("lattice-db: schema watcher started (after seq {since})");
                    loop {
                        let entry = match watcher.next().await {
                            Ok(e) => e,
                            Err(e) => {
                                eprintln!(
                                    "lattice-db: schema watcher disconnected: {e} — reconnecting"
                                );
                                break;
                            }
                        };
                        since = entry.revision;
                        let table_name = entry.key.clone();
                        match entry.operation {
                            nats_wasi::kv::Operation::Put => {
                                if let Ok(schema) =
                                    serde_json::from_slice::<serde_json::Value>(&entry.value)
                                {
                                    schema_state.borrow_mut().table(&table_name).schema =
                                        Some(schema);
                                    eprintln!(
                                        "lattice-db: schema updated for {table_name} (rev {})",
                                        entry.revision
                                    );
                                }
                            }
                            _ => {
                                schema_state.borrow_mut().table(&table_name).schema = None;
                                eprintln!("lattice-db: schema removed for {table_name}");
                            }
                        }
                    }
                }
            });
        }
    }

    // Load persisted index definitions from KV and rebuild in-memory indexes.
    // Without this, indexes are silently lost on reboot and never propagate
    // to newly scaled-up replicas.
    {
        let index_kv = store::get_or_create_kv(&shared_store, "_indexes").await;
        if let Ok(kv) = index_kv {
            let mut max_rev = 0u64;
            if let Ok(entries) = kv.load_all().await {
                let mut s = shared_state.borrow_mut();
                for entry in &entries {
                    let Ok(def) = serde_json::from_slice::<serde_json::Value>(&entry.value) else {
                        continue;
                    };
                    let Some(table) = def.get("table").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    let fields: Vec<String> = def
                        .get("fields")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    if fields.is_empty() {
                        continue;
                    }
                    let ts = s.table(table);
                    if fields.len() == 1 {
                        ts.create_index(&fields[0]);
                    } else {
                        ts.create_compound_index(&fields);
                    }
                    eprintln!("lattice-db: loaded index for {table}: {}", fields.join("+"));
                }
                max_rev = entries.iter().map(|e| e.revision).max().unwrap_or(0);
            }
            // Spawn index watcher for cross-replica sync. Reconnects
            // automatically on disconnect so a NATS hiccup doesn't silently
            // stop index propagation to newly scaled-up replicas.
            let index_state = shared_state.clone();
            let index_kv_handle = kv.clone();
            wit_bindgen::spawn(async move {
                let mut since = max_rev;
                loop {
                    let watcher = match index_kv_handle.watch(since).await {
                        Ok(w) => w,
                        Err(e) => {
                            eprintln!("lattice-db: index watcher setup failed: {e} — retrying");
                            wasip3::clocks::monotonic_clock::wait_for(nats_wasi::client::secs(5))
                                .await;
                            continue;
                        }
                    };
                    eprintln!("lattice-db: index watcher started (after seq {since})");
                    loop {
                        let entry = match watcher.next().await {
                            Ok(e) => e,
                            Err(e) => {
                                eprintln!(
                                    "lattice-db: index watcher disconnected: {e} — reconnecting"
                                );
                                break;
                            }
                        };
                        since = entry.revision;
                        match entry.operation {
                            nats_wasi::kv::Operation::Put => {
                                let Ok(def) =
                                    serde_json::from_slice::<serde_json::Value>(&entry.value)
                                else {
                                    continue;
                                };
                                let Some(table) =
                                    def.get("table").and_then(|v| v.as_str()).map(String::from)
                                else {
                                    continue;
                                };
                                let fields: Vec<String> = def
                                    .get("fields")
                                    .and_then(|v| v.as_array())
                                    .map(|a| {
                                        a.iter()
                                            .filter_map(|v| v.as_str().map(String::from))
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                if fields.is_empty() {
                                    continue;
                                }
                                let mut s = index_state.borrow_mut();
                                let ts = s.table(&table);
                                if fields.len() == 1 {
                                    ts.create_index(&fields[0]);
                                } else {
                                    ts.create_compound_index(&fields);
                                }
                                eprintln!(
                                    "lattice-db: index updated for {table}: {} (rev {})",
                                    fields.join("+"),
                                    entry.revision
                                );
                            }
                            _ => {
                                // Key format: "{table}.{index_name}".
                                if let Some((table, name)) = entry.key.split_once('.') {
                                    let mut s = index_state.borrow_mut();
                                    let ts = s.table(table);
                                    ts.drop_index(name);
                                    ts.drop_compound_index(name);
                                    eprintln!("lattice-db: index removed for {table}: {name}");
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    // Subscribe to all lattice-db operations (queue group for scaling).
    let sub_subject = format!("{instance}.>");
    let queue_group = format!("{instance}-workers");
    let sub = client.subscribe_queue(&sub_subject, &queue_group)?;

    eprintln!("lattice-db: listening on {sub_subject} (queue group: {queue_group})");

    loop {
        let msg = sub.next().await?;

        // Spawn a task per request for concurrency.
        let client = client.clone();
        let js = js.clone();
        let cfg = config.clone();
        let state = shared_state.clone();
        let store = shared_store.clone();
        wit_bindgen::spawn(async move {
            handler::handle(&client, &js, &cfg, &state, &store, msg).await;
        });
    }
}
