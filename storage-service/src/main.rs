//! lattice-db storage service — main entry point (wasip3).
//!
//! Connects to NATS, subscribes to `ldb.>`, and dispatches incoming
//! requests to the handler. All state lives in-memory (backed by NATS KV).

mod handler;
mod state;
mod store;
mod txn;

use nats_wasi::client::{Client, ConnectConfig, secs};
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
    // Initialize monotonic clock for TTL tracking.
    state::init_clock();
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
        eprintln!("lattice-db: TLS enabled (host-side wasi:tls)");
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

    let shared_state = state::new_shared_state();
    let shared_store = store::new_shared_store(client.clone());
    let js = JetStream::new(client.clone());

    // Auth / multi-tenancy config.
    let auth_token = std::env::var("LDB_AUTH_TOKEN").ok();
    let multi_tenant = std::env::var("LDB_MULTI_TENANT").map_or(false, |v| v == "1" || v == "true");
    if auth_token.is_some() {
        eprintln!("lattice-db: auth token required (_auth field)");
    }
    if multi_tenant {
        eprintln!("lattice-db: multi-tenant mode enabled (_tenant field required)");
    }
    let config: handler::SharedConfig = Rc::new(handler::Config {
        auth_token,
        multi_tenant,
    });

    // Set up WAL stream and recover incomplete transactions.
    txn::ensure_wal_stream(&js)
        .await
        .map_err(|e| format!("wal stream setup: {e}"))?;

    let recovered = txn::recover(&js, &shared_state, &shared_store).await?;
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
            // Spawn schema watcher for cross-replica sync.
            let schema_state = shared_state.clone();
            let schema_kv_handle = kv.clone();
            wit_bindgen::spawn(async move {
                let watcher = match schema_kv_handle.watch(max_rev).await {
                    Ok(w) => w,
                    Err(e) => {
                        eprintln!("lattice-db: schema watcher setup failed: {e}");
                        return;
                    }
                };
                eprintln!("lattice-db: schema watcher started (after seq {max_rev})");
                loop {
                    let entry = match watcher.next().await {
                        Ok(e) => e,
                        Err(e) => {
                            eprintln!("lattice-db: schema watcher error: {e}");
                            break;
                        }
                    };
                    let table_name = entry.key;
                    match entry.operation {
                        nats_wasi::kv::Operation::Put => {
                            if let Ok(schema) = serde_json::from_slice::<serde_json::Value>(&entry.value) {
                                schema_state.borrow_mut().table(&table_name).schema = Some(schema);
                                eprintln!("lattice-db: schema updated for {table_name} (rev {})", entry.revision);
                            }
                        }
                        _ => {
                            schema_state.borrow_mut().table(&table_name).schema = None;
                            eprintln!("lattice-db: schema removed for {table_name}");
                        }
                    }
                }
            });
        }
    }

    // Background reaper for expired entries.
    {
        let reaper_state = shared_state.clone();
        wit_bindgen::spawn(async move {
            loop {
                // Sleep ~30s using P3 monotonic clock.
                wasip3::clocks::monotonic_clock::wait_for(secs(30)).await;
                let reaped = reaper_state.borrow_mut().reap_expired();
                if reaped > 0 {
                    eprintln!("lattice-db: reaped {reaped} expired entries");
                }
            }
        });
    }

    // Subscribe to all lattice-db operations (queue group for scaling).
    let sub = client.subscribe_queue("ldb.>", "ldb-workers")?;

    eprintln!("lattice-db: listening on ldb.> (queue group: ldb-workers)");

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
