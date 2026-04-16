//! lattice-sql — SQL frontend for lattice-db.
//!
//! Connects to NATS, loads the SQL catalog from lattice-db, and serves
//! SQL queries on `ldb.sql.>` subjects.

mod catalog;
mod executor;
mod handler;
mod join;
mod parser;
mod planner;

use lattice_db_client::LatticeDb;
use nats_wasi::client::{Client, ConnectConfig};

// ── wasip3 command entry point ─────────────────────────────────────

wasip3::cli::command::export!(LatticeSql);

struct LatticeSql;

impl wasip3::exports::cli::run::Guest for LatticeSql {
    async fn run() -> Result<(), ()> {
        if let Err(e) = run().await {
            eprintln!("fatal: {e}");
        }
        Ok(())
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let nats_addr = std::env::var("NATS_URL")
        .ok()
        .or_else(|| std::env::args().nth(1))
        .unwrap_or_else(|| "127.0.0.1:4222".to_string());

    let use_tls = std::env::var("NATS_TLS").map_or(false, |v| v == "1" || v == "true");

    eprintln!("lattice-sql: connecting to NATS at {nats_addr}");

    let client = Client::connect(ConnectConfig {
        address: nats_addr.to_string(),
        name: Some("lattice-sql".to_string()),
        tls: use_tls,
        ..Default::default()
    })
    .await?;

    eprintln!(
        "lattice-sql: connected to {} ({})",
        client.server_info().server_name,
        client.server_info().version,
    );

    // Create lattice-db client (talks to storage-service over NATS).
    let db = LatticeDb::new(client.clone());

    // Load SQL catalog from lattice-db.
    let catalog = catalog::Catalog::load(&db)
        .await
        .map_err(|e| format!("catalog load: {e}"))?;
    let tables = catalog.table_names();
    eprintln!(
        "lattice-sql: loaded catalog ({} table{})",
        tables.len(),
        if tables.len() == 1 { "" } else { "s" }
    );
    for t in &tables {
        eprintln!("  - {t}");
    }

    let shared_catalog = handler::new_shared_catalog(catalog);

    // Subscribe to SQL subjects (queue group for horizontal scaling).
    let sub = client.subscribe_queue("ldb.sql.>", "ldb-sql-workers")?;

    eprintln!("lattice-sql: listening on ldb.sql.> (queue group: ldb-sql-workers)");

    loop {
        let msg = sub.next().await?;

        let client = client.clone();
        let db_handle = LatticeDb::new(client.clone());
        let cat = shared_catalog.clone();
        wit_bindgen::spawn(async move {
            handler::handle(&client, &db_handle, &cat, msg).await;
        });
    }
}
