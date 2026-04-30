//! TCP server for localhost (workload-internal) access.
//!
//! Listens on `127.0.0.1:PORT` and accepts length-prefixed JSON frames.
//! Wire protocol:
//!   Request:  [4 bytes: u32 BE payload length] [JSON payload with `_op` field]
//!   Response: [4 bytes: u32 BE payload length] [JSON payload]
//!
//! This allows co-located components to bypass NATS and talk directly to
//! the in-memory cache via virtual pipes (sub-millisecond latency).

use wasip3::sockets::types::{
    IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, TcpSocket,
};
use wasip3::wit_stream;
use wit_bindgen::{StreamReader, StreamResult, StreamWriter};

use crate::handler::{self, SharedConfig};
use crate::state::SharedState;
use crate::store::SharedStore;
use nats_wasi::client::Client;
use nats_wasi::jetstream::JetStream;

/// Default TCP port for the localhost listener.
const DEFAULT_PORT: u16 = 4080;

/// Start the TCP listener. Spawns a background task that never returns.
pub fn start(
    client: Client,
    js: JetStream,
    config: SharedConfig,
    state: SharedState,
    store: SharedStore,
) {
    let port = std::env::var("LDB_TCP_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);

    wit_bindgen::spawn(async move {
        if let Err(e) = run_listener(port, client, js, config, state, store).await {
            eprintln!("lattice-db: tcp listener fatal: {e}");
        }
    });
}

async fn run_listener(
    port: u16,
    client: Client,
    js: JetStream,
    config: SharedConfig,
    state: SharedState,
    store: SharedStore,
) -> Result<(), String> {
    let socket = TcpSocket::create(IpAddressFamily::Ipv4)
        .map_err(|e| format!("tcp create: {e:?}"))?;

    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress {
        port,
        address: (127, 0, 0, 1),
    });
    socket.bind(addr).map_err(|e| format!("tcp bind: {e:?}"))?;

    let mut incoming: StreamReader<TcpSocket> = socket
        .listen()
        .map_err(|e| format!("tcp listen: {e:?}"))?;

    eprintln!("lattice-db: tcp listening on 127.0.0.1:{port}");

    loop {
        let read_buf = Vec::with_capacity(1);
        let (status, sockets) = incoming.read(read_buf).await;
        match status {
            StreamResult::Complete(n) => {
                for conn in sockets.into_iter().take(n) {
                    let client = client.clone();
                    let js = js.clone();
                    let cfg = config.clone();
                    let st = state.clone();
                    let sto = store.clone();
                    wit_bindgen::spawn(async move {
                        handle_connection(conn, client, js, cfg, st, sto).await;
                    });
                }
            }
            StreamResult::Dropped | StreamResult::Cancelled => {
                return Err("listener stream closed".into());
            }
        }
    }
}

async fn handle_connection(
    conn: TcpSocket,
    client: Client,
    js: JetStream,
    config: SharedConfig,
    state: SharedState,
    store: SharedStore,
) {
    let (mut rx, _rx_done) = conn.receive();
    let (mut tx, tx_rx) = wit_stream::new::<u8>();
    let _send_fut = conn.send(tx_rx);

    let mut buf = Vec::new();

    loop {
        // Read until we have at least 4 bytes for the length prefix.
        while buf.len() < 4 {
            if stream_read_u8(&mut rx, &mut buf).await == 0 {
                return; // Connection closed.
            }
        }

        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        buf.drain(..4);

        // Read until we have the full payload.
        while buf.len() < len {
            if stream_read_u8(&mut rx, &mut buf).await == 0 {
                return; // Connection closed mid-frame.
            }
        }

        let payload: Vec<u8> = buf.drain(..len).collect();

        // Dispatch the request.
        let resp_bytes = dispatch(&client, &js, &config, &state, &store, &payload).await;

        // Write length-prefixed response.
        let resp_len = (resp_bytes.len() as u32).to_be_bytes();
        let mut frame = Vec::with_capacity(4 + resp_bytes.len());
        frame.extend_from_slice(&resp_len);
        frame.extend_from_slice(&resp_bytes);

        let remaining = tx.write_all(frame).await;
        if !remaining.is_empty() {
            return; // Connection broken.
        }
    }
}

/// Dispatch a TCP request. The payload is JSON with an `_op` field.
async fn dispatch(
    client: &Client,
    js: &JetStream,
    config: &SharedConfig,
    state: &SharedState,
    store: &SharedStore,
    payload: &[u8],
) -> Vec<u8> {
    // Parse the `_op` field from the JSON payload.
    let val: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            return serde_json::to_vec(&serde_json::json!({"error": format!("parse: {e}")}))
                .unwrap_or_default();
        }
    };

    let op = match val.get("_op").and_then(|v| v.as_str()) {
        Some(op) => op.to_string(),
        None => {
            return serde_json::to_vec(&serde_json::json!({"error": "missing _op field"}))
                .unwrap_or_default();
        }
    };

    // Auth check (same as NATS path).
    if let Some(ref token) = config.auth_token {
        if let Err(e) = handler::check_auth(payload, token) {
            return serde_json::to_vec(&serde_json::json!({"error": e})).unwrap_or_default();
        }
    }

    // Check reserved table names.
    if let Err(e) = handler::check_no_reserved_tables(&op, payload) {
        return serde_json::to_vec(&serde_json::json!({"error": e})).unwrap_or_default();
    }

    let instance = config.instance.as_str();
    let result = match op.as_str() {
        "get" => handler::handle_get(state, store, payload).await,
        "put" => handler::handle_put(client, state, store, payload, instance).await,
        "delete" => handler::handle_delete(client, state, store, payload, instance).await,
        "cas" => handler::handle_cas(client, state, store, payload, instance).await,
        "cas_delete" => handler::handle_cas_delete(client, state, store, payload, instance).await,
        "purge" => handler::handle_purge(client, state, store, payload, instance).await,
        "get_revision" => handler::handle_get_revision(state, store, payload).await,
        "create" => handler::handle_create(client, state, store, payload, instance).await,
        "exists" => handler::handle_exists(state, store, payload).await,
        "keys" => handler::handle_keys(state, store, payload).await,
        "scan" => handler::handle_scan(state, store, payload).await,
        "count" => handler::handle_count(state, store, payload).await,
        "index.create" => handler::handle_index_create(state, store, payload).await,
        "index.drop" => handler::handle_index_drop(state, store, payload).await,
        "index.list" => handler::handle_index_list(state, payload),
        "txn" => handler::handle_txn(js, state, store, payload, config.data_instance.as_str()).await,
        "batch.get" => handler::handle_batch_get(state, store, payload).await,
        "batch.put" => handler::handle_batch_put(client, state, store, payload, instance).await,
        "aggregate" => handler::handle_aggregate(state, store, payload).await,
        "schema.set" => handler::handle_schema_set(state, store, payload).await,
        "schema.get" => handler::handle_schema_get(state, payload),
        "schema.delete" => handler::handle_schema_delete(state, store, payload).await,
        _ => Err(format!("unknown operation: {op}")),
    };

    match result {
        Ok(json) => json,
        Err(e) => serde_json::to_vec(&serde_json::json!({"error": e})).unwrap_or_default(),
    }
}

async fn stream_read_u8(rx: &mut StreamReader<u8>, buf: &mut Vec<u8>) -> usize {
    let read_buf = Vec::with_capacity(8192);
    let (status, data) = rx.read(read_buf).await;
    match status {
        StreamResult::Complete(n) => {
            buf.extend_from_slice(&data[..n]);
            n
        }
        StreamResult::Dropped | StreamResult::Cancelled => 0,
    }
}
