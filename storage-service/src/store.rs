//! NATS KV wrapper — maps tables to KV buckets.
//!
//! Each table `foo` maps to NATS KV bucket `ldb-foo`. Buckets are created
//! lazily on first access. The Store is now a simple bucket cache — all
//! async KV operations happen outside the RefCell borrow to avoid panics
//! under concurrent tasks.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use nats_wasi::client::Client;
use nats_wasi::jetstream::JetStream;
use nats_wasi::kv::{KeyValue, KvConfig};
use nats_wasi::Error;

/// Persistent store backed by NATS KV — bucket cache only.
pub struct Store {
    js: JetStream,
    /// Open KV buckets, keyed by table name.
    buckets: HashMap<String, KeyValue>,
}

impl Store {
    pub fn new(client: Client) -> Self {
        Self {
            js: JetStream::new(client),
            buckets: HashMap::new(),
        }
    }

    /// Get a cloned JetStream handle.
    pub fn js(&self) -> JetStream {
        self.js.clone()
    }

    /// Get a cloned KV handle if the bucket is already open.
    pub fn get_kv(&self, table: &str) -> Option<KeyValue> {
        self.buckets.get(table).cloned()
    }

    /// Store a KV handle in the cache.
    pub fn insert_kv(&mut self, table: &str, kv: KeyValue) {
        self.buckets.insert(table.to_string(), kv);
    }
}

/// Shared store handle.
pub type SharedStore = Rc<RefCell<Store>>;

pub fn new_shared_store(client: Client) -> SharedStore {
    Rc::new(RefCell::new(Store::new(client)))
}

/// Get or create a KV bucket handle without holding the store borrow during
/// async bucket creation. This is the key fix for RefCell contention.
pub async fn get_or_create_kv(store: &SharedStore, table: &str) -> Result<KeyValue, Error> {
    // Fast path: bucket already cached (very brief borrow).
    if let Some(kv) = store.borrow().get_kv(table) {
        return Ok(kv);
    }
    // Slow path: create bucket. Clone JS handle and release borrow first.
    let js = store.borrow().js();
    let bucket_name = format!("ldb-{table}");
    let kv = KeyValue::new(
        js,
        KvConfig {
            bucket: bucket_name,
            history: 1,
            // Enable per-message TTL (NATS server 2.11+). This makes TTL
            // durable across restarts — expired keys are never resurrected.
            allow_msg_ttl: true,
            ..Default::default()
        },
    )
    .await?;
    store.borrow_mut().insert_kv(table, kv.clone());
    Ok(kv)
}
