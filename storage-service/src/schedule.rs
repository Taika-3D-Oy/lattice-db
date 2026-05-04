//! Server-side message scheduling support (NATS 2.14+, ADR-51).
//!
//! ## How it works
//!
//! `schedule_put` publishes a **schedule message** to the `{data_instance}-sched`
//! JetStream stream.  The NATS server re-publishes the body on the schedule's
//! target subject when the `@at` timestamp arrives.  storage-service subscribes
//! to those fire subjects and performs the actual KV `put`.
//!
//! ## Subject layout (all within `{data_instance}-sched` stream)
//!
//! | Subject | Role |
//! |---|---|
//! | `{data_instance}-sched.{table}.{key}` | Holds the schedule definition |
//! | `{data_instance}-sched-fire.{table}.{key}` | Target — NATS fires here |
//!
//! The body stored in the schedule message (and delivered to the fire subject)
//! is a small JSON envelope:
//! ```json
//! {"v":"<base64 value>","ttl":null}
//! ```
//!
//! storage-service runs a plain subscription on `{data_instance}-sched-fire.>`
//! and on each delivery performs a KV put using the table/key extracted from
//! the subject.

use nats_wasi::jetstream::{JetStream, StreamConfig};
use nats_wasi::schedule::{Schedule, ScheduleSpec};
use nats_wasi::Error;

/// Name of the schedules JetStream stream.
pub fn schedule_stream(data_instance: &str) -> String {
    format!("{data_instance}-sched")
}

/// Subject prefix for schedule definition messages.
pub fn schedule_slot_prefix(data_instance: &str) -> String {
    format!("{data_instance}-sched.")
}

/// Subject for a specific schedule slot.
pub fn schedule_slot(data_instance: &str, table: &str, key: &str) -> String {
    format!("{data_instance}-sched.{table}.{key}")
}

/// Wildcard subject for all fired schedule deliveries.
pub fn schedule_fire_wildcard(data_instance: &str) -> String {
    format!("{data_instance}-sched-fire.>")
}

/// Subject that NATS fires the schedule body to.
pub fn schedule_fire_subject(data_instance: &str, table: &str, key: &str) -> String {
    format!("{data_instance}-sched-fire.{table}.{key}")
}

/// Ensure the schedules stream exists.  Called on startup.
pub async fn ensure_schedule_stream(js: &JetStream, data_instance: &str) -> Result<(), Error> {
    let config = StreamConfig {
        name: schedule_stream(data_instance),
        // Stream must cover both schedule slots and fire targets.
        subjects: vec![
            format!("{data_instance}-sched.>"),
            format!("{data_instance}-sched-fire.>"),
        ],
        allow_msg_schedules: true,
        allow_msg_ttl: true,
        allow_rollup_hdrs: true,
        allow_direct: true,
        ..Default::default()
    };
    js.create_stream(&config).await?;
    Ok(())
}

/// Publish a one-shot delayed write schedule.
///
/// The NATS server will deliver the body to the fire subject when `at`
/// (RFC 3339 UTC) arrives; storage-service picks it up and performs the KV put.
pub async fn publish_schedule_at(
    js: &JetStream,
    data_instance: &str,
    table: &str,
    key: &str,
    body: &[u8],
    at: &str,
) -> Result<(), Error> {
    let spec = ScheduleSpec {
        schedule: Some(Schedule::At(at.to_string())),
        target: schedule_fire_subject(data_instance, table, key),
        ..Default::default()
    };
    let slot = schedule_slot(data_instance, table, key);
    js.publish_with_headers(&slot, &spec.to_headers(), body)
        .await?;
    Ok(())
}
