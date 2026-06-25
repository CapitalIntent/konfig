//! Shared `#[cfg(test)]` fixtures for the split Subscribe submodules.
//!
//! The original monolithic `mod tests` shared these helpers across every
//! concern; co-locating tests per submodule (CU-86aj7k5rf) requires the
//! fixtures to live in one crate-internal place the submodule test modules
//! can import.

#![allow(unused_imports)]

pub(crate) use std::collections::{BTreeMap, VecDeque};
pub(crate) use std::sync::{Arc, Mutex};
pub(crate) use std::time::{Duration, Instant};

pub(crate) use dashmap::DashMap;
pub(crate) use kube::core::DynamicObject;
pub(crate) use kube::runtime::watcher::{self as kube_watcher, Event};
pub(crate) use serde_json::json;
pub(crate) use tokio::sync::{Notify, broadcast, mpsc};

pub(crate) use crate::cache::ConfigCache;
pub(crate) use crate::proto::{ConfigEvent, config_event::EventType};
pub(crate) use crate::types::ConfigSnapshot;

pub(crate) use super::{
    BroadcastFrame, MAX_BROADCAST_SHARDS, MIN_BROADCAST_SHARDS, REPLAY_BUFFER_SIZE, ReplayBuffer,
    ReplayEntry, ShardSet, SubscribeFilter, bridge_broadcast, filter_allows_event, gc_task,
    gc_tick, get_or_create_broadcast, pump_subscribe_namespace_events, push_replay,
    resume_from_buffer, try_send_or_disconnect,
};

pub(crate) fn make_event(rv: &str, schema_version: u32) -> ConfigEvent {
    ConfigEvent {
        event_type: EventType::Modified as i32,
        config: Some(crate::grpc::snapshot_to_proto(&ConfigSnapshot {
            name: "cfg".into(),
            namespace: "default".into(),
            schema_version,
            content: json!({}),
            resource_version: rv.into(),
            ..Default::default()
        })),
    }
}

/// Wrap a `ConfigEvent` in a fresh `BroadcastFrame` for tests that need
/// to inject events into a broadcast channel.  Empty labels — label-
/// selector filtering is exercised by the dedicated parser / filter unit
/// tests and the resume-path end-to-end test below.
pub(crate) fn make_frame(event: ConfigEvent) -> Arc<BroadcastFrame> {
    Arc::new(BroadcastFrame {
        sent_at: Instant::now(),
        event: Arc::new(event),
        labels: Arc::new(BTreeMap::new()),
    })
}

/// An allow-all filter (no `names`, empty `label_selector`) for tests that
/// drive the resume/bridge paths without exercising filtering.
pub(crate) fn allow_all_filter() -> Arc<SubscribeFilter> {
    Arc::new(SubscribeFilter::new(&[], "").expect("empty filter is valid"))
}

pub(crate) fn make_replay_buf(entries: &[(&str, u32)]) -> ReplayBuffer {
    let buf = Arc::new(Mutex::new(VecDeque::new()));
    for (rv, sv) in entries {
        push_replay(
            &buf,
            rv,
            Arc::new(make_event(rv, *sv)),
            Arc::new(BTreeMap::new()),
        );
    }
    buf
}

pub(crate) fn make_cache(namespace: &str, entries: &[(&str, &str, u32)]) -> Arc<ConfigCache> {
    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    for (name, rv, sv) in entries {
        cache.update(ConfigSnapshot {
            name: name.to_string(),
            namespace: namespace.to_string(),
            schema_version: *sv,
            content: json!({}),
            resource_version: rv.to_string(),
            ..Default::default()
        });
    }
    cache
}

pub(crate) fn dyn_config(
    name: &str,
    namespace: &str,
    schema_version: u32,
    rv: &str,
) -> DynamicObject {
    let mut obj = DynamicObject::new(name, &crate::watcher::config_api_resource());
    obj.metadata.name = Some(name.to_string());
    obj.metadata.namespace = Some(namespace.to_string());
    obj.metadata.resource_version = Some(rv.to_string());
    obj.data = json!({
        "spec": {
            "schema_version": schema_version,
            "content": {"k": rv},
        }
    });
    obj
}

pub(crate) fn ns_watcher_err() -> kube_watcher::Error {
    kube_watcher::Error::WatchFailed(kube::Error::Api(kube::core::ErrorResponse {
        status: "Failure".to_string(),
        message: "synthetic".to_string(),
        reason: "synthetic".to_string(),
        code: 500,
    }))
}

pub(crate) fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
