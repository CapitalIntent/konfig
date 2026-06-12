//! K8s watcher for `Config.konfig.io/v1` CRDs.
//!
//! Streams events via kube-rs and updates `ConfigCache` on each Apply/InitApply.
//! Delete events log a warning and retain the last-known-good value (CP semantics).

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use kube::api::ApiResource;
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::metrics::LastEventAt;
use crate::types::{ConfigSnapshot, ConfigSpec};

// ── Constants ─────────────────────────────────────────────────────────────────

pub const GROUP: &str = "konfig.io";
pub const VERSION: &str = "v1";
pub const KIND: &str = "Config";
pub const PLURAL: &str = "configs";

/// Reconnect backoff schedule in seconds: 1, 2, 4, 8, 16, 30, 30, ...
/// Used by the watcher loop; exported for unit tests.
pub const BACKOFF_STEPS_SECS: &[u64] = &[1, 2, 4, 8, 16, 30];

// Compile-time guarantee that the `.last().unwrap()` below cannot trip.
// If anyone ever edits `BACKOFF_STEPS_SECS` to be empty, the build fails
// here instead of at runtime.
const _BACKOFF_STEPS_NON_EMPTY: () = assert!(
    !BACKOFF_STEPS_SECS.is_empty(),
    "BACKOFF_STEPS_SECS must contain at least one entry",
);

/// Compute the next reconnect delay given the attempt index (0-based).
/// Caps at the last element in `BACKOFF_STEPS_SECS` — guaranteed non-empty
/// by `_BACKOFF_STEPS_NON_EMPTY` above.
pub fn backoff_delay(attempt: usize) -> std::time::Duration {
    let secs = BACKOFF_STEPS_SECS
        .get(attempt)
        .copied()
        .unwrap_or(*BACKOFF_STEPS_SECS.last().unwrap());
    std::time::Duration::from_secs(secs)
}

/// Run `f` in an infinite reconnect loop. Each invocation runs to completion;
/// any return (Ok = clean stream end, Err = stream error) is logged with the
/// supplied `label` + `namespace` and followed by a `backoff_delay(attempt)`
/// sleep before the next call.
///
/// `on_disconnect` runs after each return BEFORE the sleep — used by callers
/// that need to side-effect on disconnect (e.g. mark cache stale).
///
/// This function never returns. Callers spawn it on its own task; any panic
/// inside `f` will tear down only that task, not the binary.
pub async fn run_with_reconnect<F, Fut, E, D>(
    label: &'static str,
    namespace: String,
    mut on_disconnect: D,
    mut f: F,
) -> !
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
    E: std::fmt::Display,
    D: FnMut(),
{
    let mut attempt: usize = 0;
    loop {
        match f(attempt).await {
            Ok(()) => warn!(
                label,
                namespace = %namespace,
                attempt,
                "watcher stream ended cleanly — reconnecting",
            ),
            Err(e) => warn!(
                label,
                namespace = %namespace,
                attempt,
                "watcher error: {e} — reconnecting",
            ),
        }
        on_disconnect();
        tokio::time::sleep(backoff_delay(attempt)).await;
        attempt = attempt.saturating_add(1);
    }
}

pub fn config_api_resource() -> ApiResource {
    ApiResource {
        group: GROUP.to_string(),
        version: VERSION.to_string(),
        api_version: format!("{GROUP}/{VERSION}"),
        kind: KIND.to_string(),
        plural: PLURAL.to_string(),
    }
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum WatcherError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("watcher error: {0}")]
    Watcher(#[from] kube_watcher::Error),
}

// ── Watcher ───────────────────────────────────────────────────────────────────

pub struct Watcher {
    client: Client,
}

impl Watcher {
    pub fn new(client: Client) -> Self {
        Watcher { client }
    }

    /// Run the watcher with exponential-backoff reconnect.
    ///
    /// On stream error: marks cache stale, waits `backoff_delay(attempt)`, retries.
    /// On clean stream end: returns Ok(()).
    ///
    /// `last_event_at` is touched on every successfully-received event so the
    /// `konfig_stale_seconds` gauge sampler in `grpc::serve` can observe the
    /// freshness of this watcher.  Cold start (no event yet) leaves it `None`,
    /// which the sampler interprets as "fresh" (gauge stays at 0).
    pub async fn run(
        self,
        cache: Arc<ConfigCache>,
        namespace: String,
        config_name: String,
        last_event_at: Arc<LastEventAt>,
    ) -> Result<(), WatcherError> {
        let ar = config_api_resource();
        let mut attempt: usize = 0;

        loop {
            let api: Api<DynamicObject> =
                Api::namespaced_with(self.client.clone(), &namespace, &ar);
            let wc =
                kube_watcher::Config::default().fields(&format!("metadata.name={config_name}"));
            let mut stream = kube_watch_stream(api, wc).boxed();

            info!(
                namespace = %namespace,
                name = %config_name,
                attempt,
                "Config watcher started"
            );

            loop {
                match stream.try_next().await {
                    Ok(Some(event)) => {
                        // Touch BEFORE handle_event so the freshness signal is
                        // updated even for events that fail to parse downstream.
                        last_event_at.touch();
                        handle_event(event, &cache);
                        attempt = 0;
                    }
                    Ok(None) => {
                        info!("Config watcher stream ended cleanly");
                        return Ok(());
                    }
                    Err(e) => {
                        warn!(attempt, "Config watcher error: {e} — marking cache stale");
                        cache.mark_all_stale();
                        let delay = backoff_delay(attempt);
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        break;
                    }
                }
            }
        }
    }
}

pub(crate) fn handle_event(event: Event<DynamicObject>, cache: &Arc<ConfigCache>) {
    match event {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let name = obj.metadata.name.as_deref().unwrap_or("<unknown>");
            if let Some(snap) = parse_config_object(&obj) {
                info!(name = %name, schema_version = snap.schema_version, "Config applied — cache updated");
                cache.update(snap);
            } else {
                warn!(name = %name, "Config object could not be parsed — cache unchanged");
            }
        }
        Event::Delete(obj) => {
            let name = obj.metadata.name.as_deref().unwrap_or("<unknown>");
            warn!(name = %name, "Config deleted — cache retains last-known-good");
        }
        Event::Init => debug!("Watch stream: initial list phase"),
        Event::InitDone => debug!("Watch stream: initial list complete"),
    }
}

/// Parse a `DynamicObject` (Config CRD) into a `ConfigSnapshot`.
///
/// Expects `obj.data["spec"]` to deserialize as `ConfigSpec`.
pub fn parse_config_object(obj: &DynamicObject) -> Option<ConfigSnapshot> {
    let resource_version = obj.metadata.resource_version.clone().unwrap_or_default();
    let name = obj.metadata.name.clone().unwrap_or_default();
    let namespace = obj.metadata.namespace.clone().unwrap_or_default();

    let spec_value = obj.data.get("spec")?;
    let spec: ConfigSpec = serde_json::from_value(spec_value.clone())
        .map_err(|e| warn!(name = %name, "Failed to parse Config spec: {e}"))
        .ok()?;

    Some(ConfigSnapshot::from_spec(
        name,
        namespace,
        spec,
        resource_version,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_obj(name: &str, schema_version: u32, content: serde_json::Value) -> DynamicObject {
        let mut obj = DynamicObject::new(name, &config_api_resource());
        obj.metadata.name = Some(name.to_string());
        obj.metadata.namespace = Some("default".to_string());
        obj.metadata.resource_version = Some("rv-001".to_string());
        obj.data = json!({
            "spec": {
                "schema_version": schema_version,
                "content": content,
            }
        });
        obj
    }

    #[test]
    fn parse_valid_object() {
        let obj = make_obj("my-config", 5, json!({"key": "value"}));
        let snap = parse_config_object(&obj).expect("must parse");
        assert_eq!(snap.name, "my-config");
        assert_eq!(snap.namespace, "default");
        assert_eq!(snap.schema_version, 5);
        assert_eq!(snap.content["key"], "value");
        assert_eq!(snap.resource_version, "rv-001");
    }

    #[test]
    fn parse_missing_spec_returns_none() {
        let mut obj = DynamicObject::new("x", &config_api_resource());
        obj.data = json!({});
        assert!(parse_config_object(&obj).is_none());
    }

    #[test]
    fn parse_missing_content_defaults_to_null() {
        let obj = make_obj("cfg", 1, serde_json::Value::Null);
        let snap = parse_config_object(&obj).unwrap();
        assert!(snap.content.is_null());
    }

    #[test]
    fn apply_event_updates_cache() {
        let obj = make_obj("cfg", 7, json!({"x": 1}));
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        handle_event(Event::Apply(obj), &cache);
        assert_eq!(cache.load().schema_version, 7);
    }

    #[test]
    fn delete_event_leaves_cache_unchanged() {
        let obj = make_obj("cfg", 3, json!({}));
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        handle_event(Event::Apply(obj.clone()), &cache);
        assert_eq!(cache.load().schema_version, 3);
        handle_event(Event::Delete(obj), &cache);
        assert_eq!(cache.load().schema_version, 3);
    }

    #[test]
    fn backoff_delay_schedule() {
        let expected = &[1u64, 2, 4, 8, 16, 30, 30, 30];
        for (attempt, &want_secs) in expected.iter().enumerate() {
            let got = backoff_delay(attempt);
            assert_eq!(
                got,
                std::time::Duration::from_secs(want_secs),
                "attempt {attempt}: expected {want_secs}s got {got:?}"
            );
        }
    }

    /// `run_with_reconnect` must keep restarting the inner future after both
    /// clean-end (Ok) and error returns, and must call `on_disconnect` after
    /// each.  This is the safety net that prevents a panicked watcher from
    /// crashing the binary (see `main.rs` spawn site and the original
    /// `.expect("watcher exited with error")` regression).
    #[tokio::test(start_paused = true)]
    async fn run_with_reconnect_loops_on_clean_end_and_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let disconnects = Arc::new(AtomicUsize::new(0));

        let calls_inner = Arc::clone(&calls);
        let disconnects_inner = Arc::clone(&disconnects);

        // Drive the helper on a spawned task — it never returns, so we abort
        // after observing the desired number of restarts.
        let handle = tokio::spawn(async move {
            run_with_reconnect(
                "test",
                "ns".to_string(),
                move || {
                    disconnects_inner.fetch_add(1, Ordering::SeqCst);
                },
                move |attempt| {
                    let calls = Arc::clone(&calls_inner);
                    async move {
                        let n = calls.fetch_add(1, Ordering::SeqCst);
                        // Attempt 0: Err.  Attempt 1: Ok (clean end).
                        // Attempt 2+: Ok forever (we abort first).
                        if attempt == 0 {
                            Err::<(), &'static str>("simulated stream error")
                        } else {
                            let _ = n;
                            Ok(())
                        }
                    }
                },
            )
            .await
        });

        // Auto-advance virtual time past two backoff sleeps (1s + 2s).
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        handle.abort();

        // Must have run at least 3 times (Err, Ok, Ok…) and disconnected
        // after each return.
        assert!(
            calls.load(Ordering::SeqCst) >= 3,
            "expected ≥3 invocations, got {}",
            calls.load(Ordering::SeqCst)
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            disconnects.load(Ordering::SeqCst),
            "on_disconnect must run once per return",
        );
    }
}
