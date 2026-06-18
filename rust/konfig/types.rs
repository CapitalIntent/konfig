//! Generic snapshot types for konfig.
//!
//! `ConfigSpec` — deserialized from the CRD spec field.
//! `ConfigSnapshot` — owned cache entry; no borrows, Send + 'static.

use std::sync::{Arc, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// ── ConfigSpec — matches CRD spec ────────────────────────────────────────────

/// The `spec` field of a `Config.konfig.io/v1` CRD.
///
/// `schema_version` is required; `content` is an arbitrary JSON object.
/// Apply YAML must deserialize into this type.
fn default_content() -> Value {
    Value::Object(serde_json::Map::new())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigSpec {
    pub schema_version: u32,
    #[serde(default = "default_content")]
    pub content: Value,
}

// ── ConfigSnapshot — cache entry ─────────────────────────────────────────────

/// Owned snapshot stored in `ConfigCache`.
///
/// All fields are `Clone` + `Send` + `'static` — safe for `ArcSwap`.
///
/// `content_json_cache` memoises the result of serialising `content` once
/// per snapshot.  Without the cache, every `grpc::snapshot_to_proto` call
/// re-serialised the JSON payload, which becomes per-event work on the
/// `Subscribe` hot path.  The cache is wrapped in `Arc<OnceLock<_>>` so
/// `Clone` (and `Arc<ConfigSnapshot>` fan-out) share the same memoised
/// string across all clones.
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub name: String,
    pub namespace: String,
    pub schema_version: u32,
    /// Arbitrary config payload — JSON object, array, or primitive.
    pub content: Value,
    pub resource_version: String,
    pub loaded_at: Instant,
    /// Set when the watcher loses its K8s connection; cleared on reconnect.
    /// All snapshots in the cache share the same stale instant (set by
    /// `ConfigCache::mark_all_stale` when the watcher disconnects).
    pub stale_since: Option<Instant>,
    /// Memoised JSON encoding of `content`.  Reset on every `Clone` would
    /// be wasteful, so we put it behind `Arc<OnceLock<…>>`: clones share
    /// the same cell and the first reader wins.  Public to keep struct-
    /// literal construction simple at call-sites; do not write to it
    /// directly — go through [`Self::content_json`] or replace the whole
    /// snapshot.  `OnceLock`'s single-write semantics make stale-cache
    /// bugs impossible in practice.
    pub content_json_cache: Arc<OnceLock<String>>,
}

impl Default for ConfigSnapshot {
    fn default() -> Self {
        Self {
            name: String::new(),
            namespace: String::new(),
            schema_version: 0,
            content: Value::Null,
            resource_version: String::new(),
            loaded_at: Instant::now(),
            stale_since: None,
            content_json_cache: Arc::new(OnceLock::new()),
        }
    }
}

impl ConfigSnapshot {
    pub fn from_spec(
        name: String,
        namespace: String,
        spec: ConfigSpec,
        resource_version: String,
    ) -> Self {
        Self {
            name,
            namespace,
            schema_version: spec.schema_version,
            content: spec.content,
            resource_version,
            loaded_at: Instant::now(),
            stale_since: None,
            content_json_cache: Arc::new(OnceLock::new()),
        }
    }

    /// Serialise `content` once per snapshot and return the cached string.
    ///
    /// Returns `&str` to encourage callers to clone at the proto boundary
    /// instead of re-serialising.  Callers that need an owned `String`
    /// (gRPC proto fields are `String`) do a single `.to_owned()` — the
    /// per-RPC serialisation work disappears.
    pub fn content_json(&self) -> &str {
        self.content_json_cache
            .get_or_init(|| serde_json::to_string(&self.content).unwrap_or_default())
    }
}

// ── SecretSnapshot ────────────────────────────────────────────────────────────

/// Snapshot of a managed K8s Secret (label: konfig.io/managed=true).
///
/// `data_json_cache` memoises the serialised `data_json` payload that
/// `grpc::secret_get::secret_snapshot_to_proto` emits.  Without the cache,
/// every `SubscribeSecrets` event re-built a transient `HashMap<&str, &str>`
/// and re-ran `serde_json::to_string` — per-event work on the hot path.
/// Mirrors `ConfigSnapshot::content_json_cache`.
#[derive(Debug, Clone)]
pub struct SecretSnapshot {
    pub name: String,
    pub namespace: String,
    pub schema_version: u32,
    /// Base64-encoded byte values, keyed by Secret data key.
    /// Values are NOT decoded server-side.
    pub data: std::collections::HashMap<String, bytes::Bytes>,
    pub resource_version: String,
    pub loaded_at: std::time::Instant,
    /// `Some(t)` once the secret watcher loses its K8s connection (set by
    /// `SecretCache::mark_all_stale`); `None` while fresh.  Surfaced as
    /// `SecretResponse.stale_since_ms`.  Mirrors `ConfigSnapshot::stale_since`.
    pub stale_since: Option<Instant>,
    /// Memoised JSON encoding of `data` (`{"key": "<base64>"}`).  Shared
    /// across clones via `Arc<OnceLock<…>>` — see `ConfigSnapshot::
    /// content_json_cache` for the same pattern.  Populate via
    /// [`Self::data_json`]; do not write to it directly.
    pub data_json_cache: Arc<OnceLock<String>>,
}

impl Default for SecretSnapshot {
    fn default() -> Self {
        Self {
            name: String::new(),
            namespace: String::new(),
            schema_version: 0,
            data: std::collections::HashMap::new(),
            resource_version: String::new(),
            loaded_at: std::time::Instant::now(),
            stale_since: None,
            data_json_cache: Arc::new(OnceLock::new()),
        }
    }
}

impl SecretSnapshot {
    /// Serialise `data` once per snapshot and return the cached JSON string.
    ///
    /// Values are already base64-encoded ASCII bytes — `from_utf8` is
    /// effectively a no-op pointer cast.  Returning `&str` lets callers
    /// clone at the proto boundary instead of re-serialising per event.
    pub fn data_json(&self) -> &str {
        self.data_json_cache.get_or_init(|| {
            let data_map: std::collections::HashMap<&str, &str> = self
                .data
                .iter()
                .map(|(k, v)| {
                    // kube secret data is base64 on the wire — `parse_secret`
                    // re-encodes to base64 into `Bytes`, so every byte here
                    // is ASCII (a strict UTF-8 subset). If `from_utf8` ever
                    // fails the upstream bytes were tampered with somehow;
                    // surface "" + a `warn!` rather than silently shipping
                    // garbled JSON.
                    match std::str::from_utf8(v) {
                        Ok(s) => (k.as_str(), s),
                        Err(e) => {
                            tracing::warn!(
                                key = %k,
                                err = %e,
                                "secret value is not valid UTF-8 — emitting empty string",
                            );
                            (k.as_str(), "")
                        }
                    }
                })
                .collect();
            serde_json::to_string(&data_map).unwrap_or_default()
        })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn config_spec_deserializes_from_yaml() {
        let yaml = "schema_version: 3\ncontent:\n  key: value\n  count: 42\n";
        let spec: ConfigSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.schema_version, 3);
        assert_eq!(spec.content["key"], "value");
        assert_eq!(spec.content["count"], 42);
    }

    #[test]
    fn config_spec_missing_content_defaults_to_empty_object() {
        let yaml = "schema_version: 1\n";
        let spec: ConfigSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.schema_version, 1);
        // Defaults to {} so K8s CRD schema (type: object) accepts it.
        assert!(spec.content.is_object());
        assert_eq!(spec.content.as_object().unwrap().len(), 0);
    }

    #[test]
    fn snapshot_content_json_round_trips() {
        let snap = ConfigSnapshot {
            schema_version: 2,
            content: json!({"a": 1, "b": [1, 2, 3]}),
            ..Default::default()
        };
        let json_str = snap.content_json();
        let reparsed: Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(reparsed["a"], 1);
    }

    /// Repeated `content_json()` calls must return the same memoised string,
    /// not a freshly serialised one. Checking pointer identity proves the
    /// `OnceLock` cache is wired up correctly.
    #[test]
    fn content_json_is_memoised() {
        let snap = ConfigSnapshot {
            schema_version: 1,
            content: json!({"k": "v"}),
            ..Default::default()
        };
        let p1 = snap.content_json().as_ptr();
        let p2 = snap.content_json().as_ptr();
        assert_eq!(
            p1, p2,
            "content_json must return cached string on second call"
        );
    }

    /// Clones of a snapshot must share the same memoised JSON cache —
    /// `Arc<OnceLock<…>>` lets one clone's `content_json()` populate the
    /// cache and a different clone see the result without re-serialising.
    #[test]
    fn content_json_cache_is_shared_across_clones() {
        let snap_a = ConfigSnapshot {
            schema_version: 1,
            content: json!({"k": "v"}),
            ..Default::default()
        };
        let snap_b = snap_a.clone();
        let p_a = snap_a.content_json().as_ptr();
        let p_b = snap_b.content_json().as_ptr();
        assert_eq!(p_a, p_b, "clones must share the same OnceLock cell");
    }

    /// Concurrent `content_json()` calls on clones of the same snapshot must
    /// resolve to the SAME backing string — `OnceLock::get_or_init` must
    /// dedupe the serialisation across racing threads, not let two threads
    /// each materialise their own copy. Proving this with pointer-equality
    /// closes the gap the single-threaded `_is_memoised` test left open:
    /// without `OnceLock` we'd still observe identical *bytes* but distinct
    /// allocations, which would silently break the
    /// "serialise once per snapshot" invariant the `serve()` hot path relies on.
    #[test]
    fn content_json_cache_dedupes_under_concurrent_init() {
        use std::sync::Arc as StdArc;
        use std::sync::Barrier;
        use std::thread;

        // Run many trials to expose the race window — a single trial can
        // accidentally serialise (init thread wins by a wide margin) and miss
        // a regression where the `OnceLock` is replaced by something that
        // doesn't synchronise.
        for _ in 0..32 {
            let snap = ConfigSnapshot {
                schema_version: 1,
                content: json!({"k": "v", "n": 42, "arr": [1, 2, 3]}),
                ..Default::default()
            };
            let snap_a = snap.clone();
            let snap_b = snap.clone();
            let barrier = StdArc::new(Barrier::new(2));
            let b_a = StdArc::clone(&barrier);
            let b_b = StdArc::clone(&barrier);

            let h_a = thread::spawn(move || {
                b_a.wait();
                snap_a.content_json().as_ptr() as usize
            });
            let h_b = thread::spawn(move || {
                b_b.wait();
                snap_b.content_json().as_ptr() as usize
            });

            let p_a = h_a.join().expect("thread A panicked");
            let p_b = h_b.join().expect("thread B panicked");
            assert_eq!(
                p_a, p_b,
                "concurrent content_json() must return the same Arc-backed cache pointer \
                 (OnceLock must dedupe the serialisation work)",
            );
        }
    }
}
