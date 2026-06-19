//! Owned snapshot of a single `Config` as delivered over the konfig gRPC
//! `Subscribe` stream.
//!
//! Lives behind `ArcSwap<ConfigSnapshot>` inside each [`crate::ConfigHandle`]
//! so reads are lock-free. `content` is `serde_json::Value` (parsed once from
//! the proto `content_json`) so callers can do
//! `snap.content["risk"]["max"].as_u64()` without re-parsing on every access.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tracing::warn;

use crate::proto::Config as ProtoConfig;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid content_json: {0}")]
    InvalidContent(#[from] serde_json::Error),
}

fn default_content() -> Value {
    Value::Object(serde_json::Map::new())
}

/// Logical spec carried by a Config: the monotonic schema version plus the
/// arbitrary JSON content the consumer defines.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigSpec {
    pub schema_version: u32,
    #[serde(default = "default_content")]
    pub content: Value,
}

/// Immutable, lock-free-readable view of one Config at a point in time.
///
/// Published into a per-name `ArcSwap` by the stream driver task; read via
/// [`crate::ConfigHandle::get`] (an `ArcSwap::load_full`).
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub content: Value,
    pub schema_version: u32,
    pub resource_version: String,
    pub loaded_at: Instant,
    /// Set when the stream driver loses its connection to the konfig service;
    /// cleared on the next successfully-received event. Mirrors the server's
    /// `Config.stale_since_ms` semantics.
    pub stale_since: Option<Instant>,
}

impl Default for ConfigSnapshot {
    fn default() -> Self {
        Self {
            content: Value::Null,
            schema_version: 0,
            resource_version: String::new(),
            loaded_at: Instant::now(),
            stale_since: None,
        }
    }
}

impl ConfigSnapshot {
    /// Build a snapshot for tests / fallbacks from a literal JSON value.
    /// `schema_version` defaults to 0; `resource_version` is empty.
    pub fn compiled_in(content: Value) -> Self {
        Self {
            content,
            schema_version: 0,
            resource_version: String::new(),
            loaded_at: Instant::now(),
            stale_since: None,
        }
    }

    pub fn from_spec(spec: ConfigSpec, resource_version: String) -> Self {
        Self {
            content: spec.content,
            schema_version: spec.schema_version,
            resource_version,
            loaded_at: Instant::now(),
            stale_since: None,
        }
    }

    pub fn with_stale_since(mut self, when: Instant) -> Self {
        self.stale_since = Some(when);
        self
    }
}

/// Parse a proto [`ProtoConfig`] (as delivered in a `ConfigEvent`) into a
/// [`ConfigSnapshot`].
///
/// Returns `Err` if `content_json` is non-empty but fails to parse as JSON —
/// the stream driver retains the previous snapshot in that case (CP
/// semantics). An empty `content_json` parses to an empty JSON object.
pub fn snapshot_from_proto(cfg: &ProtoConfig) -> Result<ConfigSnapshot, ParseError> {
    let content = if cfg.content_json.is_empty() {
        default_content()
    } else {
        match serde_json::from_str(&cfg.content_json) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    namespace = %cfg.namespace,
                    name = %cfg.name,
                    "konfig-consumer: failed to parse content_json: {e}"
                );
                return Err(ParseError::InvalidContent(e));
            }
        }
    };

    Ok(ConfigSnapshot {
        content,
        schema_version: cfg.schema_version,
        resource_version: cfg.resource_version.clone(),
        loaded_at: Instant::now(),
        stale_since: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn proto(name: &str, schema_version: u32, content_json: &str) -> ProtoConfig {
        ProtoConfig {
            namespace: "default".to_string(),
            name: name.to_string(),
            schema_version,
            content_json: content_json.to_string(),
            resource_version: "rv-7".to_string(),
            age_ms: 0,
            stale_since_ms: -1,
        }
    }

    #[test]
    fn parse_valid_proto() {
        let cfg = proto("risk-config", 5, r#"{"risk": {"max": 100}}"#);
        let snap = snapshot_from_proto(&cfg).expect("parses");
        assert_eq!(snap.schema_version, 5);
        assert_eq!(snap.content["risk"]["max"], 100);
        assert_eq!(snap.resource_version, "rv-7");
        assert!(snap.stale_since.is_none());
    }

    #[test]
    fn empty_content_json_yields_empty_object() {
        let cfg = proto("x", 2, "");
        let snap = snapshot_from_proto(&cfg).expect("parses");
        assert_eq!(snap.schema_version, 2);
        assert!(snap.content.is_object());
    }

    #[test]
    fn invalid_content_json_returns_err() {
        let cfg = proto("x", 1, "not-json");
        assert!(snapshot_from_proto(&cfg).is_err());
    }

    #[test]
    fn compiled_in_constructs_from_value() {
        let snap = ConfigSnapshot::compiled_in(json!({"k": 1}));
        assert_eq!(snap.content["k"], 1);
        assert_eq!(snap.schema_version, 0);
        assert!(snap.stale_since.is_none());
    }

    #[test]
    fn from_spec_carries_fields() {
        let spec = ConfigSpec {
            schema_version: 9,
            content: json!({"a": true}),
        };
        let snap = ConfigSnapshot::from_spec(spec, "rv-9".to_string());
        assert_eq!(snap.schema_version, 9);
        assert_eq!(snap.content["a"], true);
        assert_eq!(snap.resource_version, "rv-9");
    }
}
