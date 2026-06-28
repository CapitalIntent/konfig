//! Konfig — generic K8s config distribution.
//!
//! # Modules
//!
//! - [`types`] — `ConfigSnapshot`, `ConfigSpec`, `SecretSnapshot`
//! - [`acl`] — cluster-scoped `ConfigACL.konfig.io/v1` table + watcher (authz)
//! - [`quota`] — cluster-scoped `TenantQuota.konfig.io/v1` table + watcher (per-tenant budgets)
//! - [`cache`] — DashMap-backed multi-key lock-free config cache
//! - [`secret_cache`] — DashMap-backed multi-key lock-free secret cache
//! - [`watcher`] — kube-rs watcher for `Config.konfig.io/v1` CRDs
//! - [`configmap_watcher`] — watcher for ConfigMaps (konfig.io/managed=true)
//! - [`secret_watcher`] — watcher for Secrets (konfig.io/managed=true)
//! - [`schema`] — per-(namespace, configName) draft-07 JSON Schema registry + watcher
//! - [`grpc`] — gRPC server (Protobuf, standard tonic codec)
//! - [`import`] — CLI helper: onboard existing ConfigMaps as Config CRDs
//! - [`telemetry`] — optional OTLP/gRPC trace export (tracing-opentelemetry bridge)

pub mod acl;
pub mod cache;
pub mod cache_key;
pub mod configmap_watcher;
pub mod cow_cache;
pub mod grpc;
pub mod import;
pub mod metrics;
pub mod quota;
pub mod schema;
pub mod secret_cache;
pub mod secret_watcher;
pub mod startup;
#[cfg(feature = "snmalloc_profiling")]
pub mod stream_sink;
pub mod sync_util;
pub mod telemetry;
pub mod tenant_cache;
pub mod types;
pub mod value_parse;
pub mod watcher;

// Generated protobuf types (via build.rs + tonic-build).
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/konfig.v1.rs"));
}
