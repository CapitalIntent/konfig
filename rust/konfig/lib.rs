//! Konfig — generic K8s config distribution.
//!
//! # Modules
//!
//! - [`types`] — `ConfigSnapshot`, `ConfigSpec`, `SecretSnapshot`
//! - [`cache`] — DashMap-backed multi-key lock-free config cache
//! - [`secret_cache`] — DashMap-backed multi-key lock-free secret cache
//! - [`watcher`] — kube-rs watcher for `Config.konfig.io/v1` CRDs
//! - [`configmap_watcher`] — watcher for ConfigMaps (konfig.io/managed=true)
//! - [`secret_watcher`] — watcher for Secrets (konfig.io/managed=true)
//! - [`grpc`] — gRPC server (Protobuf, standard tonic codec)
//! - [`import`] — CLI helper: onboard existing ConfigMaps as Config CRDs

pub mod cache;
pub mod cache_key;
pub mod configmap_watcher;
pub mod grpc;
pub mod import;
pub mod metrics;
pub mod secret_cache;
pub mod secret_watcher;
pub mod sync_util;
pub mod types;
pub mod value_parse;
pub mod watcher;

// Generated protobuf types (via build.rs + tonic-build).
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/konfig.v1.rs"));
}
