//! Konfig — generic K8s config distribution.
//!
//! # Modules
//!
//! - [`types`] — `ConfigSnapshot` and `ConfigSpec` (no domain-specific types)
//! - [`cache`] — `ArcSwap`-backed lock-free snapshot cache
//! - [`watcher`] — kube-rs watcher for `Config.konfig.io/v1` CRDs
//! - [`grpc`] — gRPC server (Protobuf, standard tonic codec)
//! - [`import`] — CLI helper: onboard existing ConfigMaps as Config CRDs

pub mod cache;
pub mod grpc;
pub mod import;
pub mod types;
pub mod watcher;

// Generated protobuf types (via build.rs + tonic-build).
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/konfig.v1.rs"));
}
