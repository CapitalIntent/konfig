//! Thin async gRPC **client** for the konfig service.
//!
//! This crate talks to the konfig service over the gRPC `Subscribe` RPC and
//! maintains a lock-free, in-process view of one or more `Config`s. It does
//! **not** watch the Kubernetes CRD directly: konfig the service owns the
//! kube watch + cache + fan-out, so this client needs **no `kube::Client`, no
//! RBAC, no ServiceAccount token, and no kube/k8s-openapi dependencies**.
//!
//! A single [`KonfigClient`] is multiplexed: one client opens one `Subscribe`
//! stream per [`KonfigClient::watch`] call and demultiplexes events to a
//! per-name [`ConfigHandle`]. Reads via [`ConfigHandle::get`] are
//! `ArcSwap::load_full` — an atomic pointer load, no locks.
//!
//! # Runtime model
//!
//! The client **owns no tokio runtime**. [`KonfigClient::connect`] builds its
//! transport on the caller's runtime, and [`KonfigClient::watch`] spawns the
//! stream-driver task with `tokio::spawn` on the **ambient caller runtime**.
//! Dropping all [`ConfigHandle`]s for a subscription aborts that driver task.
//!
//! # Consumption contract (READ THIS)
//!
//! Consume `konfig-consumer` as a **Cargo / crate_universe dependency in YOUR
//! workspace** so it compiles against **your** `tonic` + `tokio` (one shared
//! instance). Do **NOT** pull konfig in as a separate `bazel_dep` module for
//! the client path — that builds a second `tonic`/`tokio` instance, and the
//! stream driver's `tokio::spawn` then panics with `there is no reactor
//! running`. You provide the endpoint (plus an optional mTLS
//! [`tonic::transport::ClientTlsConfig`]) and drive everything on your own
//! runtime; the client owns no runtime and needs no kube access / RBAC.
//!
//! ```no_run
//! use konfig_consumer::KonfigClient;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! // Built + spawned on the caller's runtime; no runtime is created here.
//! let client = KonfigClient::connect("http://konfig.svc:50051").await?;
//!
//! // One stream, two configs (multiplexed).
//! let handles = client.watch("default", &["risk-config", "limits"]).await?;
//! let risk = &handles[0];
//!
//! // Lock-free read; returns the default snapshot until the first event.
//! let snap = risk.get();
//! let _max = snap.content["risk"]["max_order_size_usd"].as_u64();
//! # Ok(()) }
//! ```

pub mod metrics;
pub mod snapshot;
pub mod stream;

/// Generated konfig gRPC client bindings (`konfig.v1`).
///
/// Client-only codegen (see `build.rs`): exposes
/// `konfig_service_client::KonfigServiceClient` plus the `Config`,
/// `ConfigEvent`, and `SubscribeRequest` message types.
pub mod proto {
    #![allow(clippy::all, clippy::pedantic, missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/konfig.v1.rs"));
}

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use thiserror::Error;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub use crate::metrics::{LastEventAt, MetricsError, register_stale_seconds, spawn_stale_sampler};
pub use crate::snapshot::{ConfigSnapshot, ConfigSpec, ParseError, snapshot_from_proto};
pub use crate::stream::{BACKOFF_STEPS_SECS, backoff_delay};

use crate::proto::konfig_service_client::KonfigServiceClient;
use crate::stream::{Store, StreamDriver};

/// Errors surfaced by [`KonfigClient`] construction.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("watch called with no config names")]
    NoNames,
}

/// A multiplexed, async gRPC client for the konfig service.
///
/// Cheaply cloneable: clones share the same underlying `tonic` `Channel`
/// (which itself multiplexes HTTP/2 streams over one connection).
#[derive(Clone)]
pub struct KonfigClient {
    client: KonfigServiceClient<Channel>,
}

impl KonfigClient {
    /// Connect to `endpoint` (e.g. `http://konfig.svc:50051`), building the
    /// transport lazily on the **caller's** runtime. The first RPC drives the
    /// actual connect, so this does not block on network IO.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let channel = Self::endpoint(endpoint)?.connect_lazy();
        Ok(Self::new(channel))
    }

    /// Connect with mTLS. The caller supplies a fully-configured
    /// [`ClientTlsConfig`] (CA roots, client identity), keeping all TLS policy
    /// on the consumer side.
    pub async fn connect_tls(
        endpoint: impl Into<String>,
        tls: ClientTlsConfig,
    ) -> Result<Self, ClientError> {
        let channel = Self::endpoint(endpoint)?
            .tls_config(tls)?
            .connect_lazy();
        Ok(Self::new(channel))
    }

    /// Build a client from a pre-constructed `tonic` [`Channel`]. Use this when
    /// you want full control over the transport (custom interceptors, pooling,
    /// load balancing, bespoke TLS).
    pub fn new(channel: Channel) -> Self {
        Self {
            client: KonfigServiceClient::new(channel),
        }
    }

    fn endpoint(endpoint: impl Into<String>) -> Result<Endpoint, ClientError> {
        let raw = endpoint.into();
        Channel::from_shared(raw.clone()).map_err(|_| ClientError::InvalidEndpoint(raw))
    }

    /// Subscribe to `names` in `namespace` over **one** `Subscribe` stream and
    /// return one [`ConfigHandle`] per name (order matches `names`).
    ///
    /// Spawns a single stream-driver task on the caller's runtime that reads
    /// `ConfigEvent`s, demuxes by name, and publishes per-name snapshots. The
    /// driver reconnects with backoff and resumes from the last observed
    /// `resource_version`. The returned handles share ownership of the driver
    /// task; it is aborted once the **last** handle for this call is dropped.
    pub async fn watch(
        &self,
        namespace: &str,
        names: &[&str],
    ) -> Result<Vec<ConfigHandle>, ClientError> {
        if names.is_empty() {
            return Err(ClientError::NoNames);
        }

        // One ArcSwap per name; the driver publishes into these, handles read.
        let mut stores: HashMap<String, Store> = HashMap::with_capacity(names.len());
        for name in names {
            stores.insert(
                (*name).to_string(),
                Arc::new(ArcSwap::from_pointee(ConfigSnapshot::default())),
            );
        }

        let last_event_at = Arc::new(LastEventAt::new());
        let driver = StreamDriver {
            client: self.client.clone(),
            namespace: namespace.to_string(),
            names: names.iter().map(|s| (*s).to_string()).collect(),
            stores: stores.clone(),
            last_event_at,
        };

        // Spawn on the ambient caller runtime — NO runtime is created here.
        let join = tokio::spawn(driver.run());
        // Shared guard: aborts the driver when the last ConfigHandle drops.
        let task = Arc::new(DriverTask(join));

        let handles = names
            .iter()
            .map(|name| ConfigHandle {
                name: (*name).to_string(),
                store: stores[*name].clone(),
                _task: task.clone(),
            })
            .collect();

        Ok(handles)
    }

    /// Single-config convenience over [`KonfigClient::watch`].
    pub async fn watch_one(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<ConfigHandle, ClientError> {
        let mut handles = self.watch(namespace, &[name]).await?;
        Ok(handles.remove(0))
    }
}

/// Owns the spawned driver `JoinHandle`; aborts it on drop. Shared (`Arc`)
/// across all handles from one [`KonfigClient::watch`] call so the task lives
/// until the last handle is dropped.
struct DriverTask(JoinHandle<()>);

impl Drop for DriverTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Lock-free read handle for a single config in a subscription.
///
/// Clone to share reads across tasks. The underlying stream-driver task stays
/// alive as long as **any** clone (across all names in the same `watch` call)
/// is held.
#[derive(Clone)]
pub struct ConfigHandle {
    name: String,
    store: Store,
    _task: Arc<DriverTask>,
}

impl ConfigHandle {
    /// The config name this handle tracks.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a cheap `Arc` clone of the current snapshot pointer
    /// (`ArcSwap::load_full`). Returns [`ConfigSnapshot::default`] until the
    /// first event for this name arrives.
    pub fn get(&self) -> Arc<ConfigSnapshot> {
        self.store.load_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Multiplex + shares-caller-runtime + no-panic acceptance test.
    ///
    /// One `KonfigClient` against an unreachable endpoint, `watch` for two
    /// names → two handles, each `.get()` returns a default snapshot without
    /// panicking. The driver retries the (failing) connect in the background;
    /// the key property is that `tokio::spawn` ran on this `#[tokio::test]`
    /// runtime (no "no reactor running" panic) and reads never block.
    #[tokio::test]
    async fn watch_multiplexes_two_configs_without_panic() {
        let client = KonfigClient::connect("http://127.0.0.1:1")
            .await
            .expect("connect_lazy builds without IO");

        let handles = client
            .watch("default", &["a", "b"])
            .await
            .expect("watch spawns driver on caller runtime");

        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].name(), "a");
        assert_eq!(handles[1].name(), "b");

        // No server in CI: each handle returns the default snapshot, no panic.
        let a = handles[0].get();
        let b = handles[1].get();
        assert!(a.content.is_null());
        assert!(b.content.is_null());
        assert_eq!(a.schema_version, 0);
    }

    #[tokio::test]
    async fn watch_one_returns_single_handle() {
        let client = KonfigClient::connect("http://127.0.0.1:1")
            .await
            .expect("builds");
        let handle = client
            .watch_one("ns", "only")
            .await
            .expect("watch_one spawns");
        assert_eq!(handle.name(), "only");
        assert!(handle.get().content.is_null());
    }

    #[tokio::test]
    async fn watch_with_no_names_errors() {
        let client = KonfigClient::connect("http://127.0.0.1:1")
            .await
            .expect("builds");
        let err = client.watch("ns", &[]).await.err().expect("no names");
        assert!(matches!(err, ClientError::NoNames));
    }

    #[tokio::test]
    async fn invalid_endpoint_errors() {
        let err = KonfigClient::connect("not a url")
            .await
            .err()
            .expect("invalid endpoint");
        assert!(matches!(err, ClientError::InvalidEndpoint(_)));
    }

    #[tokio::test]
    async fn from_channel_constructs() {
        // `new(channel)` path — caller supplies the transport.
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let client = KonfigClient::new(channel);
        let handle = client.watch_one("ns", "c").await.expect("spawns");
        assert!(handle.get().content.is_null());
    }

    /// Dropping the last handle aborts the driver task.
    #[tokio::test]
    async fn dropping_last_handle_aborts_driver() {
        let client = KonfigClient::connect("http://127.0.0.1:1")
            .await
            .expect("builds");
        let handles = client.watch("ns", &["a", "b"]).await.expect("spawns");
        // Grab an abort handle to observe completion after the guard drops.
        let abort = handles[0]._task.0.abort_handle();
        // Dropping every handle drops the last `Arc<DriverTask>`, whose
        // `Drop` aborts the spawned driver.
        drop(handles);
        // Poll the runtime until the task observes the abort (bounded loop so
        // a regression can't hang the test).
        for _ in 0..1000 {
            if abort.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(abort.is_finished(), "driver task should be aborted on drop");
    }
}
