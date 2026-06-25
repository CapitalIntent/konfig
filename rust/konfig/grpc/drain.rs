//! Graceful-drain state: the post-SIGTERM RPC-rejection timeout and the per-handler
//! drain check that returns UNAVAILABLE so clients reconnect to a healthy pod.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tonic::Status;

/// Maximum time we wait for in-flight RPCs to complete after SIGTERM before
/// forcing the gRPC server to stop accepting connections.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Helper used at the top of each RPC handler — returns an `Err(Status::unavailable)`
/// when the server is draining so the client reconnects to a healthy pod.
pub(crate) fn check_drain(draining: &AtomicBool) -> Result<(), Status> {
    // Acquire-only load — pairs with the Release/AcqRel writer in
    // `begin_drain`/serve.  Runs on every RPC entry, so the cheaper
    // ordering matters.
    if draining.load(Ordering::Acquire) {
        Err(Status::unavailable("server draining"))
    } else {
        Ok(())
    }
}
