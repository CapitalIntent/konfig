//! gRPC channel factory + authenticated client setup and shared apply/seed helpers.

use std::time::Duration;

use tonic::transport::Channel;
use tracing::warn;

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, GetRequest};

// ── Channel factory ───────────────────────────────────────────────────────────

pub(crate) async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
    Channel::from_shared(addr.to_owned())
        .expect("valid URI")
        .http2_keep_alive_interval(std::time::Duration::from_secs(20))
        .keep_alive_timeout(std::time::Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .connect()
        .await
}

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Resolve the seed schema_version (current + 1) for a config so applies start
/// strictly above any pre-existing state. Returns 1 when the resource is
/// missing or unreadable.
pub(crate) async fn seed_start_seq(
    addr: &str,
    namespace: &str,
    config_name: &str,
) -> Result<u32, String> {
    let ch = connect(addr)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let mut client = KonfigServiceClient::new(ch);
    let seq = match client
        .get(tonic::Request::new(GetRequest {
            namespace: namespace.to_owned(),
            name: config_name.to_owned(),
        }))
        .await
    {
        Ok(r) => r.into_inner().schema_version + 1,
        Err(_) => 1,
    };
    Ok(seq)
}

/// Drive `start_seq..=end_seq` apply RPCs at `interval_ms` cadence on a single
/// connection. Returns (ok, err) counts. Connection errors are fatal and
/// returned as `Err`.
pub(crate) async fn drive_applies(
    addr: &str,
    namespace: &str,
    config_name: &str,
    start_seq: u32,
    end_seq: u32,
    interval_ms: u64,
    scenario_label: &str,
) -> Result<(u32, u32), String> {
    let ch = connect(addr)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let mut driver = KonfigServiceClient::new(ch);
    let mut ok: u32 = 0;
    let mut err: u32 = 0;
    for seq in start_seq..=end_seq {
        let yaml = format!(
            "schema_version: {seq}\ncontent:\n  iteration: {seq}\n  scenario: {scenario_label}\n"
        );
        match driver
            .apply(tonic::Request::new(ApplyRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => ok += 1,
            Err(e) => {
                err += 1;
                warn!(seq, "{scenario_label}: Apply failed: {e}");
            }
        }
        if seq < end_seq {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
    }
    Ok((ok, err))
}
