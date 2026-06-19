//! Generates the konfig gRPC **client** bindings into `OUT_DIR`.
//!
//! Client-only (`build_server(false)`): the consumer crate talks to the konfig
//! service via the `Subscribe` RPC; it never serves the service. Mirrors
//! `rust/konfig/build.rs` but drops the server side so no `tonic` server
//! codegen is pulled in.
//!
//! The proto source is resolved relative to `CARGO_MANIFEST_DIR`
//! (`<workspace>/rust/konfig-consumer`), so `../../proto/...` points at the
//! repo-root `proto/` tree both under Cargo and under the Bazel
//! `cargo_build_script` sandbox (where the proto is staged at the same
//! relative path via the `data` attribute).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &["../../proto/konfig/v1/konfig_service.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
