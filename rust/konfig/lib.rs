//! Konfig V0 — K8s ConfigMap watcher with FlatBuffers-encoded trading config.
//!
//! # Architecture
//!
//! - [`types`] — owned Rust snapshot structs (no FlatBuffers borrows)
//! - [`cache`] — `ArcSwap`-backed lock-free cache for the current config
//! - [`codec`] — FlatBuffers tonic codec (promoted from Phase 0 spike)
//! - [`watcher`] — kube-rs ConfigMap watcher that drives cache updates
//! - [`grpc`] — gRPC server (Phase 2A): Read / ReadAll / Apply handlers

pub mod cache;
pub mod codec;
pub mod grpc;
pub mod types;
pub mod watcher;
