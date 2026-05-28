# syntax=docker/dockerfile:1
# Multistage build for konfig-cli.
# Stage "artifact" exports only the stripped binary — used by `--output type=local`.
FROM rust:1.95.0-slim AS builder
WORKDIR /build
RUN apt-get update -q && apt-get install -y -q protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p konfig-cli --bin konfig-cli \
 && strip target/release/konfig-cli

FROM scratch AS artifact
COPY --from=builder /build/target/release/konfig-cli /konfig-cli
