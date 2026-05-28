# syntax=docker/dockerfile:1
FROM rust:1.95.0-slim AS builder
WORKDIR /build
RUN apt-get update -q && apt-get install -y -q protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p konfig --bin konfig

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /build/target/release/konfig /konfig
EXPOSE 50051 9090
ENTRYPOINT ["/konfig"]
