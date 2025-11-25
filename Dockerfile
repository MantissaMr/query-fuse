# --- Stage 1: Builder ---
FROM rust:latest AS builder

WORKDIR /usr/src/query-fuse
COPY . .

RUN cargo build --release

# --- Stage 2: Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/query-fuse/target/release/query-fuse /usr/local/bin/query-fuse

RUN mkdir -p /data
WORKDIR /data

ENTRYPOINT ["query-fuse"]