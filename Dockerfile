
# ---- build stage ----
FROM rust:1.75 as builder
WORKDIR /app

# (optional) if you have workspace / extra crates, copy those manifests too
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

# ---- runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates tini \
  && rm -rf /var/lib/apt/lists/*

# Non-root
RUN useradd -r -u 10001 -g root mini && mkdir -p /etc/mini-fintickstreams \
  && chown -R 10001:0 /etc/mini-fintickstreams

COPY --from=builder /app/target/release/mini-fintickstreams /usr/local/bin/mini-fintickstreams

ENV RUST_BACKTRACE=1
EXPOSE 8080 8000

USER 10001

ENTRYPOINT ["/usr/bin/tini","--","/usr/local/bin/mini-fintickstreams"]

