FROM rust:1.83-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p heimdall-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/heimdall /usr/local/bin/heimdall

EXPOSE 2399
ENTRYPOINT ["heimdall"]
CMD ["serve", "--bind", "0.0.0.0:2399"]
