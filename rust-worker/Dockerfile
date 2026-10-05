FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/renderer-venv \
    && /opt/renderer-venv/bin/pip install --no-cache-dir \
      'azure-identity>=1.19,<2' 'azure-storage-blob>=12.24,<13'
COPY --from=build /src/target/release/uspto-ingestion-worker /usr/local/bin/uspto-ingestion-worker
COPY render_cloud.py /usr/local/bin/render_cloud.py
RUN mkdir -p /work
ENTRYPOINT ["/opt/renderer-venv/bin/python", "/usr/local/bin/render_cloud.py"]
