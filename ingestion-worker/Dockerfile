FROM rust:1.90-bookworm AS aps-build
WORKDIR /aps
COPY aps-worker ./
RUN cargo build --release --locked

FROM rust:1.90-bookworm AS renderer-build
WORKDIR /src
COPY rust-worker/Cargo.toml rust-worker/Cargo.lock ./
COPY rust-worker/src ./src
RUN cargo build --release --locked

FROM quickwit/quickwit:0.9.0
USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/ingestion-venv
ENV PATH=/opt/ingestion-venv/bin:$PATH
COPY pyproject.toml /opt/patpub/pyproject.toml
COPY ingestion /opt/patpub/ingestion
COPY fts /opt/patpub/fts
RUN pip install --no-cache-dir '/opt/patpub[azure,parquet]'
COPY --from=renderer-build /src/target/release/uspto-ingestion-worker /usr/local/bin/uspto-ingestion-worker
COPY --from=aps-build /aps/target/release/aps-backfill /usr/local/bin/aps-backfill
COPY fts/production/indexer/node.yaml /quickwit/node.yaml
COPY fts/production/indexer/index.yaml /quickwit/index.yaml
ENV PYTHONPATH=/opt/patpub
RUN mkdir -p /work/qwdata
ENTRYPOINT ["/opt/ingestion-venv/bin/python", "-m", "ingestion.worker"]
