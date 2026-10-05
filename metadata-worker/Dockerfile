FROM rust:1.90-bookworm AS aps-build
WORKDIR /aps
COPY aps-worker ./
RUN cargo build --release --locked

FROM rust:1.90-bookworm AS renderer-build
WORKDIR /src
COPY rust-worker/Cargo.toml rust-worker/Cargo.lock ./
COPY rust-worker/src ./src
RUN cargo build --release --locked

FROM python:3.13-slim

WORKDIR /app
COPY pyproject.toml ./
COPY ingestion ./ingestion
COPY --from=renderer-build /src/target/release/uspto-ingestion-worker /usr/local/bin/uspto-ingestion-worker
COPY --from=aps-build /aps/target/release/aps-backfill /usr/local/bin/aps-backfill
RUN pip install --no-cache-dir '.[azure,parquet]'

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "ingestion.metadata_cloud"]
