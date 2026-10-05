# PatPub

PatPub ingests published USPTO patent documents and serves release-aware full-text
search, canonical source hydration, snippets, and static publication metadata.

This repository contains the steady-state pipeline, service, infrastructure,
and their maintenance tests. Historical backfills, migration tools, experimental
services, production data, and deployment credentials are maintained separately.

## Components

- `fetch-worker/`: daily archive acquisition and immutable deduplication markers.
- `rust-worker/`: XML renderer and verified source-pack producer.
- `aps-worker/`: APS renderer used by metadata and indexing workers for retained
  archives; includes the Apache-licensed `patpubrender` source dependency.
- `ingestion/`: generation records, metadata projections, releases, and pipeline
  orchestration.
- `metadata-worker/` and `orchestration-worker/`: production image definitions.
- `fts/production/`: Quickwit indexing, authenticated REST/MCP serving, hydration,
  and snippets.
- `api-gateway/`: public API gateway.
- `terraform/`: the Azure deployment, organized by resource responsibility.
- `tests/`, `fts/tests/`, and worker fixtures: maintenance checks.
- `scripts/`: operator smoke check.

## Development

```sh
uv sync --extra dev --extra azure --extra parquet --extra router --extra mcp
uv run python -m pytest
cargo test --locked --manifest-path rust-worker/Cargo.toml
cargo test --locked --manifest-path aps-worker/Cargo.toml
```

See [deployment](docs/deployment.md), [operations](docs/operations.md),
[REST and MCP access](docs/mcp.md), and the [search implementation](fts/README.md).

## License and data

The software is licensed under [Apache License 2.0](LICENSE); see the
[third-party notices](THIRD-PARTY-NOTICES.md). USPTO archives and patent documents
are retrieved separately and retain their applicable terms. Corpora, archives,
source packs, generated indexes, Terraform state, and secrets are excluded from
version control. Included fixtures are small synthetic documents.
