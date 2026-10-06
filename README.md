# PatPub

PatPub ingests published USPTO patent documents and serves release-aware full-text
search, canonical source hydration, snippets, and static publication metadata.

This repository contains the pipeline, service, infrastructure and maintenance
checks. The same worker bootstraps an empty store and maintains an existing
corpus; no migration tools or prebuilt index are required.

## Launch

With Python 3.11+, Azure CLI and Terraform installed, sign in with `az login`,
copy the saved ingestion choices, supply your USPTO ODP key, and choose a unique
deployment suffix:

```sh
cp ingestion.example.toml ingestion.toml
# Edit ingestion.toml to choose document categories and year ranges.
export USPTO_API_KEY='your USPTO ODP key'
python scripts/launch_azure.py --suffix mypub123
```

This provisions billable Azure resources, builds two images and starts backfill.
Search becomes available after the first validated archive; the hourly worker
continues from the oldest missing archive to the newest across enabled categories.
Credentials and state are private under `.patpub/<suffix>/`. Document choices
stay in ignored `ingestion.toml`. See [launch details](docs/deployment.md) for
access keys, prerequisites, coverage and updates. Docker Compose hosting is not
yet implemented.

## Components

- `ingestion-worker/`: the single production ingestion image.
- `ingestion/`: acquisition, shared rendering, metadata, index/release checkpoints
  and atomic publication.
- `rust-worker/` and `aps-worker/`: source renderers with locked, local dependencies.
- `fts/production/`: Quickwit indexing and authenticated REST/MCP serving.
- `api-gateway/`: optional custom-domain gateway.
- `terraform/`: one Azure root, organized by resource responsibility.
- `tests/`, `fts/tests/` and Rust fixtures: maintenance and empty-state checks.
- `scripts/`: fresh-stack launcher and operator smoke check.

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

## Empty-state integration check

After building `patpub-ingestion`, this check runs the real renderer, Parquet
projection and Quickwit against empty disk, then verifies publication, search,
source-frame hydration and idempotent replay. It uses file-backed Blob transport
and requires no Azure account:

```sh
docker run --rm --entrypoint /opt/ingestion-venv/bin/python \
  -v "$PWD/tests:/tests:ro" -v "$PWD/rust-worker/fixtures:/fixtures:ro" \
  patpub-ingestion /tests/bootstrap_integration.py
```

For the APS bootstrap check, use the same image and tests mount with `--aps`:

```sh
docker run --rm --entrypoint /opt/ingestion-venv/bin/python \
  -v "$PWD/tests:/tests:ro" patpub-ingestion /tests/bootstrap_integration.py --aps
```
