# PatPub

PatPub ingests published USPTO patent grants and applications and serves
full-text search through REST and MCP. It converts APS and XML archives to
canonical Markdown, stores publication metadata in Parquet, and indexes text
with Quickwit.

Search uses a published release that binds index state, metadata, and source
generations. The API verifies each hit against that release, reads the matching
document from an immutable source pack, and generates snippets. Serving uses
stored publication data and has no live dependency on patent-record services.

## Deploy

Requirements: Python 3.11+, Azure CLI, Terraform 1.6+, an Azure subscription
with permission to create resources and assign roles, and a USPTO Open Data
Portal API key. Choose a globally unique suffix of 5–10 lowercase letters or
digits.

```sh
az login
cp ingestion.example.toml ingestion.toml
# Set document categories and year ranges in ingestion.toml.
export USPTO_API_KEY='your USPTO ODP key'
python scripts/launch_azure.py --suffix mypub123
```

The launcher creates billable Azure resources, builds two container images in
Azure Container Registry, and starts ingestion. Search becomes available after
the first archive passes validation and its release is published. Private
credentials and Terraform state are stored in `.patpub/<suffix>/`.

See [deployment](docs/deployment.md) for configuration, credentials, and updates.
Azure is the supported deployment target; Docker Compose hosting is not
implemented.

## Operating model

- One hourly worker handles initial backfill, retries, and new publications.
  Enabled catalogs are processed from the oldest missing archive to the newest;
  new weeks wait behind the historical backlog.
- The default configuration covers APS grants from 1976–2001, XML grants from
  2002 onward, and application publications from 2001 onward. Pre-1976 ingestion
  is not implemented.
- Quickwit uses a file-backed metastore. The worker stops serving during index
  writes and restarts it for validation. Search is unavailable during writes
  and cold starts; serving scales from zero to one replica.

## Repository

| Path | Responsibility |
| --- | --- |
| `ingestion/` | Archive acquisition, rendering, metadata, checkpoints, and release publication |
| `ingestion-worker/` | Ingestion container, including both Rust renderers and the Quickwit writer |
| `rust-worker/`, `aps-worker/` | XML and APS renderers |
| `fts/production/` | Quickwit indexing, source retrieval, snippets, and REST/MCP serving |
| `terraform/` | Azure infrastructure |
| `api-gateway/` | Optional Cloudflare gateway for a custom domain |
| `scripts/` | Deployment launcher and serving checks |
| `tests/`, `fts/tests/` | Pipeline, release, and search tests |

## Development

Install uv and Rust, then run from the repository root:

```sh
uv sync --locked --extra dev --extra azure --extra parquet --extra router --extra mcp
uv run python -m pytest
cargo test --locked --manifest-path rust-worker/Cargo.toml
cargo test --locked --manifest-path aps-worker/Cargo.toml
```

[Contributing](CONTRIBUTING.md) covers focused checks and container integration
tests. The [documentation index](docs/README.md) links deployment, operations,
client access, and search references.

## License and data

The software is licensed under [Apache License 2.0](LICENSE). Dependencies retain
their own licenses; see [third-party notices](THIRD-PARTY-NOTICES.md). USPTO data
is retrieved separately and remains subject to its applicable terms.

The repository includes small synthetic fixtures. Corpora, archives, source
packs, generated indexes, Terraform state, and secrets are excluded from
version control.
