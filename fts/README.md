# Full-text search

`production/indexer/` updates the durable Quickwit index from verified temporary
projections of retained raw archives. `production/serving/` contains the combined
Quickwit and REST/MCP application, source hydration, and snippets.

The release pipeline selects the active index shards and source generations.
Search results are verified against that release before source frames are read.
Metadata and indexing workers use the XML renderer in `rust-worker/` and the APS
renderer in `aps-worker/`. Both are built from this repository.

See the [router contract](production/docs/source-router.md),
[Quickwit serving](docs/reference/quickwit-serving.md),
[source format](docs/reference/zstd-source-format.md), and
[operations guide](../docs/operations.md).

Build the service and indexer from the repository root:

```sh
docker build -f fts/production/serving/combined-app/Dockerfile .
docker build -f fts/production/indexer/Dockerfile .
```
