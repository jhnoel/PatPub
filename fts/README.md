# Full-text search

PatPub indexes patent text with Quickwit and retrieves canonical Markdown from
immutable source packs. The search API returns stored publication metadata,
generated snippets, and the active release ID.

## Indexing

The ingestion image runs [the indexer](production/indexer/indexer.py) as part of
the archive pipeline. The XML and APS renderers produce one temporary projection
per archive attempt. Metadata generation and indexing consume that projection;
the raw archive and source packs remain in Blob Storage.

Quickwit indexes the text and stores publication metadata and source pointers.
The index and metastore are durable; local caches are disposable. The
[serving reference](docs/reference/quickwit-serving.md) describes the mapping
and metastore concurrency requirements.

## Serving

The serving container runs Quickwit on loopback and exposes an authenticated
REST/MCP API. For each search, the router resolves the active release, restricts
the query to its selected indexes and source generations, verifies the returned
source pointers, and reads one compressed document frame per hit.

The release binds index state, metadata snapshots, source generations, and pack
identities. Source checks prevent a hit from being hydrated from an unrelated
generation. Publication validation and request-time checks have separate roles;
see the [router contract](production/docs/source-router.md) and
[source format](docs/reference/zstd-source-format.md).

The worker stops serving during index writes because the file-backed metastore
requires exclusive access. Deployment runs zero to one serving replica.
See [operations](../docs/operations.md) for failure recovery and rollback.

## Build and test

The two production images are the ingestion image and the combined serving
image. Build commands are in [deployment](../docs/deployment.md#build-images-locally).

Run search tests from the repository root after installing development
dependencies:

```sh
uv run python -m pytest fts/tests
```
