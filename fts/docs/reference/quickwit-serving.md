# Quickwit serving

The [index mapping](../../production/indexer/index.yaml) indexes title, abstract,
claims, description, publication identifiers, dates, and classifications. Large
text fields are retrieved from source packs; Quickwit stores the small metadata
and pointers needed to identify and hydrate results.

The [searcher configuration](../../production/serving/searcher-node.yaml) reads
index objects and the file metastore from Azure Blob Storage. Local data and
in-memory caches are disposable. The combined Container App runs one Quickwit
process and one API process, with Quickwit listening on loopback.

Serving scales from zero to one replica. Startup, readiness, and liveness use
`/readyz`, which checks Quickwit readiness. A cold start recreates local caches;
clients must allow for startup and the configured search timeout.

The file metastore requires a single writer. The orchestrator stops serving
before index updates and restores it for the release canary. Do not run readers
alongside an index writer or increase serving replicas without redesigning
metastore concurrency.

Search is restricted to the active release's shards. Release manifests bind
source generations, pack identities, metadata snapshots, and publication
selections. Changing the selector without the matching index/metastore can
break that binding; see [operations](../../../docs/operations.md).
