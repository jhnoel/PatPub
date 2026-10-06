# Quickwit serving

## Index fields

The [index mapping](../../production/indexer/index.yaml) uses a strict schema
and disables storage of the original input document.

| Fields | Behavior |
| --- | --- |
| `title`, `abstract`, `claims`, `description` | Indexed with positional records; searched by default; retrieved from source packs |
| Publication/application identifiers, dates, `doc_kind` | Indexed and stored |
| `applicant`, `assignee`, `ipc`, `us_class` | Indexed and stored |
| `source_generation` | Indexed and stored; used to restrict queries to the active release |
| Pack identity, key, offset, lengths, revision, fingerprint | Stored source pointers used to retrieve and verify document text |

The API reads canonical source text to generate snippets. It does not perform
a metadata lookup for each hit.

## Process and storage

The [searcher configuration](../../production/serving/searcher-node.yaml) reads
index objects and the file-backed metastore from Azure Blob Storage. In the
combined Container App, Quickwit listens on loopback and the API owns ingress.
Local data and in-memory caches are disposable.

Serving scales from zero to one replica. Startup, readiness, and liveness
probes use `/readyz`, which checks Quickwit readiness. It does not confirm that
a release has been published. Cold starts recreate local caches.

The search budget is 60 seconds. The router waits up to 65 seconds per upstream
request by default; clients also need time for source reads and startup.

## Concurrency and release state

The file-backed metastore requires a single writer and no concurrent readers
during writes. The ingestion worker stops serving before an index update and
restarts it after the writer exits. Increasing replicas or running a reader
alongside the writer requires a different metastore concurrency design.

Queries use the active release's index IDs and source-generation filter.
Release manifests also bind metadata snapshots and source-pack identities.
A selector change does not restore index state. Rollback must restore the
matching metastore and split set; see [operations](../../../docs/operations.md#roll-back-an-index-update).
