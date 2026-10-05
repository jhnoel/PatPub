# Steady-state operations

## Routine ingestion

Fetch runs daily at 12:00 UTC with a maximum of two archives per run. Immutable
`fetch/v1/` markers prevent repeat downloads. Fetch writes raw archives and queue
events; one queue-triggered orchestrator runs rendering, metadata, indexing,
release validation, and selector publication. Metadata uses 16 buckets for new
archives. Immutable artifacts and result checkpoints support retries.

The controller stops serving during index writes, then starts it for the canary.
An index writer must never overlap readers or another writer. Search can be
unavailable during ingestion and cold starts. Serving returns to zero replicas
when idle; frequent external health polling can keep it running.

For a failed archive, inspect the fetch/orchestrator and downstream job logs,
the event record, result checkpoint, and current selector. Verify no worker is
still writing before retrying the original immutable event. Do not manually
advance the selector past a failed canary. An already-published generation is
idempotent and returns the current release.

## Verify serving after an update

Set `ROUTER_BEARER_TOKEN` in the private process environment, then run the
operator check with a query known to return a publication in the corpus:

```sh
PATPUB_URL=https://api.jamienoel.com/patpub \
python scripts/check_serving.py --query 'wireless charging'
```

Set `SERVING_URL` to the origin URL to include readiness. Set `PATPUB_MCP_KEY` to
include MCP initialization and tool invocation. The check verifies missing-key
rejection, authenticated OpenAPI access, search results and release IDs. It makes
live requests and can wake a zero-replica service; run it on demand.

## Durable corpus compatibility

Historical migration tools are separate from this repository. Their retained
outputs are still part of the running corpus contract:

- Existing release manifests, source generations, metadata snapshots, selected
  publication IDs, and superseded generation IDs remain immutable.
- `migration/v1/artifact-version-capture.json`,
  `migration/v1/full-release-graph-validation.json`, and
  `migration/v1/full-release-candidate.json` support verified artifact reuse.
- `migration/v1/integrity/` contains hash evidence used by the indexer when its
  verification cache is enabled.
- `migration/v1/correction-withdrawal-exact.json` is consulted when a corrected
  archive omits previously selected publications.

Preserve these objects and profile-specific readers. Their `migration/` names
are persisted data keys, not disposable temporary files. New or changed artifacts
still require validation. Removing historical jobs does not remove their data.

## Recovery and rollback

Keep retained raw archives, immutable releases, source packs, metadata, the
validated Quickwit metastore, and the matching split set. Blob versioning and
soft-delete provide a limited recovery window; they do not substitute for a
validated rollback record.

Before an index-changing operation, record the selector bytes/ETag, metastore
snapshot, matching split versions, and image digests. Pause new ingestion and
stop readers and writers before restoring data. Restore the metastore and splits
bound to the chosen release, then update the selector with compare-and-swap.
Never roll back only `releases/current.json` against an unrelated metastore.

Restart serving and verify readiness, authentication, release identity, known
publication counts, and source hydration across the affected renderer profiles.
Resume ingestion only after validation. Corpus-specific repair and historical
reconstruction tools remain in the private maintenance archive.
