# Operations

## Ingestion

One hourly worker performs acquisition, rendering, metadata projection,
indexing, and release publication. It uses the same pipeline for the first
archive, historical backfill, retries, and new publications.

The worker reads category and year selections from the deployed ingestion
configuration. It processes pending events first, then acquires missing
archives from enabled catalogs in chronological order. Disabled categories are
excluded from acquisition and pending-event processing. The default
`archives_per_run = 0` processes all available work; a positive value sets a
per-run archive limit. The job's 12-hour timeout limits execution, and later
runs resume from checkpoints.

A renewable 60-second Blob lease covers the entire worker run. Overlapping
executions exit without processing. If renewal fails, the worker terminates
its process group, including the Quickwit writer. The lease expires after
process death.

### Checkpoints

All paths below are in the releases container.

| Path | Purpose |
| --- | --- |
| `fetch/v1/` | Acquisition receipts that avoid repeated downloads and historical header checks |
| `events/render-*.json` | Immutable archive events for pending work |
| `events/results/` | Completed render, metadata, and index stage receipts |
| `ingestion/completed/` | Completed archive publication receipts |
| `integrity/v1/` | Hash evidence bound to Blob versions |
| `releases/current.json` | Selector for the active release manifest |

A retry reconstructs the temporary projection and compares it with the render
receipt. Completed metadata and index stages can be reused. A generation
already present in the selected release returns without rendering again.
Checkpoints make work discoverable after a crash between stage writes.

For older deployments without fetch receipts, a release-bound inventory
compares published archive filenames, sizes, and modification dates with the
publisher catalog. Matching archives retain their recorded renderer profile.
Revised archives use normal acquisition; missing dates do not count as a
match. This comparison does not download historical archives or rebuild the
index.

### Indexing and availability

Quickwit's file-backed metastore requires exclusive access during writes. The
worker stops the serving app, runs the synchronous writer, waits for it to
exit, then restarts serving for validation. Search is unavailable during
index writes and cold starts.

An index failure leaves serving stopped. The next worker reconciles the
incomplete generation before restarting serving. Do not restart readers
against an incomplete write.

Before publishing a release, the worker validates artifacts and runs a canary
that checks search and source retrieval. It then changes the active selector
with compare-and-swap. A correction can supersede a source generation only if
its metadata retains every publication ID in that generation. A correction
that removes publications fails before indexing; withdrawal lists are not
supported.

### Integrity evidence

Validation records each artifact's Blob key, ETag, size, and SHA-256 digest.
Unchanged versions reuse verified hashes. New or changed versions are hashed
and receive new evidence.

Older verified receipts can be adopted after the worker validates their graph
proof and matches the Blob versions. This reads evidence records, not corpus
bytes. For published artifacts absent from an older aggregate cache, the
worker can reuse a release-validated digest only when the size matches and
Azure reports equal creation and modification times strictly before release
publication.

If a published artifact lacks reusable evidence, ingestion stops rather than
rehashing the historical corpus. Preserve verification receipts during
upgrades. New deployments create them through ordinary ingestion.

## Verify serving

Set `ROUTER_BEARER_TOKEN` in the environment and choose a query known to match
the deployed corpus.

For a direct Container App endpoint:

```sh
export PATPUB_URL='https://your-serving-host'
export SERVING_URL="$PATPUB_URL"
export PATPUB_MCP_PATH='/patpub/mcp'
python scripts/check_serving.py --query 'wireless charging'
```

For the checked-in gateway:

```sh
export PATPUB_URL='https://api.jamienoel.com/patpub'
export PATPUB_MCP_PATH='/mcp'
python scripts/check_serving.py --query 'wireless charging'
```

The check verifies missing and invalid REST credentials, OpenAPI, authenticated
search, release identity, and hydrated snippets. `SERVING_URL` adds a readiness
check against the direct serving URL. `PATPUB_MCP_KEY` adds MCP authentication
and search checks. See [client configuration](mcp.md) for MCP host requirements.
The check can wake a service that has scaled to zero.

## Recover a failed run

Inspect the ingestion job log, archive event, stage receipts, and current
selector. Resolve the reported failure, then start the ingestion job again.
Do not edit the selector or delete stage receipts to bypass validation.

Retain raw archives, source packs, metadata, immutable generation and release
manifests, verification receipts, metastore snapshots, and their matching
split files. Preserve publication inventories and renderer profiles when
upgrading an existing corpus. Blob versioning and soft delete provide a
limited recovery window.

## Roll back an index update

Before an index-changing update, record the active selector, metastore
snapshot, matching split versions, and image digests.

1. Stop ingestion and serving.
2. Restore the metastore and split set bound to the target release.
3. Update the release selector using compare-and-swap.
4. Start serving and verify publication counts, authentication, release
   identity, and source retrieval.
5. Resume ingestion after verification succeeds.

Restoring only the selector is insufficient when the live index state differs
from the target release.
