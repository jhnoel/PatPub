# Operation and recovery

## Ingestion

One hourly worker discovers archives, downloads immutable ZIPs, renders each
archive once, produces Parquet metadata and appends to Quickwit using that same
temporary projection. It validates the release and atomically changes the active
selector after a search and source-hydration canary. The same path handles the
first archive, backlog and new weekly publications. The local `ingestion.toml`
chooses enabled categories and year ranges. Backfill merges their catalogs from
oldest missing archive to newest. Disabled categories are excluded from both
acquisition and pending-event processing.

Existing validated archive generations are recognized even when an older
deployment lacks fetch markers. A small inventory, cached against the selected
release digest, compares publisher filenames, sizes and modification dates.
Unchanged published archives retain their existing renderer profile. New
revisions or recent publisher updates use normal acquisition; absent dates
never silently skip an archive. This does not download or hash historical raw
archives or rebuild the index.

`fetch/v1/` markers deduplicate acquisition. Immutable `events/render-*.json`
records retain pending work; stage results under `events/results/` retain
completed metadata and index work. `ingestion/completed/` records publication.
A crash between these writes leaves discoverable work for the next run. A retry
renders once to reconstruct its temporary projection and checks it against the
previous render receipt; metadata and index results can be reused. A generation
already present in the selected release returns immediately.

A renewable 60-second Blob lease protects the whole worker. Overlapping scheduled
or manual executions exit without processing. Renewal failure terminates the
worker's process group, including Quickwit. Process death lets the lease expire,
so a permanent manual unlock is unnecessary.

The worker stops the serving app before running its synchronous Quickwit writer,
waits for the writer to exit, then restarts serving for the canary. This remains
necessary because Quickwit uses a file-backed metastore. Search is unavailable
during writes and cold starts. A failed index append leaves serving stopped;
the next worker reconciles the incomplete generation and resumes serving.

Normal validation creates hash evidence under `integrity/v1/`. Each proof is
bound to the exact Blob key, ETag, size and digest. New or changed objects are
hashed; unchanged versions reuse verified hashes. Existing deployments also
automatically adopt their previous verified receipts after checking the graph
proof and matching Blob versions. This copies small evidence records without
rereading the corpus. Missing evidence for previously published artifacts fails
closed instead of triggering a historical rehash. New deployments create these
records through ordinary ingestion and need no compatibility files.

Corrections can supersede a generation only if their metadata preserves all its
publication IDs. A correction that removes publications fails before indexing
and keeps the previous release selected. This conservative rule does not accept
an out-of-band withdrawal list.

Inspect the single ingestion job log, archive event, stage receipts and current
selector after a failure. Retry by starting the ingestion job; do not edit the
selector or delete stage receipts to bypass a failed canary. The first release
can take a full archive's download/render/index duration. Increasing
By default each hourly run continues through the selected backlog until it is
caught up. Set a positive `archives_per_run` value in the saved TOML file to
deliberately bound a run; Azure's 12-hour job timeout remains the hard limit, and
receipts let a later scheduled run resume safely.

## Verify serving

Set `ROUTER_BEARER_TOKEN` in the private environment, then use a query expected to
match your corpus:

```sh
PATPUB_URL=https://your-serving-host python scripts/check_serving.py --query 'wireless charging'
```

Set `SERVING_URL` to include readiness and `PATPUB_MCP_KEY` to include MCP checks.
For a direct serving URL, set `PATPUB_MCP_PATH=/patpub/mcp`; the gateway default
is `/mcp`.
The check verifies authentication, OpenAPI, search and release identity. It can
wake a zero-replica service; use it on demand.

## Recovery and rollback

Retain raw archives, immutable generations/releases, source packs, metadata,
metastore snapshots and matching split files. Existing selected-publication
inventories and renderer profiles remain supported. Keep previous verification receipts during an upgrade: the worker
automatically adopts the verified aggregate cache after checking its graph proof,
then reuses matching versions from batched Blob listings. No corpus bytes are
read during this adoption. Later published artifacts absent from that old cache
can also reuse their validated release digest when Azure confirms their creation
and last-modified timestamps are equal and strictly before release publication.
Changed objects cannot use this path. Blob versioning and soft-delete offer a
limited recovery window.

Before an index-changing update, record the selector, metastore snapshot,
matching split versions and image digests. Stop readers and writers before
restoring the metastore and split set bound to the desired release, then update
the selector using compare-and-swap. Restoring only the selector against an
unrelated live index is insufficient. Verify counts, authentication, release
identity and source hydration before resuming ingestion.
