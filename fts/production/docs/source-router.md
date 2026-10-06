# Source router

[The router](../serving/api/source_router.py) connects Quickwit results to
canonical Markdown in Blob Storage. It runs inside the combined serving
Container App and uses a managed identity for release and source reads.

## Search contract

For each public `POST /api/search` request, the router:

1. Reads `releases/current.json` and verifies the selected immutable manifest's
   digest. A verified manifest is cached until the selector changes.
2. Sends the query to the release's explicit index IDs, filtered to its active
   source generations and publication selections.
3. Checks each hit's generation, pack digest, and pack key against the release.
4. Reads the hit's compressed frame through a bounded Blob range request.
5. Verifies the frame fingerprint, decompressed size, canonical text SHA-256,
   and embedded publication identity.
6. Generates snippets and returns the hit with the active release ID.

The caller cannot select an index through the public endpoint. Responses
include `active_release_id`, `active_selector_sha256`, and
`active_manifest_sha256` so clients can identify the release used by a request.

Publication validates the complete artifact graph. Serving checks the selected
manifest and requested frames; it does not rehash every index split or source
pack on each search.

## Limits

| Limit | Value |
| --- | --- |
| Request body | 1 MiB |
| Returned hits | 100 |
| Result offset | 0–900 |
| Compressed or uncompressed document frame | 64 MiB each |
| Combined compressed and uncompressed bytes per response | 128 MiB by default |
| Timeout per upstream search request | 65 seconds by default |

`MAX_TOTAL_HYDRATION_BYTES` and `UPSTREAM_TIMEOUT_SECONDS` configure the response
budget and upstream timeout. The [source format](../../docs/reference/zstd-source-format.md)
specifies the pointer and fingerprint formats.

## Interfaces

REST search and `GET /openapi.json` require
`Authorization: Bearer <ROUTER_BEARER_TOKEN>`. The
[OpenAPI definition](../serving/api/openapi.json) describes the public search
operation. MCP uses a separate key and removes internal pointer fields from
its results; see [client access](../../../docs/mcp.md).

The authenticated candidate-index route is used for pre-publication canaries
and is omitted from OpenAPI. It does not select a published release.

Changing the release selector does not restore a previous index or metastore.
See [rollback](../../../docs/operations.md#roll-back-an-index-update) before
changing release state.
