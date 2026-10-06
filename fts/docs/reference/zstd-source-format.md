# Source packs

A source pack is a sequence of independently compressed Zstandard frames.
Each frame contains one canonical Markdown publication. Packs are immutable
and addressed by the SHA-256 digest of their complete bytes.

## Source pointer

Each indexed document stores the fields needed to read its frame.

| Field | Meaning |
| --- | --- |
| `publication_uid` | Publication identity, also embedded in the Markdown's first line |
| `source_generation` | Generation recorded in the release manifest |
| `source_pack` | Pack identity as `sha256:<digest>` |
| `source_pack_key` | Blob key for the pack |
| `offset`, `length` | Compressed frame byte range |
| `uncompressed_length` | Expected Markdown byte length |
| `source_revision` | Canonical Markdown identity as `sha256:<digest>` |
| `source_fingerprint` | First 128 bits of BLAKE2b-512 over the compressed frame |

The fingerprint is serialized with the prefix `blake2b-128:`. It is a truncated
BLAKE2b-512 digest; configuring BLAKE2b to produce a 128-bit digest produces a
different value.

## Retrieval checks

The router verifies that the pointer's generation, pack digest, and pack key
belong together in the active release. It reads the requested Blob range,
checks the returned range and byte count, verifies the frame fingerprint,
decompresses to the recorded length, and checks the Markdown SHA-256 and
publication identity.

The router limits compressed and uncompressed frame sizes to 64 MiB each.
The default per-response budget is 128 MiB for their combined sizes. Pack-level
hashes are validated during release publication; serving verifies the frames
it reads rather than hashing the complete pack for every hit.

## Renderer compatibility

Current ingestion profiles are `uspto-xml-markdown-v2:zstd-9-checksum` and
`aps-greenbook-markdown-v2:zstd-9-checksum`. Retained generations may record
earlier profiles. Preserve their pack bytes, digests, pointers, and generation
identities. A renderer update must create a new generation rather than replace
a previously recorded source.

Implementations:
[XML renderer](../../../rust-worker/src/lib.rs),
[APS pack writer](../../../aps-worker/src/pack.rs),
[archive pipeline](../../../ingestion/archive.py), and
[router](../../production/serving/api/source_router.py).
