# Immutable source packs

A source pack contains independently compressed Markdown documents. Each source
pointer identifies the pack key and digest, compressed offset and length,
source generation, publication identity, and frame fingerprint. Current renderer
profiles also record uncompressed length and canonical text SHA-256.

The router reads exactly one Blob range, checks its bounds, verifies the frame
fingerprint, bounds decompression, and checks the document identity and text
integrity according to the recorded renderer profile. The index pointer is
sufficient for hydration; a metadata lookup is not required for each hit.

New archives use `uspto-xml-markdown-v2:zstd-9-checksum`. Retained corpus records
may use earlier profiles. Keep profile-specific reader support and preserve the
original pack bytes, digests, and generation identities. Re-rendering an old
archive with a current renderer must not silently replace its recorded source.

Implementations: [XML producer](../../../rust-worker/src/lib.rs),
[APS producer](../../../aps-worker/src/pack.rs),
[archive rendering](../../../ingestion/archive.py), and
[router](../../production/serving/api/source_router.py).
