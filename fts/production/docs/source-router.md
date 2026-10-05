# Release-aware source router

`fts.production.serving.api.source_router` is the serving boundary between the versioned
Quickwit index and canonical Markdown. It runs as a stateless Azure Container
App with a user-assigned managed identity.

For each `POST /api/search` request it:

1. reads and verifies `releases/current.json` and the immutable release
   manifest;
2. sends the query only to the manifest's index and active shards;
3. checks every hit's source generation, pack digest, and pack key against the
   active release;
4. performs one bounded Blob range read for the compressed frame;
5. verifies the Rust producer's Blake2b-512-truncated fingerprint, Zstandard
   output size, canonical text SHA-256, and publication identity; and
6. returns the Quickwit hit with generated text snippets and the active release ID.

The router never accepts an index name from the caller. A release change is
therefore one selector CAS away from changing both search and hydration. Keep
ingress private or behind an authenticated gateway. For service-to-service
authentication, set `ROUTER_BEARER_TOKEN` as a secret and send
`Authorization: Bearer <token>` with each request.

The router publishes its OpenAPI 3.0 document at `GET /openapi.json`. Add the
router's base URL as an OpenAPI tool server in Open WebUI; configure Bearer
authentication there with the configured `ROUTER_BEARER_TOKEN`. The document
describes the public `POST /api/search` operation and intentionally omits the
internal candidate-index search route.
