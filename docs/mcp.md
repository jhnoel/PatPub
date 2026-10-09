# REST and MCP

REST and MCP search the same active release. Results include publication
metadata, snippets generated from canonical source text, and
`active_release_id`. Full-text point reads return complete canonical Markdown
by `publication_uid`. MCP omits internal source-pack pointers.

## Endpoints and authentication

Paths differ between the direct serving app and the checked-in custom-domain
gateway.

| Interface | Direct serving path | Gateway URL | Required header |
| --- | --- | --- | --- |
| REST search | `/api/search` | `https://api.jamienoel.com/patpub/api/search` | `Authorization: Bearer <REST token>` |
| REST batch full text | `/api/publications` (POST) | `https://api.jamienoel.com/patpub/api/publications` | `Authorization: Bearer <REST token>` |
| REST full text | `/api/publications/{publication_uid}` | `https://api.jamienoel.com/patpub/api/publications/{publication_uid}` | `Authorization: Bearer <REST token>` |
| OpenAPI | `/openapi.json` | `https://api.jamienoel.com/patpub/openapi.json` | `Authorization: Bearer <REST token>` |
| MCP | `/patpub/mcp` | `https://api.jamienoel.com/patpub/mcp` | `X-Functions-Key: <MCP key>` |

Terraform's `router_bearer_token` variable supplies the `ROUTER_BEARER_TOKEN`
environment variable through a Container App secret. The separate MCP key
uses `patpub_mcp_key` and `PATPUB_MCP_KEY` in the same way.

The gateway forwards credentials to the serving app and sets the OpenAPI
server URL to `https://api.jamienoel.com/patpub`.

## REST request

Set `PATPUB_URL` to the direct serving URL or the gateway base URL, including
`/patpub` for the gateway. Set `ROUTER_BEARER_TOKEN` in the environment.

```sh
curl --fail-with-body "$PATPUB_URL/api/search" \
  -H "Authorization: Bearer $ROUTER_BEARER_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"query":"wireless charging","max_hits":10,"start_offset":0}'
```

The [OpenAPI definition](../fts/production/serving/api/openapi.json) describes
the request and response schemas. `max_hits` defaults to 10 and is capped at
100. `start_offset` defaults to 0 and accepts values from 0 through 900.

## Full-text point reads

Use the `publication_uid` from a search hit to read a publication:

```sh
curl --fail-with-body "$PATPUB_URL/api/publications/US:9999999:B2" \
  -H "Authorization: Bearer $ROUTER_BEARER_TOKEN"
```

The response contains `publication_uid`, `publication_number`, `markdown`, and
`active_release_id`. `markdown` is the complete canonical publication text.
Reads use the active release and its correction selection, so a publication
absent from that release returns HTTP 404. Invalid IDs return HTTP 400.
Source integrity or upstream failures return HTTP 502.

MCP exposes the same operation as `get_patent(publication_uid="US:9999999:B2")`.
Invalid or missing publications produce MCP tool errors.

For multiple publications, submit a list of 1 to 100 canonical IDs:

```sh
curl --fail-with-body "$PATPUB_URL/api/publications" \
  -H "Authorization: Bearer $ROUTER_BEARER_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"publication_uids":["US:9999999:B2","US:10000000:B2"]}'
```

MCP accepts `get_patents(publication_uids=["US:9999999:B2", "US:10000000:B2"])`
or `get_patent(publication_uid=["US:9999999:B2", "US:10000000:B2"])`.
Batch responses contain `documents`, `missing_publication_uids`, and
`active_release_id`. Documents follow input order with duplicate IDs removed.
Missing publications are reported in the list, including when all IDs are
missing; a valid batch returns HTTP 200. Invalid input returns HTTP 400.
All documents use one selected release and share the configured hydration byte
budget; budget, integrity, or retrieval failures fail the entire request.

### Response format

Single reads return a publication object. Batch reads return this envelope:

```json
{
  "documents": [
    {
      "publication_uid": "US:9999999:B2",
      "publication_number": "US9999999B2",
      "markdown": "<!-- publication_uid: US:9999999:B2 -->\n# Patent title\n\n## Abstract\nComplete abstract.\n\n## Description\nComplete description.\n\n## Claims\nComplete claims.\n",
      "active_release_id": "release-example"
    }
  ],
  "missing_publication_uids": [
    "US:10000000:B2"
  ],
  "active_release_id": "release-example"
}
```

`markdown` contains the entire canonical document, without snippet truncation.
`publication_number` can be `null`. Every document and the batch envelope carry
the same `active_release_id`. An all-missing batch returns an empty `documents`
array and lists every requested ID in `missing_publication_uids`.

## Query fields

Queries use Quickwit syntax. Unqualified queries search `title`, `abstract`,
`claims`, and `description`. Narrowing the fields can reduce search latency:

```text
claims:"large language model"
assignee:Google AND (title:transformer OR abstract:attention)
```

Indexed publication fields include `publication_number`, `publication_uid`,
`patent_number`, `application_number`, `doc_kind`, `publication_date`, and
`filing_date`. Names and classifications use `applicant`, `assignee`, `ipc`, and
`us_class`. See the [index mapping](../fts/production/indexer/index.yaml) for
the complete field definitions.

`inventor` is not indexed. `applicant:Vaswani` searches applicant names, which
contain inventor names in some records; it does not provide complete inventor
coverage. Invalid fields and query syntax return a query explanation.

Quickwit's configured search budget is 60 seconds. The router's upstream
timeout defaults to 65 seconds and can be changed with
`UPSTREAM_TIMEOUT_SECONDS`. Keep it above the engine budget and allow
additional time for source retrieval and cold starts in client timeouts.

## MCP clients

The `search_patents` tool accepts `query`, `max_hits`, and `start_offset` with
the same query behavior and limits as REST. The server uses stateless
Streamable HTTP with JSON responses.

For Open WebUI or another MCP client, configure:

```text
URL: https://api.jamienoel.com/patpub/mcp
Header: X-Functions-Key: <PatPub MCP key>
```

For another deployment, use its MCP endpoint from the table above. The MCP
transport validates the request Host against an allowlist. The defaults include
the checked-in gateway and serving host, plus local development hosts. Add
other hostnames through `PATPUB_MCP_ALLOWED_HOSTS` in the serving app's
environment; the Terraform root does not currently configure this setting.

Clients that send an `Origin` header also need that origin in the optional,
comma-separated Terraform variable `patpub_mcp_allowed_origins`. Requests
without an Origin header are allowed.

If Cloudflare Browser Integrity Check blocks machine clients, scope a Skip
rule to `/patpub/mcp`, `/patpub/openapi.json`, `/patpub/api/search`, and `/patpub/api/publications*` on the
gateway host and skip only Browser Integrity Check (`bic`). The rule name for
the documented `api.jamienoel.com` configuration is `patpub_skip_bic`. Origin
authentication and other security controls still apply. Use the
[serving check](operations.md#verify-serving) to verify access after deployment.
