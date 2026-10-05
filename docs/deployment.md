# Deployment and updates

The [Terraform root](../terraform/README.md) owns PatPub's dedicated Azure
resource group, publication storage, Quickwit storage, queue, registry, Key
Vault, identities, logging, serving app, and five pipeline jobs. The public API
gateway is maintained separately in `api-gateway/`.

## Build images

Build from the repository root, except the XML renderer which uses its own
build context:

```sh
docker build -f fetch-worker/Dockerfile -t patpub-fetch .
docker build -f rust-worker/Dockerfile -t patpub-renderer rust-worker
docker build -f metadata-worker/Dockerfile -t patpub-metadata .
docker build -f fts/production/indexer/Dockerfile -t patpub-indexer .
docker build -f orchestration-worker/Dockerfile -t patpub-orchestrator .
docker build -f fts/production/serving/combined-app/Dockerfile -t patpub-serving .
```

Push to the deployment registry and record immutable `@sha256:` image references
in the ignored deployment variables. Keep the previous validated image digests
for rollback. Metadata and indexer builds compile both source renderers locally;
no private base image or sibling repository is needed.

## Maintain an existing deployment

Keep the existing backend, subscription, resource names, resource suffix,
`patent_index_id`, and `index_metastore_prefix`. The checked-in variable example
is a template, not a replacement for the running deployment's configuration.
Changing the suffix or corpus namespaces targets different resources or data.

```sh
terraform -chdir=terraform init -backend-config=backend.local.hcl
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=tfplan
```

Review the plan before applying. Resource definitions may move between `.tf`
files without changing their Terraform addresses. Storage and identity resources
remain when `compute_enabled=false`; that setting removes compute resources and
is not the routine ingestion pause switch. Changing `ingestion_enabled` changes
fetch/orchestrator trigger types and can replace jobs; do it while jobs are idle.

Secrets belong in encrypted inputs or the ignored local tfvars file. The USPTO
API key is installed in the project Key Vault outside Terraform. REST uses
`router_bearer_token`; MCP uses a separate `patpub_mcp_key`. Public serving
requires both. State and plan files contain sensitive infrastructure attributes
and must remain private.

For a new stack, register the Azure resource providers and provision a protected
Azure Blob backend before initializing this root. The backend is configured
with `backend.local.hcl`; see its checked-in example. This repository provisions
infrastructure and future ingestion, not an initial historical corpus.

## Public gateway

`api-gateway/worker.js` forwards the REST search, authenticated OpenAPI document,
and MCP routes to the serving app. Configure its upstream hostname and Wrangler
routes for the target deployment. It forwards the authentication headers and
serves the OpenAPI document with the public server URL. Review changes with the
serving API tests and run the [operator check](operations.md) after deployment.

The current gateway also depends on a Cloudflare Browser Integrity Check Skip
rule limited to its three API paths. Preserve that rule when maintaining the
public hostname; [client access](mcp.md) describes the route boundary.
