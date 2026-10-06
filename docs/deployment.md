# Deployment

PatPub deploys to Azure Container Apps with Blob Storage, Key Vault, and
Container Registry. The ingestion worker creates the index and publishes the
first release from an empty store.

## Create a deployment

Requirements:

- Python 3.11+, Azure CLI, and Terraform 1.6+.
- An Azure subscription with permission to provision resources and assign roles.
- A USPTO Open Data Portal API key.
- A globally unique suffix of 5–10 lowercase letters or digits.

Run from the repository root:

```sh
az login
cp ingestion.example.toml ingestion.toml
# Set document categories and year ranges before launching.
export USPTO_API_KEY='your USPTO ODP key'
python scripts/launch_azure.py --suffix mypub123
```

The launcher provisions billable resources, registers Azure providers, installs
the USPTO key in Key Vault, builds two Linux AMD64 images in Container Registry,
deploys them by immutable digest, and starts ingestion. Local Docker is not
required. The defaults are the active Azure CLI subscription and `eastus2`;
use `--subscription`, `--location`, and `--config` to override them.

The launcher stores credentials, deployment inputs, and local Terraform state
in `.patpub/<suffix>/`, excluded from Git and Docker build contexts. Back up
this directory privately. To resume a failed launch, rerun the command with
the same suffix, subscription, and region.

Search becomes available after the first archive is rendered, indexed, and
validated. Validation includes artifact checks and a search/source-retrieval
canary before publication of `releases/current.json`. Until then, search can
report that no release is available. `/readyz` checks Quickwit readiness, not
corpus availability.

## Select the corpus

Copy [ingestion.example.toml](../ingestion.example.toml) to `ingestion.toml` and
edit the local file. The launcher validates it and passes its contents to the
worker. Each category accepts `enabled`, `from_year`, and `through_year`.
Omitting `through_year` uses the category's default upper bound.

| Category | USPTO product | Supported publication years | Default |
| --- | --- | --- | --- |
| `aps_grants` | PTGRAPS | 1976–2001 | Enabled |
| `xml_grants` | PTGRXML | 2002 onward | Enabled |
| `application_publications` | APPXML | 2001 onward | Enabled |
| `pre_1976_grants` | Not implemented | — | Disabled |

Year filters use publication coverage dates, not archive upload or modification
dates. To include only 2001 APS grants, set `from_year = 2001`. To exclude APS
grants, set `categories.aps_grants.enabled = false`. Enabling pre-1976 ingestion
fails configuration validation because an OCR adapter is not implemented.

Acquisition merges enabled catalogs in chronological order. New publications
wait behind unfinished historical backfill. The worker first processes pending
retries, then acquires and processes missing archives.

`archives_per_run = 0`, the default, processes the selected backlog until caught
up. A positive value limits the number of archives processed in one run. Each
job execution has a 12-hour timeout; the hourly schedule resumes from durable
checkpoints.

Changing the selection affects future acquisition and pending work. It does
not remove published data. Pending events for disabled categories are retained
and can be processed when those categories are enabled again.

## Connect clients

The launcher prints the serving URL. Retrieve `router_bearer_token` and
`patpub_mcp_key` from the private `.patpub/<suffix>/terraform.tfvars.json` file.

| Endpoint | Authentication |
| --- | --- |
| `POST <serving-url>/api/search` | `Authorization: Bearer <router_bearer_token>` |
| `GET <serving-url>/openapi.json` | `Authorization: Bearer <router_bearer_token>` |
| `<serving-url>/patpub/mcp` | `X-Functions-Key: <patpub_mcp_key>` |

See [REST and MCP](mcp.md) for query examples and MCP host/origin configuration,
and [operations](operations.md#verify-serving) for a deployment check.

## Update a deployment

### Launcher-managed state

Keep the original `.patpub/<suffix>/` directory, subscription, and region.
After editing `ingestion.toml`, rerun the launcher with the original suffix:

```sh
python scripts/launch_azure.py --suffix mypub123
```

Existing image digests are reused. To deploy application changes, rebuild both
images:

```sh
python scripts/launch_azure.py --suffix mypub123 --rebuild-images
```

Apply updates while the ingestion worker is idle. The launcher applies its
Terraform configuration automatically and starts a job after deployment.

### Existing Terraform backend

Use the existing backend and private inputs in `terraform/`. Preserve the
subscription, resource names, suffix, index ID, and metastore namespace. Back
up state and the active release selector before changing the deployment.

```sh
terraform -chdir=terraform init -backend-config=backend.local.hcl
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=tfplan
```

Review the plan and apply it while workers are idle. Remove saved plans after
applying or abandoning them. Public ingress requires both REST and MCP keys.

`compute_enabled = false` removes the serving app and ingestion job while
retaining storage and identities. `ingestion_enabled = false` creates a manual
job instead of an hourly job; changing the trigger may replace the job.

For deployments using the earlier five-job pipeline, replace the old worker
image inputs with `ingestion_image` and retain `container_image`. The new
configuration removes the old jobs and queue and preserves the controller
identity through a Terraform `moved` block. Pending render events are recovered
from Blob Storage. Preserve existing verification receipts; their compatibility
rules are described in [operations](operations.md#integrity-evidence).

The launcher uses separate local state and does not modify the backend or
private inputs in `terraform/`. Moving its state to a remote backend requires
Terraform state migration.

## Build images locally

Run from the repository root:

```sh
docker build --platform linux/amd64 -f ingestion-worker/Dockerfile -t patpub-ingestion .
docker build --platform linux/amd64 -f fts/production/serving/combined-app/Dockerfile -t patpub-serving .
```

The ingestion image builds both Rust renderers from checked-in sources and
lockfiles. Push the images to your registry and set `ingestion_image` and
`container_image` to their immutable digest references.

## Optional gateway

The serving app exposes authenticated REST and MCP endpoints directly.
[api-gateway/worker.js](../api-gateway/worker.js) provides a Cloudflare gateway
for a custom domain. Set its upstream, routes, and OpenAPI server URL for your
deployment; the checked-in values refer to `api.jamienoel.com`.
