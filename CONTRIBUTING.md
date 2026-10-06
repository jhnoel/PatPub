# Contributing

## Report a defect

Include the affected commit, a minimal reproduction, expected behavior, and
observed behavior. For ingestion or search failures, identify the archive,
renderer profile, and release involved.

Use small synthetic fixtures. Do not attach USPTO archives, publication
corpora, credentials, Azure resource IDs, or production data to issues or pull
requests. Report vulnerabilities through the [security policy](SECURITY.md).

## Verify a change

The [README](README.md#development) lists dependency setup and the full Python
and Rust test commands. Run the checks relevant to the change:

- Acquisition, configuration, metadata, pipeline, or releases: the corresponding
  files in `tests/`.
- Indexing, search, snippets, or source retrieval: `fts/tests/`.
- XML or APS rendering: the relevant Rust crate tests and container integration
  check below.

Describe the behavior changed and the validation performed in the pull request.
Keep publication metadata static and sufficient for serving. Search and source
retrieval must not depend on live PatRecords calls.

## Container integration

These checks run the real renderer, Parquet projection, and Quickwit from empty
disk. They verify release publication, search, source-frame integrity, and
idempotent replay using file-backed Blob transport. No Azure account is needed.

Build the ingestion image from the repository root:

```sh
docker build --platform linux/amd64 -f ingestion-worker/Dockerfile -t patpub-ingestion .
```

Run the XML check:

```sh
docker run --rm --entrypoint /opt/ingestion-venv/bin/python \
  -v "$PWD/tests:/tests:ro" -v "$PWD/rust-worker/fixtures:/fixtures:ro" \
  patpub-ingestion /tests/bootstrap_integration.py
```

Run the APS check:

```sh
docker run --rm --entrypoint /opt/ingestion-venv/bin/python \
  -v "$PWD/tests:/tests:ro" \
  patpub-ingestion /tests/bootstrap_integration.py --aps
```

## Contribution license

By intentionally submitting a contribution for inclusion, you agree that it is
submitted under Apache License 2.0, as described in section 5 of that license.
Submit only code and data that you have the right to license.
