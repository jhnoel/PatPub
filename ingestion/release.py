"""Immutable release publication and atomic active-release selection.

The downloader stops at the immutable raw archive.  This module owns the
downstream publication boundary: validated source generations and search
artifacts are bound in one manifest and ``current.json`` is changed
with compare-and-swap semantics.  The small storage protocol keeps the logic
testable against :class:`LocalBlobStore` while the Azure adapter can use
``azure-storage-blob`` with ``DefaultAzureCredential`` in production.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from typing import Any, Iterator, Mapping, Protocol

from .models import canonical_json
from .store import LocalBlobStore


class ReleaseError(RuntimeError):
    """Raised when an artifact or release invariant is violated."""


MIXED_RENDERER_PROFILE = "generation-bound-renderer-profiles-v1"


class ReleaseStore(Protocol):
    def exists(self, key: str) -> bool: ...

    def read(self, key: str) -> bytes: ...

    def iter_chunks(self, key: str) -> Iterator[bytes]: ...

    def write_json_if_absent(self, key: str, value: Any) -> bool: ...

    def compare_and_swap_json(
        self, key: str, expected_sha256: str | None, value: Any
    ) -> bool: ...


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _artifact(value: Mapping[str, Any], name: str) -> dict[str, Any]:
    required = {"key", "sha256", "bytes"}
    missing = required - value.keys()
    if missing:
        raise ReleaseError(f"{name} is missing {', '.join(sorted(missing))}")
    key = str(value["key"])
    digest = str(value["sha256"])
    size = int(value["bytes"])
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise ReleaseError(f"{name} has an unsafe object key")
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ReleaseError(f"{name} must use a lowercase SHA-256 digest")
    if size < 0:
        raise ReleaseError(f"{name} has a negative byte count")
    return {"key": key, "sha256": digest, "bytes": size}


def _iter_artifacts(value: Any, path: str):
    if isinstance(value, Mapping):
        if {"key", "sha256", "bytes"} <= value.keys():
            yield path, value
        else:
            for key, child in value.items():
                yield from _iter_artifacts(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_artifacts(child, f"{path}[{index}]")


def _digest_identity(value: Any, name: str) -> str:
    text = str(value or "")
    if not text.startswith("sha256:"):
        raise ReleaseError(f"{name} must use sha256:<digest> identity")
    digest = text[len("sha256:") :]
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ReleaseError(f"{name} must use a lowercase SHA-256 digest")
    return text


@dataclass(frozen=True)
class ReleaseManifest:
    """The immutable, cross-product release contract."""

    release_id: str
    publication_count: int
    source_generations: tuple[str, ...]
    canonical_text_profile: str
    fulltext: Mapping[str, Any]
    source: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    dense: Mapping[str, Any] = field(default_factory=lambda: {"enabled": False})
    schema_version: int = 1

    def as_dict(self) -> dict[str, Any]:
        if not self.release_id or "/" in self.release_id:
            raise ReleaseError("release_id must be a path-safe non-empty value")
        if self.publication_count < 0:
            raise ReleaseError("publication_count cannot be negative")
        if not self.source_generations:
            raise ReleaseError("at least one source generation is required")
        if not self.canonical_text_profile:
            raise ReleaseError("canonical_text_profile is required")
        fulltext = dict(self.fulltext)
        source = dict(self.source)
        metadata = dict(self.metadata)
        dense = dict(self.dense)
        for name, section in (("source", source), ("fulltext", fulltext), ("metadata", metadata)):
            for key, value in section.items():
                if isinstance(value, Mapping) and {"key", "sha256", "bytes"} <= value.keys():
                    section[key] = _artifact(value, f"{name}.{key}")
        return {
            "schema_version": self.schema_version,
            "release_id": self.release_id,
            "publication_count": self.publication_count,
            "source_generations": list(self.source_generations),
            "canonical_text_profile": self.canonical_text_profile,
            "source": source,
            "fulltext": fulltext,
            "dense": dense,
            "metadata": metadata,
        }

    @property
    def digest(self) -> str:
        return _sha256(canonical_json(self.as_dict()))


class ReleasePublisher:
    """Publish immutable release artifacts and validate their reachability.

    ``LocalBlobStore`` is used in unit tests.  Azure production code should
    expose the same methods using a Blob SDK client authenticated by managed
    identity.  The manifest itself is content-addressed by release ID and is
    never overwritten.  The active selector is intentionally the only mutable
    object.
    """

    def __init__(self, store: ReleaseStore):
        self.store = store

    def _check_artifact(self, artifact: Mapping[str, Any], name: str) -> None:
        normalized = _artifact(artifact, name)
        reuse = getattr(self.store, "verified_artifact", None)
        if reuse is not None and reuse(normalized):
            return
        if not self.store.exists(normalized["key"]):
            raise ReleaseError(f"{name} object is missing: {normalized['key']}")
        digest = hashlib.sha256()
        actual_size = 0
        for chunk in self.store.iter_chunks(normalized["key"]):
            digest.update(chunk)
            actual_size += len(chunk)
        actual = digest.hexdigest()
        if actual != normalized["sha256"]:
            raise ReleaseError(
                f"{name} hash mismatch: expected {normalized['sha256']}, got {actual}"
            )
        if actual_size != normalized["bytes"]:
            raise ReleaseError(
                f"{name} size mismatch: expected {normalized['bytes']}, got {actual_size}"
            )

    def validate(self, manifest: ReleaseManifest) -> None:
        payload = manifest.as_dict()
        source = payload["source"]
        fulltext = payload["fulltext"]
        metadata = payload["metadata"]
        if not isinstance(source.get("source_pack"), Mapping) and not isinstance(
            source.get("source_packs"), list
        ):
            raise ReleaseError("source must reference source_pack or source_packs")
        shards = fulltext.get("shards")
        snapshots = metadata.get("snapshots") if isinstance(metadata, Mapping) else None
        durable_index = shards is None and isinstance(snapshots, list)
        if shards is None and snapshots is None:
            shards = [fulltext]
            snapshots = [metadata]
            cumulative = False
        elif durable_index:
            shards = [fulltext]
            cumulative = True
        elif not isinstance(shards, list) or not isinstance(snapshots, list):
            raise ReleaseError("incremental release requires fulltext.shards and metadata.snapshots")
        else:
            cumulative = True
        if not shards or not snapshots:
            raise ReleaseError("release requires at least one search shard and metadata snapshot")
        for shard in shards:
            if not isinstance(shard, Mapping) or not str(shard.get("index_id") or ""):
                raise ReleaseError("fulltext.index_id is required")
            if not isinstance(shard.get("metastore"), Mapping):
                raise ReleaseError("fulltext.metastore is required")
            if not isinstance(shard.get("splits"), list) or not shard["splits"]:
                raise ReleaseError("fulltext.splits must contain at least one artifact")
        for snapshot in snapshots:
            parquet = snapshot.get("parquet") if isinstance(snapshot, Mapping) else None
            if not isinstance(parquet, Mapping) or not isinstance(parquet.get("manifest"), Mapping):
                raise ReleaseError("metadata.parquet.manifest is required")
            if not isinstance(parquet.get("parts"), list) or not parquet["parts"]:
                raise ReleaseError("metadata.parquet.parts must contain at least one artifact")
        for section_name in ("source", "fulltext", "metadata"):
            section = payload[section_name]
            if section_name == "source":
                # Older manifests recorded NDJSON render digests. The ZIP is
                # retained for replay, so these legacy references are no
                # longer a release availability dependency.
                section = {key: value for key, value in section.items() if key != "projections"}
            for name, value in _iter_artifacts(section, section_name):
                self._check_artifact(value, name)
        selected_count = 0
        superseded = source.get("superseded_generation_ids", [])
        if not isinstance(superseded, list) or len(set(superseded)) != len(superseded):
            raise ReleaseError("superseded_generation_ids must be a unique list")
        if not set(superseded) < set(payload["source_generations"]):
            raise ReleaseError("superseded generations must be a proper subset")
        selections = source.get("selected_publication_uids", {})
        if not isinstance(selections, dict) or not set(selections) <= (set(payload["source_generations"]) - set(superseded)):
            raise ReleaseError("selected publication inventory must bind active generations")
        if selections:
            from .search_query import generation_filter
            try:
                generation_filter(set(payload["source_generations"]) - set(superseded), selections)
            except ValueError as exc:
                raise ReleaseError(str(exc)) from exc
        generation_counts: dict[str, int] = {}
        active_source_identities: set[str] = set()
        for generation in payload["source_generations"]:
            _digest_identity(generation, "source_generations")

        source_packs = source.get("source_packs")
        if source_packs is None:
            source_packs = [source["source_pack"]]
        if not source_packs:
            raise ReleaseError("source_packs must contain at least one artifact")
        pack_identities = {
            (str(item.get("key")), str(item.get("sha256")), int(item.get("bytes", -1)))
            for item in source_packs
            if isinstance(item, Mapping)
        }
        generation_refs = source.get("generation_manifests")
        if generation_refs is None:
            if len(payload["source_generations"]) != 1:
                raise ReleaseError(
                    "multiple source generations require source.generation_manifests"
                )
            generation_refs = [
                {
                    "generation_id": payload["source_generations"][0],
                    "artifact": source.get("generation_manifest"),
                }
            ]
        elif isinstance(generation_refs, Mapping):
            generation_refs = [
                {"generation_id": generation, "artifact": artifact}
                for generation, artifact in generation_refs.items()
            ]
        if not isinstance(generation_refs, list):
            raise ReleaseError("source.generation_manifests must be a list or mapping")
        refs_by_generation: dict[str, Mapping[str, Any]] = {}
        for item in generation_refs:
            if not isinstance(item, Mapping) or not isinstance(item.get("artifact"), Mapping):
                raise ReleaseError("source.generation_manifests has an invalid entry")
            generation = _digest_identity(item.get("generation_id"), "generation_manifest.generation_id")
            if generation in refs_by_generation:
                raise ReleaseError("duplicate source generation manifest")
            refs_by_generation[generation] = item["artifact"]
        if set(refs_by_generation) != set(payload["source_generations"]):
            raise ReleaseError("source generation manifests do not match source_generations")
        renderer_profiles = source.get("renderer_profiles")
        if renderer_profiles is not None:
            if not isinstance(renderer_profiles, Mapping) or set(renderer_profiles) != set(payload["source_generations"]):
                raise ReleaseError("renderer_profiles must bind every source generation")
            if any(not isinstance(profile, str) or not profile for profile in renderer_profiles.values()):
                raise ReleaseError("renderer_profiles must contain non-empty profiles")
            profiles = set(renderer_profiles.values())
            expected_profile = next(iter(profiles)) if len(profiles) == 1 else MIXED_RENDERER_PROFILE
            if manifest.canonical_text_profile != expected_profile:
                raise ReleaseError("canonical_text_profile does not match generation profiles")
        elif manifest.canonical_text_profile == MIXED_RENDERER_PROFILE:
            raise ReleaseError("mixed renderer release requires renderer_profiles")
        projection_refs = source.get("projections", [])
        if projection_refs:
            if not isinstance(projection_refs, list):
                raise ReleaseError("source.projections must be a list")
            projection_ids = [str(item.get("generation_id") or "") for item in projection_refs]
            if len(projection_ids) != len(set(projection_ids)) or set(projection_ids) != set(payload["source_generations"]):
                raise ReleaseError("source.projections must bind every source generation once")
            for index, item in enumerate(projection_refs):
                if not isinstance(item.get("artifact"), Mapping):
                    raise ReleaseError(f"source.projections[{index}] has no artifact")
        for generation in payload["source_generations"]:
            generation_artifact = refs_by_generation[generation]
            self._check_artifact(generation_artifact, f"source.generation_manifest[{generation}]")
            generation_payload = json.loads(self.store.read(str(generation_artifact["key"])))
            if not isinstance(generation_payload, Mapping):
                raise ReleaseError("source generation manifest must be a JSON object")
            if generation_payload.get("generation_id") != generation:
                raise ReleaseError("source generation manifest identity mismatch")
            if renderer_profiles is not None and generation_payload.get("renderer_profile") != renderer_profiles[generation]:
                raise ReleaseError("source generation renderer profile mismatch")
            generation_count = int(generation_payload.get("publication_count", -1))
            if generation_count < 1:
                raise ReleaseError("source generation publication count must be positive")
            if generation in selections:
                if len(selections[generation]) > generation_count:
                    raise ReleaseError("selected publication count exceeds generation count")
                generation_count = len(selections[generation])
            generation_counts[generation] = generation_count
            source_identity = generation_payload.get("source_identity")
            if generation not in superseded and generation not in selections and source_identity:
                if source_identity in active_source_identities:
                    raise ReleaseError("multiple active generations claim the same source archive")
                active_source_identities.add(str(source_identity))
            if generation not in superseded:
                selected_count += generation_count
            generation_packs = generation_payload.get("source_packs")
            if not isinstance(generation_packs, list) or not generation_packs:
                raise ReleaseError("source generation has no source packs")
            generation_pack_identities = set()
            for index, item in enumerate(generation_packs):
                normalized = _artifact(item, f"source generation source_packs[{index}]")
                generation_pack_identities.add(
                    (normalized["key"], normalized["sha256"], normalized["bytes"])
                )
            if not generation_pack_identities <= pack_identities:
                raise ReleaseError("source generation does not bind its source packs")

        if selected_count != manifest.publication_count:
            raise ReleaseError("selected source generation publication count mismatch")

        if cumulative:
            active_generations = set(payload["source_generations"]) - set(superseded)
            groups = [(snapshots, "metadata.snapshots")]
            if not durable_index:
                groups.insert(0, (shards, "fulltext.shards"))
            for group, name in groups:
                ids = [str(item.get("generation_id") or "") for item in group]
                if len(ids) != len(set(ids)) or set(ids) != active_generations:
                    raise ReleaseError(f"{name} must bind exactly the active source generations")
            if not durable_index and sum(int(item.get("publication_count", -1)) for item in shards) != manifest.publication_count:
                raise ReleaseError("search shard publication count mismatch")
            if sum(int(item.get("publication_count", -1)) for item in snapshots) != manifest.publication_count:
                raise ReleaseError("metadata snapshot publication count mismatch")
            for group in ((snapshots,) if durable_index else (shards, snapshots)):
                for item in group:
                    if int(item.get("publication_count", -1)) != generation_counts[item["generation_id"]]:
                        raise ReleaseError("generation artifact publication count mismatch")
        for snapshot in snapshots:
            parquet = snapshot["parquet"]
            self._validate_parquet(parquet, int(snapshot.get("publication_count", manifest.publication_count)))
        for shard in shards:
            self._validate_shard(shard, int(shard.get("publication_count", manifest.publication_count)))

    def _validate_parquet(self, parquet: Mapping[str, Any], expected_count: int) -> None:
        metadata_manifest = json.loads(self.store.read(str(parquet["manifest"]["key"])))
        if not isinstance(metadata_manifest, Mapping) or metadata_manifest.get("schema_version") != 1:
            raise ReleaseError("metadata manifest has an unsupported schema")
        if int(metadata_manifest.get("publication_count", -1)) != expected_count:
            raise ReleaseError("metadata publication count mismatch")
        manifest_parts = metadata_manifest.get("parts")
        if not isinstance(manifest_parts, list) or not manifest_parts:
            raise ReleaseError("metadata manifest has no parts")
        release_part_identities = {
            (str(item.get("key")), str(item.get("sha256")), int(item.get("bytes", -1)))
            for item in parquet["parts"]
            if isinstance(item, Mapping)
        }
        manifest_part_identities = set()
        rows_by_projection: dict[str, int] = {}
        for index, item in enumerate(manifest_parts):
            normalized = _artifact(item, f"metadata manifest parts[{index}]")
            manifest_part_identities.add(
                (normalized["key"], normalized["sha256"], normalized["bytes"])
            )
            if "rows" in item:
                part_rows = int(item["rows"])
                if part_rows < 0:
                    raise ReleaseError("metadata manifest has a negative row count")
                projection = str(item.get("projection") or "all")
                rows_by_projection[projection] = rows_by_projection.get(projection, 0) + part_rows
        if rows_by_projection:
            if any(value != expected_count for value in rows_by_projection.values()):
                raise ReleaseError("metadata manifest row count mismatch")
        if not manifest_part_identities <= release_part_identities:
            raise ReleaseError("metadata manifest parts are not bound by the release")

    def _validate_shard(self, fulltext: Mapping[str, Any], expected_count: int) -> None:
        metastore = json.loads(self.store.read(str(fulltext["metastore"]["key"])))
        if not isinstance(metastore, Mapping):
            raise ReleaseError("Quickwit metastore must be a JSON object")
        index_config = metastore.get("index", {}).get("index_config", {})
        if index_config.get("index_id") != fulltext["index_id"]:
            raise ReleaseError("Quickwit metastore index_id mismatch")
        split_names = {
            str(item["key"]).rsplit("/", 1)[-1]
            for item in fulltext["splits"]
            if isinstance(item, Mapping)
        }
        metastore_splits = metastore.get("splits")
        if not isinstance(metastore_splits, list) or not metastore_splits:
            raise ReleaseError("Quickwit metastore has no published splits")
        observed_split_names = {
            f"{item.get('split_id')}.split"
            for item in metastore_splits
            if isinstance(item, Mapping) and item.get("split_id")
        }
        if not observed_split_names <= split_names:
            raise ReleaseError("Quickwit metastore contains an unbound split")
        # Delete tasks make the physical split count larger than the live
        # document count until compaction. The index worker records the live
        # count after querying Quickwit once deletes and ingestion commit.
        physical_docs = sum(int(item.get("num_docs", 0)) for item in metastore_splits)
        live_docs = int(fulltext.get("document_count", physical_docs))
        if live_docs != expected_count:
            raise ReleaseError("Quickwit live document count mismatch")

    def publish(self, manifest: ReleaseManifest) -> dict[str, Any]:
        self.validate(manifest)
        payload = manifest.as_dict()
        manifest_key = f"releases/{manifest.release_id}.json"
        if self.store.exists(manifest_key):
            existing = json.loads(self.store.read(manifest_key))
            if existing != payload:
                raise ReleaseError(f"release already exists with different contents: {manifest.release_id}")
        elif not self.store.write_json_if_absent(manifest_key, payload):
            existing = json.loads(self.store.read(manifest_key))
            if existing != payload:
                raise ReleaseError(f"release raced with different contents: {manifest.release_id}")
        return payload

    def selector_payload(self, manifest: ReleaseManifest) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "release_id": manifest.release_id,
            "manifest_key": f"releases/{manifest.release_id}.json",
            "manifest_sha256": _sha256(canonical_json(manifest.as_dict())),
        }

    def activate(
        self,
        manifest: ReleaseManifest,
        *,
        expected_selector_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Atomically make a validated manifest the active release.

        Callers must pass the current selector digest when replacing an active
        release.  A stale writer receives ``ReleaseError`` instead of silently
        rolling the system back.
        """

        self.publish(manifest)
        selector = self.selector_payload(manifest)
        key = "releases/current.json"
        if not self.store.compare_and_swap_json(key, expected_selector_sha256, selector):
            raise ReleaseError("active release changed concurrently")
        return selector

    def verify_selector(
        self, selector: Mapping[str, Any], *, validate_artifacts: bool = True
    ) -> ReleaseManifest:
        release_id = str(selector.get("release_id") or "")
        key = str(selector.get("manifest_key") or f"releases/{release_id}.json")
        if not release_id or key != f"releases/{release_id}.json":
            raise ReleaseError("invalid active release selector")
        if not self.store.exists(key):
            raise ReleaseError(f"active release manifest is missing: {key}")
        payload = json.loads(self.store.read(key))
        if payload.get("release_id") != release_id:
            raise ReleaseError("active release selector and manifest release_id differ")
        # The selector digest is over the manifest without a self-referential
        # digest field.  Older manifests may carry ``manifest_sha256`` for
        # observability, so ignore it when recomputing the canonical digest.
        digest_payload = dict(payload)
        digest_payload.pop("manifest_sha256", None)
        digest = _sha256(canonical_json(digest_payload))
        if digest != selector.get("manifest_sha256"):
            raise ReleaseError("active release manifest digest mismatch")
        manifest = ReleaseManifest(
            release_id=payload["release_id"],
            publication_count=int(payload["publication_count"]),
            source_generations=tuple(payload["source_generations"]),
            canonical_text_profile=payload["canonical_text_profile"],
            source=payload.get("source", {}),
            fulltext=payload["fulltext"],
            metadata=payload.get("metadata", {}),
            dense=payload.get("dense", {"enabled": False}),
            schema_version=int(payload.get("schema_version", 1)),
        )
        if validate_artifacts:
            self.validate(manifest)
        return manifest


class AzureBlobReleaseStore:
    """Managed-identity Blob implementation of the release store protocol.

    The Azure SDK imports are lazy so the credential-free unit-test package
    does not need Azure dependencies.  Deployments install
    ``azure-identity`` and ``azure-storage-blob`` and grant the worker
    ``Storage Blob Data Contributor`` on the staging containers plus
    ``Storage Blob Data Owner`` only where it must advance ``current.json``.
    """

    def __init__(
        self,
        account_url: str,
        container: str,
        *,
        allowed_containers: tuple[str, ...] = (
            "releases",
            "raw",
            "source",
            "metadata",
            "quickwit",
            "canonical",
        ),
        quickwit_account_url: str | None = None,
    ):
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:  # pragma: no cover - deployment-only path
            raise RuntimeError(
                "install azure-identity and azure-storage-blob for Azure publishing"
            ) from exc
        self._service = BlobServiceClient(account_url, credential=DefaultAzureCredential())
        names = set(allowed_containers) | {container}
        self._containers = {
            name: self._service.get_container_client(name) for name in names
        }
        if quickwit_account_url is None and os.environ.get("QW_AZURE_STORAGE_ACCOUNT"):
            quickwit_account_url = (
                f"https://{os.environ['QW_AZURE_STORAGE_ACCOUNT']}.blob.core.windows.net"
            )
        if quickwit_account_url:
            self._containers["quickwit"] = BlobServiceClient(
                quickwit_account_url, credential=DefaultAzureCredential()
            ).get_container_client("quickwit")
        self._default_container = container

    def _blob(self, key: str):
        if not key or key.startswith("/") or ".." in key.split("/"):
            raise ReleaseError("unsafe Blob key")
        parts = key.split("/", 1)
        if len(parts) == 2 and parts[0] in self._containers:
            container = parts[0]
            # Release objects retain the ``releases/`` namespace inside the
            # releases container so the logical key is also the physical
            # immutable object name.  Other namespaces use the first path
            # component as the container and strip it from the Blob name.
            blob_key = key if container == self._default_container else parts[1]
        else:
            container, blob_key = self._default_container, key
        if not blob_key:
            raise ReleaseError("empty Blob key")
        return self._containers[container].get_blob_client(blob_key)

    def verified_artifact(self, artifact: Mapping[str, Any]) -> bool:
        """Reuse a completed full-graph hash only for the identical Blob version."""
        cache_key = os.environ.get("ARTIFACT_VERSION_CACHE_KEY", "")
        if not cache_key:
            return False
        if not hasattr(self, "_version_cache"):
            cache = json.loads(self.read(cache_key))
            graph = json.loads(self.read("migration/v1/full-release-graph-validation.json"))
            candidate = json.loads(self.read("migration/v1/full-release-candidate.json"))
            if (cache.get("release_id") != graph.get("release_id") or
                cache.get("manifest_sha256") != candidate["manifest"]["sha256"] or
                graph.get("status") != "graph-validated-pending-search-canary"):
                raise ReleaseError("artifact version cache has no matching completed graph proof")
            from datetime import datetime
            captured = datetime.fromisoformat(cache["capture_finished_utc"])
            completed = self._blob("migration/v1/full-release-graph-validation.json").get_blob_properties().last_modified
            if captured >= completed:
                raise ReleaseError("artifact versions were captured after graph validation")
            self._version_cache = cache["versions"]
        cached = self._version_cache.get(artifact["key"])
        if not cached or any(cached.get(k) != artifact[k] for k in ("key", "sha256", "bytes")):
            return False
        if os.environ.get("BATCH_ETAG_VALIDATION", "false").lower() == "true":
            if not hasattr(self, "_current_versions"):
                # Azure listings return sizes and ETags without downloading payloads.
                # A sole guarded writer owns this release; unchanged versions reuse
                # the completed full graph hashes, while new versions are hashed.
                from concurrent.futures import ThreadPoolExecutor
                containers = {key.split("/", 1)[0] if key.split("/", 1)[0] in self._containers
                              else self._default_container for key in self._version_cache}
                def inventory(name):
                    return name, {item.name: (item.size, str(item.etag))
                                  for item in self._containers[name].list_blobs()}
                with ThreadPoolExecutor(max_workers=4) as pool:
                    self._current_versions = dict(pool.map(inventory, sorted(containers)))
            parts = artifact["key"].split("/", 1)
            container = parts[0] if len(parts) == 2 and parts[0] in self._containers else self._default_container
            key = artifact["key"] if container == self._default_container else parts[1]
            current = self._current_versions.get(container, {}).get(key)
            return (current is not None and current[0] == artifact["bytes"]
                    and current[1].strip('"') == str(cached.get("etag")).strip('"'))
        current = self._blob(artifact["key"]).get_blob_properties()
        return current.size == artifact["bytes"] and str(current.etag) == cached.get("etag")

    def exists(self, key: str) -> bool:  # pragma: no cover - deployment-only path
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._blob(key).get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def read(self, key: str) -> bytes:  # pragma: no cover - deployment-only path
        cacheable = (os.environ.get("CACHE_IMMUTABLE_JSON", "false").lower() == "true"
                     and key.endswith(".json") and not key.endswith("/current.json"))
        if cacheable:
            if not hasattr(self, "_json_cache"):
                self._json_cache = {}
            if key in self._json_cache:
                return self._json_cache[key]
        data = self._blob(key).download_blob().readall()
        if cacheable:
            self._json_cache[key] = data
        return data

    def prefetch_json(self, keys) -> None:
        if os.environ.get("CACHE_IMMUTABLE_JSON", "false").lower() != "true":
            return
        from concurrent.futures import ThreadPoolExecutor
        keys = sorted({key for key in keys if key.endswith(".json") and not key.endswith("/current.json")})
        with ThreadPoolExecutor(max_workers=16) as pool:
            for _ in pool.map(self.read, keys):
                pass

    def iter_chunks(self, key: str) -> Iterator[bytes]:  # pragma: no cover
        yield from self._blob(key).download_blob(max_concurrency=4).chunks()

    def write_json_if_absent(self, key: str, value: Any) -> bool:  # pragma: no cover
        from azure.core.exceptions import ResourceExistsError

        try:
            self._blob(key).upload_blob(
                canonical_json(value), overwrite=False, if_none_match="*"
            )
            return True
        except ResourceExistsError:
            return False

    def compare_and_swap_json(
        self, key: str, expected_sha256: str | None, value: Any
    ) -> bool:  # pragma: no cover
        from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError
        from azure.core import MatchConditions

        blob = self._blob(key)
        try:
            properties = blob.get_blob_properties()
        except ResourceNotFoundError:
            if expected_sha256 is not None:
                return False
            try:
                blob.upload_blob(canonical_json(value), overwrite=False, if_none_match="*")
                return True
            except ResourceModifiedError:
                return False
        if expected_sha256 is None:
            return False
        current = blob.download_blob().readall()
        if _sha256(current) != expected_sha256:
            return False
        try:
            blob.upload_blob(
                canonical_json(value),
                overwrite=True,
                etag=properties.etag,
                match_condition=MatchConditions.IfNotModified,
            )
            return True
        except ResourceModifiedError:
            return False


def artifact_from_store(store: LocalBlobStore, key: str) -> dict[str, Any]:
    """Create a checked artifact reference for a local or test store."""

    data = store.read(key)
    return {"key": key, "sha256": _sha256(data), "bytes": len(data)}
