"""Deterministic downstream release orchestration.

The ingestion worker uses these deterministic records for generation and
release publication. The local store exercises the same immutable-object
operations as the production store.

There are two stages:

``generation``
    Verify source-pack dependencies and publish a content-addressed
    generation manifest.

``release``
    Verify the generation, Quickwit, and Parquet artifacts; run a canary;
    publish the immutable release manifest; then compare-and-swap the active
    selector.

Events contain no wall-clock fields.  Their IDs are derived from a canonical
payload, so retries produce the same event ID.  Stage
results are immutable records keyed by that ID and are returned unchanged on
duplicate delivery.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Callable, Mapping

from .models import canonical_json
from .release import ReleaseError, ReleaseManifest, ReleasePublisher
from .store import LocalBlobStore


class OrchestrationError(RuntimeError):
    """Raised when an event or one of its dependencies is invalid."""


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _safe_key(value: Any, name: str) -> str:
    key = str(value or "")
    if not key or key.startswith("/") or ".." in key.split("/"):
        raise OrchestrationError(f"{name} has an unsafe object key")
    return key


def _digest_value(value: Any, name: str) -> str:
    text = str(value or "")
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise OrchestrationError(f"{name} must be a lowercase SHA-256 digest")
    return text


def _artifact(value: Mapping[str, Any], name: str) -> dict[str, Any]:
    try:
        key = _safe_key(value["key"], f"{name}.key")
        sha256 = _digest_value(value["sha256"], f"{name}.sha256")
        size = int(value["bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise OrchestrationError(f"{name} is not an artifact reference") from exc
    if size < 0:
        raise OrchestrationError(f"{name}.bytes cannot be negative")
    return {"key": key, "sha256": sha256, "bytes": size}


@dataclass(frozen=True)
class PipelineEvent:
    """Immutable stage record with a deterministic identity.

    ``attempt`` is metadata for observability and is intentionally excluded
    from the event identity.  A retry therefore claims the same durable
    stage record instead of creating a second release.
    """

    stage: str
    payload: Mapping[str, Any]
    attempt: int = 0

    @property
    def fingerprint(self) -> str:
        return _digest({"schema_version": 1, "stage": self.stage, "payload": self.payload})

    @property
    def event_id(self) -> str:
        return f"{self.stage}-{self.fingerprint[:32]}"

    def as_dict(self) -> dict[str, Any]:
        if self.stage not in {"render", "metadata", "index", "generation", "release"}:
            raise OrchestrationError("unsupported pipeline stage")
        if self.attempt < 0:
            raise OrchestrationError("attempt cannot be negative")
        if not isinstance(self.payload, Mapping) or not self.payload:
            raise OrchestrationError("event payload cannot be empty")
        return {
            "schema_version": 1,
            "event_id": self.event_id,
            "event_fingerprint": self.fingerprint,
            "stage": self.stage,
            "attempt": self.attempt,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PipelineEvent":
        if int(value.get("schema_version", 0)) != 1:
            raise OrchestrationError("unsupported event schema_version")
        event = cls(
            stage=str(value.get("stage") or ""),
            payload=value.get("payload") or {},
            attempt=int(value.get("attempt", 0)),
        )
        if value.get("event_id") != event.event_id or value.get("event_fingerprint") != event.fingerprint:
            raise OrchestrationError("event identity does not match its canonical payload")
        return event


Canary = Callable[[Mapping[str, Any]], bool]


class DownstreamOrchestrator:
    """Execute idempotent generation and release stages against a Blob store."""

    def __init__(self, store: LocalBlobStore):
        self.store = store
        self.publisher = ReleasePublisher(store)

    def _read_json(self, key: str) -> dict[str, Any]:
        value = json.loads(self.store.read(key))
        if not isinstance(value, dict):
            raise OrchestrationError(f"{key} is not a JSON object")
        return value

    def _completed(self, event: PipelineEvent) -> dict[str, Any] | None:
        key = f"events/completed/{event.event_id}.json"
        if not self.store.exists(key):
            return None
        completed = self._read_json(key)
        if (
            completed.get("event_fingerprint") != event.fingerprint
            or completed.get("status") != "completed"
        ):
            raise OrchestrationError(f"immutable completion conflict: {event.event_id}")
        result = completed.get("result")
        if not isinstance(result, dict):
            raise OrchestrationError(f"completion record has no result: {event.event_id}")
        return result

    def _claim(self, event: PipelineEvent) -> None:
        key = f"events/{event.event_id}.json"
        payload = event.as_dict()
        if self.store.write_json_if_absent(key, payload):
            return
        existing = self._read_json(key)
        # Attempt is delivery metadata, not part of the immutable work identity.
        existing.pop("attempt", None)
        payload.pop("attempt", None)
        if existing != payload:
            raise OrchestrationError(f"immutable event conflict: {event.event_id}")

    def _complete(self, event: PipelineEvent, result: Mapping[str, Any]) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "event_id": event.event_id,
            "event_fingerprint": event.fingerprint,
            "status": "completed",
            "result": dict(result),
        }
        key = f"events/completed/{event.event_id}.json"
        if not self.store.write_json_if_absent(key, value):
            existing = self._read_json(key)
            if existing != value:
                raise OrchestrationError(f"immutable completion conflict: {event.event_id}")
            return existing["result"]
        return dict(result)

    def _artifact(self, reference: Mapping[str, Any], name: str) -> dict[str, Any]:
        artifact = _artifact(reference, name)
        reuse = getattr(self.store, "verified_artifact", None)
        if reuse is not None and reuse(artifact):
            return artifact
        if not self.store.exists(artifact["key"]):
            raise OrchestrationError(f"{name} dependency is missing: {artifact['key']}")
        digest = hashlib.sha256()
        size = 0
        for chunk in self.store.iter_chunks(artifact["key"]):
            digest.update(chunk)
            size += len(chunk)
        if size != artifact["bytes"]:
            raise OrchestrationError(f"{name} byte count mismatch")
        if digest.hexdigest() != artifact["sha256"]:
            raise OrchestrationError(f"{name} digest mismatch")
        return artifact

    def run_generation(self, event: PipelineEvent) -> dict[str, Any]:
        if event.stage != "generation":
            raise OrchestrationError("run_generation requires a generation event")
        if (completed := self._completed(event)) is not None:
            return completed
        self._claim(event)
        payload = event.payload
        source_packs = payload.get("source_packs")
        if not isinstance(source_packs, list) or not source_packs:
            raise OrchestrationError("generation requires source_packs")
        normalized = [
            self._artifact(item, f"source_packs[{index}]")
            for index, item in enumerate(source_packs)
        ]
        publication_count = int(payload.get("publication_count", -1))
        if publication_count < 1:
            raise OrchestrationError("generation publication_count must be positive")
        renderer_profile = str(payload.get("renderer_profile") or "")
        if not renderer_profile:
            raise OrchestrationError("generation renderer_profile is required")
        identity = {
            "schema_version": 1,
            "publication_count": publication_count,
            "renderer_profile": renderer_profile,
            "source_packs": normalized,
        }
        raw = payload.get("raw")
        if raw is not None:
            raw = self._artifact(raw, "raw")
            generation_id = f"sha256:{_digest({'raw_sha256': raw['sha256'], 'renderer_profile': renderer_profile})}"
            if payload.get("generation_id") != generation_id:
                raise OrchestrationError("generation_id does not match raw and renderer profile")
            identity["raw"] = raw
            source_identity = str(payload.get("source_identity") or "")
            if len(source_identity) != 64 or any(c not in "0123456789abcdef" for c in source_identity):
                raise OrchestrationError("generation source_identity must be a SHA-256 digest")
            identity["source_identity"] = source_identity
        else:
            generation_id = f"sha256:{_digest(identity)}"
        manifest = identity | {
            "generation_id": generation_id,
            "source_packs": normalized,
        }
        manifest_key = f"source/v1/generations/{generation_id[7:]}.json"
        if not self.store.write_json_if_absent(manifest_key, manifest):
            if self._read_json(manifest_key) != manifest:
                raise OrchestrationError("generation manifest immutable conflict")
        manifest_bytes = self.store.read(manifest_key)
        return self._complete(
            event,
            {
                "generation_id": generation_id,
                "manifest": {
                    "key": manifest_key,
                    "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                    "bytes": len(manifest_bytes),
                },
                "publication_count": publication_count,
            },
        )

    def run_release(self, event: PipelineEvent, canary: Canary) -> dict[str, Any]:
        if event.stage != "release":
            raise OrchestrationError("run_release requires a release event")
        if (completed := self._completed(event)) is not None:
            return completed
        self._claim(event)
        payload = event.payload
        expected_selector = payload.get("expected_selector_sha256", "unspecified")
        if expected_selector != "unspecified":
            actual_selector = (
                hashlib.sha256(self.store.read("releases/current.json")).hexdigest()
                if self.store.exists("releases/current.json") else None
            )
            if actual_selector != expected_selector:
                raise OrchestrationError("release predecessor selector changed")
        generation_payload = payload.get("generation_manifests")
        if generation_payload is None:
            generation_payload = [
                {"generation_id": None, "artifact": payload.get("generation_manifest")}
            ]
        elif isinstance(generation_payload, Mapping):
            generation_payload = [
                {"generation_id": generation_id, "artifact": artifact}
                for generation_id, artifact in generation_payload.items()
            ]
        if not isinstance(generation_payload, list) or not generation_payload:
            raise OrchestrationError("release requires generation_manifests")

        generation_refs: list[dict[str, Any]] = []
        generations: list[dict[str, Any]] = []
        seen_generations: set[str] = set()
        for index, item in enumerate(generation_payload):
            if not isinstance(item, Mapping):
                raise OrchestrationError(f"generation_manifests[{index}] is invalid")
            if "key" in item and "artifact" not in item:
                item = {"generation_id": None, "artifact": item}
            generation_ref = self._artifact(
                item.get("artifact") or {}, f"generation_manifests[{index}]"
            )
            generation = self._read_json(generation_ref["key"])
            generation_id = str(generation.get("generation_id") or "")
            declared_id = item.get("generation_id")
            if declared_id is not None and str(declared_id) != generation_id:
                raise OrchestrationError("generation manifest identity does not match event")
            if not generation_id.startswith("sha256:"):
                raise OrchestrationError("generation manifest has no valid generation_id")
            if generation_id in seen_generations:
                raise OrchestrationError("duplicate generation manifest")
            seen_generations.add(generation_id)
            if int(generation.get("publication_count", -1)) < 1:
                raise OrchestrationError("generation publication_count must be positive")
            generation_refs.append({"generation_id": generation_id, "artifact": generation_ref})
            generations.append(generation)
        generation_ids = tuple(item["generation_id"] for item in generation_refs)
        superseded = payload.get("superseded_generation_ids", [])
        if not isinstance(superseded, list) or len(set(superseded)) != len(superseded):
            raise OrchestrationError("superseded_generation_ids must be a unique list")
        if not set(superseded) < set(generation_ids):
            raise OrchestrationError("superseded generations must be a proper subset")
        # Corrections replace a complete earlier generation. Partial-document
        # corrections require an explicit selected-publication inventory.
        selections = payload.get("selected_publication_uids", {})
        if selections:
            from .search_query import generation_filter
            try:
                generation_filter(set(generation_ids) - set(superseded), selections)
            except ValueError as exc:
                raise OrchestrationError(str(exc)) from exc
        selected_count = sum(
            len(selections[generation["generation_id"]]) if generation["generation_id"] in selections else int(generation["publication_count"])
            for generation in generations
            if generation["generation_id"] not in superseded
        )
        if selected_count != int(payload.get("publication_count", -1)):
            raise OrchestrationError("release publication_count does not match selected generations")
        fulltext = payload.get("fulltext")
        metadata = payload.get("metadata")
        if not isinstance(fulltext, Mapping) or not isinstance(metadata, Mapping):
            raise OrchestrationError("release requires fulltext and metadata sections")
        fulltext_value = dict(fulltext)
        metadata_value = dict(metadata)
        shards = fulltext_value.get("shards")
        if shards is None:
            shards = [fulltext_value]
        if not isinstance(shards, list) or not shards:
            raise OrchestrationError("fulltext.shards is required")
        for shard_index, shard in enumerate(shards):
            self._artifact(shard.get("metastore") or {}, f"fulltext.shards[{shard_index}].metastore")
            splits = shard.get("splits")
            if not isinstance(splits, list) or not splits:
                raise OrchestrationError("fulltext shard splits are required")
            for index, item in enumerate(splits):
                self._artifact(item, f"fulltext.shards[{shard_index}].splits[{index}]")
        snapshots = metadata_value.get("snapshots")
        if snapshots is None:
            snapshots = [metadata_value]
        if not isinstance(snapshots, list) or not snapshots:
            raise OrchestrationError("metadata.snapshots is required")
        for snapshot_index, snapshot in enumerate(snapshots):
            parquet = snapshot.get("parquet")
            if not isinstance(parquet, Mapping):
                raise OrchestrationError("metadata snapshot parquet is required")
            self._artifact(parquet.get("manifest") or {}, f"metadata.snapshots[{snapshot_index}].parquet.manifest")
            parts = parquet.get("parts")
            if not isinstance(parts, list) or not parts:
                raise OrchestrationError("metadata snapshot parts are required")
            for index, item in enumerate(parts):
                self._artifact(item, f"metadata.snapshots[{snapshot_index}].parquet.parts[{index}]")

        # Profiles belong to immutable generations. A cumulative release may
        # keep old source packs while a newer renderer handles future ZIPs.
        renderer_profiles = {
            generation["generation_id"]: str(generation.get("renderer_profile") or "")
            for generation in generations
        }
        if any(not profile for profile in renderer_profiles.values()):
            raise OrchestrationError("generation renderer_profile is required")
        profiles = set(renderer_profiles.values())
        from .release import MIXED_RENDERER_PROFILE
        canonical_profile = next(iter(profiles)) if len(profiles) == 1 else MIXED_RENDERER_PROFILE
        release_identity = {
            "schema_version": 1,
            "source_generations": list(generation_ids),
            "superseded_generation_ids": superseded,
            "publication_count": int(payload["publication_count"]),
            "canonical_text_profile": canonical_profile,
            "fulltext": fulltext_value,
            "metadata": metadata_value,
        }
        if selections:
            release_identity["selected_publication_uids"] = selections
        if not release_identity["canonical_text_profile"]:
            raise OrchestrationError("canonical_text_profile is required")
        if len(profiles) > 1:
            release_identity["renderer_profiles"] = renderer_profiles
        release_id = f"release-{_digest(release_identity)[:24]}"
        source_packs: list[dict[str, Any]] = []
        for generation in generations:
            for item in generation.get("source_packs", []):
                normalized = _artifact(item, "generation source_packs")
                if normalized not in source_packs:
                    source_packs.append(normalized)
        if not source_packs:
            raise OrchestrationError("generation manifests contain no source packs")
        source = {
            "generation_manifests": generation_refs,
            "source_packs": source_packs,
            "superseded_generation_ids": superseded,
        }
        if selections:
            source["selected_publication_uids"] = selections
        if len(profiles) > 1:
            source["renderer_profiles"] = renderer_profiles
        manifest = ReleaseManifest(
            release_id=release_id,
            publication_count=int(payload["publication_count"]),
            source_generations=generation_ids,
            canonical_text_profile=release_identity["canonical_text_profile"],
            source=source,
            fulltext=fulltext_value,
            metadata=metadata_value,
            dense=payload.get("dense", {"enabled": False}),
        )
        # Validate every dependency before invoking the canary. A canary may
        # query the new index and hydrate a hit, but it must never repair an
        # incomplete release.
        self.publisher.validate(manifest)
        if not canary(manifest.as_dict()):
            raise OrchestrationError("release canary rejected candidate")
        selector_sha = None
        selector_key = "releases/current.json"
        if self.store.exists(selector_key):
            selector_sha = hashlib.sha256(self.store.read(selector_key)).hexdigest()
        intended_selector = self.publisher.selector_payload(manifest)
        intent_key = f"events/activation/{event.event_id}.json"
        intent = {
            "event_fingerprint": event.fingerprint,
            "predecessor_sha256": selector_sha,
            "selector": intended_selector,
        }
        if not self.store.write_json_if_absent(intent_key, intent):
            existing_intent = self._read_json(intent_key)
            if existing_intent["event_fingerprint"] != event.fingerprint or existing_intent["selector"] != intended_selector:
                raise OrchestrationError("immutable activation intent conflict")
            intent = existing_intent
        current = self._read_json(selector_key) if self.store.exists(selector_key) else None
        if current == intended_selector:
            selector = intended_selector
        elif selector_sha != intent["predecessor_sha256"]:
            raise OrchestrationError("activation was superseded by another release")
        else:
            try:
                selector = self.publisher.activate(
                    manifest, expected_selector_sha256=intent["predecessor_sha256"]
                )
            except ReleaseError as exc:
                raise OrchestrationError(str(exc)) from exc
        return self._complete(
            event,
            {
                "release_id": release_id,
                "manifest_key": f"releases/{release_id}.json",
                "manifest_sha256": manifest.digest,
                "selector": selector,
            },
        )
