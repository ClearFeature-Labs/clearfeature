"""Immutable registry bundles + deterministic digests.

A served feature value must trace back to an exact registry snapshot (O2 lineage). A
``RegistryBundle`` is that snapshot: a canonical, order-independent digest of the registry's
*definitions* plus bounded metadata. The digest is the identity — two registries that are
logically identical (same entities/sources/views/features, regardless of YAML key order)
produce the same digest, and any definitional change (a feature body, a model digest, a
dependency edge, a lifecycle) changes it. Volatile metadata (``created_at``, ids, author)
never affects the digest.

``fsctl``/promotion/rollback are **not** here — this is the immutability control plane only
.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Protocol

from fintech_feature_platform.fs_core.hashing import canonical_value_json
from fintech_feature_platform.fs_core.registry.models import FeatureDef, Registry


def _canonical_feature(feature: FeatureDef, features_by_name: dict[str, FeatureDef]) -> dict:
    """Definition-only, order-independent view of one feature.

    Uses the normalized ``lifecycle`` (so legacy ``active`` and ``live`` hash identically) and
    the *resolved* dependency version (so an implicit pin and an equal explicit pin match).
    """
    deps = sorted(
        (
            {
                "feature": dep.feature,
                "version": dep.version or features_by_name[dep.feature].feature_version,
                "required": dep.required,
                "max_input_age_seconds": dep.max_input_age_seconds,
                "propagation": dep.propagation,
            }
            for dep in feature.deps
        ),
        key=lambda d: (d["feature"], d["version"]),
    )
    model = None
    if feature.model is not None:
        model = {
            "uri": feature.model.uri,
            "digest": feature.model.digest,
            "output_name": feature.model.output_name,
            "runner": feature.model.runner,
            "batch_only": feature.model.batch_only,
        }
    return {
        "name": feature.name,
        "kind": feature.kind,
        "feature_version": feature.feature_version,
        "dtype": feature.dtype,
        "lifecycle": feature.lifecycle,
        "udf": feature.udf,
        "inputs": sorted(feature.inputs),
        "deps": deps,
        "model": model,
    }


def _canonical_view(view) -> dict:
    features_by_name = {f.name: f for f in view.features}
    return {
        "name": view.name,
        "entity": view.entity,
        # key_fields order is semantic (composite key order) -> kept as declared.
        "key_fields": list(view.key_fields),
        "view_version": view.view_version,
        "owner": view.owner,
        "status": view.status,
        "max_dependency_depth": view.max_dependency_depth,
        "feature_groups": {
            group: sorted(members) for group, members in view.feature_groups.items()
        },
        "features": sorted(
            (_canonical_feature(f, features_by_name) for f in view.features),
            key=lambda d: (d["name"], d["feature_version"]),
        ),
    }


def canonical_registry_content(registry: Registry) -> dict:
    """The definition-only, fully-sorted structure the bundle digest is computed over."""
    return {
        "entities": sorted(
            ({"name": e.name, "key_fields": list(e.key_fields)} for e in registry.entities),
            key=lambda d: d["name"],
        ),
        "sources": sorted(
            (
                {
                    "name": s.name,
                    "type": s.type,
                    "report_type": s.report_type,
                    "ts_field": s.ts_field,
                }
                for s in registry.sources
            ),
            key=lambda d: d["name"],
        ),
        "feature_views": sorted(
            (_canonical_view(v) for v in registry.feature_views),
            key=lambda d: (d["name"], d["view_version"]),
        ),
    }


def compute_bundle_digest(registry: Registry) -> str:
    """Deterministic ``sha256:`` digest of the registry's definitions (order-independent)."""
    canonical = canonical_value_json(canonical_registry_content(registry))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_final_bundle_digest(
    registry_definition_digest: str, feature_artifact: Mapping[str, str]
) -> str:
    """The artifact-bound identity: definition digest + the exact feature wheel.

    Deterministic and order-independent over the artifact fields, so a promoted bundle
    commits to the exact code (wheel sha256) that produced the tested definitions.
    """
    canonical = canonical_value_json(
        {
            "registry_definition_digest": registry_definition_digest,
            "feature_artifact": {k: feature_artifact[k] for k in sorted(feature_artifact)},
        }
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RegistryBundle:
    """An immutable, digest-identified snapshot of a registry's definitions.

    A *legacy unbound* bundle (``feature_artifact is None``) is identified by the
    definitional digest, byte-identical to legacy bundles. An *artifact-bound* bundle
    additionally pins the exact feature wheel; its ``bundle_digest`` is the
    ``final_bundle_digest`` (definitions + artifact) while ``registry_definition_digest``
    records the definitions-only digest.
    """

    bundle_id: str
    bundle_digest: str
    created_at: datetime
    views: tuple[str, ...]
    features: tuple[str, ...]
    registry_version: str | None = None
    source_ref: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
    feature_artifact: Mapping[str, str] | None = None
    registry_definition_digest: str | None = None

    def definitional_key(self) -> tuple:
        """The immutable identity: digest + definitional refs (excludes volatile metadata)."""
        return (self.bundle_digest, self.registry_version, self.views, self.features)

    def to_dict(self) -> dict:
        out = {
            "bundle_id": self.bundle_id,
            "bundle_digest": self.bundle_digest,
            "created_at": self.created_at.isoformat(),
            "views": list(self.views),
            "features": list(self.features),
            "registry_version": self.registry_version,
            "source_ref": self.source_ref,
            "metadata": dict(self.metadata),
        }
        # Artifact-bound fields are emitted only when present, so legacy bundles serialize
        # byte-identically to legacy.
        if self.feature_artifact is not None:
            out["feature_artifact"] = dict(self.feature_artifact)
            out["registry_definition_digest"] = self.registry_definition_digest
        return out

    @classmethod
    def from_dict(cls, data: dict) -> RegistryBundle:
        feature_artifact = data.get("feature_artifact")
        return cls(
            bundle_id=data["bundle_id"],
            bundle_digest=data["bundle_digest"],
            created_at=datetime.fromisoformat(data["created_at"]),
            views=tuple(data["views"]),
            features=tuple(data["features"]),
            registry_version=data.get("registry_version"),
            source_ref=data.get("source_ref"),
            metadata=dict(data.get("metadata", {})),
            feature_artifact=dict(feature_artifact) if feature_artifact is not None else None,
            registry_definition_digest=data.get("registry_definition_digest"),
        )


def build_registry_bundle(
    registry: Registry,
    *,
    bundle_id: str | None = None,
    created_at: datetime,
    source_ref: str | None = None,
    metadata: Mapping[str, str] | None = None,
    feature_artifact: Mapping[str, str] | None = None,
) -> RegistryBundle:
    """Build a bundle from a validated registry.

    Without ``feature_artifact`` this is a legacy unbound bundle whose identity is the
    definitions-only digest (unchanged from legacy). With a ``feature_artifact`` the
    identity is the ``final_bundle_digest`` and ``registry_definition_digest`` is recorded.
    """
    definition_digest = compute_bundle_digest(registry)
    views = tuple(sorted(f"{v.name}:v{v.view_version}" for v in registry.feature_views))
    features = tuple(
        sorted(
            f"{v.name}:v{v.view_version}:{feat.name}:v{feat.feature_version}"
            for v in registry.feature_views
            for feat in v.features
        )
    )
    if feature_artifact is None:
        digest = definition_digest
        registry_definition_digest = None
    else:
        feature_artifact = dict(feature_artifact)
        digest = compute_final_bundle_digest(definition_digest, feature_artifact)
        registry_definition_digest = definition_digest
    return RegistryBundle(
        bundle_id=bundle_id or digest,
        bundle_digest=digest,
        created_at=created_at,
        views=views,
        features=features,
        registry_version=registry.registry_version,
        source_ref=source_ref,
        metadata=dict(metadata or {}),
        feature_artifact=feature_artifact,
        registry_definition_digest=registry_definition_digest,
    )


class BundleStore(Protocol):
    def put(self, bundle: RegistryBundle) -> None: ...

    def get(self, bundle_digest: str) -> RegistryBundle | None: ...

    def list(self) -> list[RegistryBundle]: ...


class InMemoryBundleStore:
    """Immutable in-memory bundle store (single process / tests).

    A bundle is immutable once stored: re-putting the same digest with the same definitional
    content is idempotent, but the same digest with different content is a hard error.
    """

    def __init__(self) -> None:
        self._bundles: dict[str, RegistryBundle] = {}

    def put(self, bundle: RegistryBundle) -> None:
        existing = self._bundles.get(bundle.bundle_digest)
        if existing is None:
            self._bundles[bundle.bundle_digest] = bundle
            return
        if existing.definitional_key() != bundle.definitional_key():
            raise ValueError(
                f"bundle digest {bundle.bundle_digest} already stored with different "
                "content; registry bundles are immutable"
            )
        # Same digest + same content -> idempotent (keep the first-stored bundle).

    def get(self, bundle_digest: str) -> RegistryBundle | None:
        return self._bundles.get(bundle_digest)

    def list(self) -> list[RegistryBundle]:
        return list(self._bundles.values())


def _digest_filename(bundle_digest: str) -> str:
    # ``sha256:abc`` -> ``sha256_abc.json`` (``:`` is not portable in filenames).
    return bundle_digest.replace(":", "_") + ".json"


class FileBundleStore:
    """Filesystem bundle store (one JSON per digest). Immutable, like the in-memory store.

    Re-publishing the same digest with the same definitional content is idempotent; the same
    digest with different content is a hard error — published bundles never change on disk.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def _path(self, bundle_digest: str) -> Path:
        return self._root / _digest_filename(bundle_digest)

    def put(self, bundle: RegistryBundle) -> Path:
        existing = self.get(bundle.bundle_digest)
        path = self._path(bundle.bundle_digest)
        if existing is not None:
            if existing.definitional_key() != bundle.definitional_key():
                raise ValueError(
                    f"bundle digest {bundle.bundle_digest} already published with different "
                    "content; registry bundles are immutable"
                )
            return path  # same digest + same content -> idempotent
        self._root.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(bundle.to_dict(), sort_keys=True, indent=2), encoding="utf-8"
        )
        return path

    def get(self, bundle_digest: str) -> RegistryBundle | None:
        path = self._path(bundle_digest)
        if not path.exists():
            return None
        return RegistryBundle.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def list(self) -> list[RegistryBundle]:
        if not self._root.exists():
            return []
        return [
            RegistryBundle.from_dict(json.loads(p.read_text(encoding="utf-8")))
            for p in sorted(self._root.glob("*.json"))
        ]
