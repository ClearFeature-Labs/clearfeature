"""Promotion governance: approval profiles, environment pointers, promote/rollback.

Feature-as-code control plane. A published bundle is immutable; making
it *serve* is a separate, audited **repoint** of an environment/stage pointer under approval
rules. Rollback is the same repoint to a previous/known digest — never a rebuild.

All records carry digests / actors / reasons / approvals / timestamps only — never feature
values or payloads. This module is pure over the store seams; the CLI wires filesystem stores.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol
from uuid import uuid4

STAGE_SHADOW = "shadow"
STAGE_LIVE = "live"
STAGES = (STAGE_SHADOW, STAGE_LIVE)


@dataclass(frozen=True)
class ApprovalProfile:
    """How many unique approvers a live promotion needs + the minimum shadow soak."""

    name: str
    required_approvers: int
    shadow_min_days: int


# Policy: bank ships the 2-person rule + 7-day shadow default;
# energy is single-approver with a shorter/zero shadow minimum.
PROFILE_BANK = ApprovalProfile("bank", required_approvers=2, shadow_min_days=7)
PROFILE_ENERGY = ApprovalProfile("energy", required_approvers=1, shadow_min_days=0)
PROFILES = {PROFILE_BANK.name: PROFILE_BANK, PROFILE_ENERGY.name: PROFILE_ENERGY}


class PromotionError(ValueError):
    """Raised when a promotion/rollback violates a governance rule."""


@dataclass(frozen=True)
class EnvironmentPointer:
    """The current bundle an (env, stage) points at — the thing that actually serves."""

    env: str
    stage: str
    bundle_digest: str
    updated_at: datetime
    shadow_started_at: datetime | None = None
    previous_digest: str | None = None

    def to_dict(self) -> dict:
        return {
            "env": self.env,
            "stage": self.stage,
            "bundle_digest": self.bundle_digest,
            "updated_at": self.updated_at.isoformat(),
            "shadow_started_at": (
                self.shadow_started_at.isoformat()
                if self.shadow_started_at is not None
                else None
            ),
            "previous_digest": self.previous_digest,
        }

    @classmethod
    def from_dict(cls, data: dict) -> EnvironmentPointer:
        shadow = data.get("shadow_started_at")
        return cls(
            env=data["env"],
            stage=data["stage"],
            bundle_digest=data["bundle_digest"],
            updated_at=datetime.fromisoformat(data["updated_at"]),
            shadow_started_at=datetime.fromisoformat(shadow) if shadow else None,
            previous_digest=data.get("previous_digest"),
        )


@dataclass(frozen=True)
class PromotionRecord:
    """An append-only audit record of one promote/rollback (values-free)."""

    promotion_id: str
    action: str  # promote | rollback
    env: str
    stage: str
    bundle_digest: str
    previous_digest: str | None
    actor: str
    reason: str
    created_at: datetime
    approved_by: tuple[str, ...] = ()
    profile: str | None = None
    shadow_started_at: datetime | None = None
    shadow_age_override: bool = False
    override_reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "promotion_id": self.promotion_id,
            "action": self.action,
            "env": self.env,
            "stage": self.stage,
            "bundle_digest": self.bundle_digest,
            "previous_digest": self.previous_digest,
            "actor": self.actor,
            "reason": self.reason,
            "created_at": self.created_at.isoformat(),
            "approved_by": list(self.approved_by),
            "profile": self.profile,
            "shadow_started_at": (
                self.shadow_started_at.isoformat()
                if self.shadow_started_at is not None
                else None
            ),
            "shadow_age_override": self.shadow_age_override,
            "override_reason": self.override_reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PromotionRecord:
        shadow = data.get("shadow_started_at")
        return cls(
            promotion_id=data["promotion_id"],
            action=data["action"],
            env=data["env"],
            stage=data["stage"],
            bundle_digest=data["bundle_digest"],
            previous_digest=data.get("previous_digest"),
            actor=data["actor"],
            reason=data["reason"],
            created_at=datetime.fromisoformat(data["created_at"]),
            approved_by=tuple(data.get("approved_by", ())),
            profile=data.get("profile"),
            shadow_started_at=datetime.fromisoformat(shadow) if shadow else None,
            shadow_age_override=data.get("shadow_age_override", False),
            override_reason=data.get("override_reason"),
        )


class PointerStore(Protocol):
    def get_pointer(self, env: str, stage: str) -> EnvironmentPointer | None: ...

    def set_pointer(self, pointer: EnvironmentPointer) -> None: ...

    def append_record(self, record: PromotionRecord) -> None: ...

    def records(self) -> list[PromotionRecord]: ...


class InMemoryPointerStore:
    """Dict-backed pointer + append-only record store (tests / single process)."""

    def __init__(self) -> None:
        self._pointers: dict[tuple[str, str], EnvironmentPointer] = {}
        self._records: list[PromotionRecord] = []

    def get_pointer(self, env: str, stage: str) -> EnvironmentPointer | None:
        return self._pointers.get((env, stage))

    def set_pointer(self, pointer: EnvironmentPointer) -> None:
        self._pointers[(pointer.env, pointer.stage)] = pointer

    def append_record(self, record: PromotionRecord) -> None:
        self._records.append(record)

    def records(self) -> list[PromotionRecord]:
        return list(self._records)


class FilePointerStore:
    """Filesystem pointer store: one JSON per (env, stage) + append-only record files."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._pointers = self._root / "pointers"
        self._records_dir = self._root / "records"

    def _pointer_path(self, env: str, stage: str) -> Path:
        return self._pointers / f"{env}__{stage}.json"

    def get_pointer(self, env: str, stage: str) -> EnvironmentPointer | None:
        path = self._pointer_path(env, stage)
        if not path.exists():
            return None
        return EnvironmentPointer.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def set_pointer(self, pointer: EnvironmentPointer) -> None:
        self._pointers.mkdir(parents=True, exist_ok=True)
        path = self._pointer_path(pointer.env, pointer.stage)
        path.write_text(
            json.dumps(pointer.to_dict(), sort_keys=True, indent=2), encoding="utf-8"
        )

    def append_record(self, record: PromotionRecord) -> None:
        self._records_dir.mkdir(parents=True, exist_ok=True)
        stamp = record.created_at.strftime("%Y%m%dT%H%M%S%f")
        path = self._records_dir / f"{stamp}_{record.promotion_id}.json"
        path.write_text(
            json.dumps(record.to_dict(), sort_keys=True, indent=2), encoding="utf-8"
        )

    def records(self) -> list[PromotionRecord]:
        if not self._records_dir.exists():
            return []
        records = [
            PromotionRecord.from_dict(json.loads(p.read_text(encoding="utf-8")))
            for p in sorted(self._records_dir.glob("*.json"))
        ]
        return records


def _unique_approvers(approved_by: Sequence[str]) -> tuple[str, ...]:
    cleaned = [a for a in approved_by if a and a.strip()]
    unique = tuple(dict.fromkeys(cleaned))
    if len(unique) != len(cleaned):
        raise PromotionError("approvers must be unique (a duplicate approver was supplied)")
    return unique


def promote(
    *,
    bundle_exists: bool,
    pointer_store: PointerStore,
    bundle_digest: str,
    env: str,
    stage: str,
    actor: str,
    reason: str,
    now: datetime,
    profile: str | None = None,
    approved_by: Sequence[str] = (),
    shadow_age_override: bool = False,
    override_reason: str | None = None,
) -> PromotionRecord:
    """Repoint (env, stage) to a published bundle under the approval rules; return the record.

    ``bundle_exists`` is the caller's check that ``bundle_digest`` is in the bundle store —
    the pointer store never rebuilds a bundle. Promoting to ``live`` enforces the profile's
    unique-approver count and shadow soak (``shadow_min_days``) unless ``shadow_age_override``
    is set with a reason (recorded).
    """
    if not bundle_exists:
        raise PromotionError(f"unknown bundle {bundle_digest!r}; publish it before promoting")
    if stage not in STAGES:
        raise PromotionError(f"target stage must be one of {list(STAGES)}, got {stage!r}")
    if not reason or not reason.strip():
        raise PromotionError("a promotion reason is required")

    if stage == STAGE_SHADOW:
        previous = pointer_store.get_pointer(env, STAGE_SHADOW)
        shadow_started_at = now
        record = PromotionRecord(
            promotion_id=f"prm_{uuid4().hex}",
            action="promote",
            env=env,
            stage=STAGE_SHADOW,
            bundle_digest=bundle_digest,
            previous_digest=previous.bundle_digest if previous else None,
            actor=actor,
            reason=reason,
            created_at=now,
            shadow_started_at=shadow_started_at,
        )
    else:  # live
        record = _promote_live(
            pointer_store=pointer_store,
            bundle_digest=bundle_digest,
            env=env,
            actor=actor,
            reason=reason,
            now=now,
            profile=profile,
            approved_by=approved_by,
            shadow_age_override=shadow_age_override,
            override_reason=override_reason,
        )

    pointer_store.set_pointer(
        EnvironmentPointer(
            env=env,
            stage=stage,
            bundle_digest=bundle_digest,
            updated_at=now,
            shadow_started_at=record.shadow_started_at,
            previous_digest=record.previous_digest,
        )
    )
    pointer_store.append_record(record)
    return record


def _promote_live(
    *,
    pointer_store: PointerStore,
    bundle_digest: str,
    env: str,
    actor: str,
    reason: str,
    now: datetime,
    profile: str | None,
    approved_by: Sequence[str],
    shadow_age_override: bool,
    override_reason: str | None,
) -> PromotionRecord:
    if profile is None:
        raise PromotionError("a --profile is required to promote to live")
    if profile not in PROFILES:
        raise PromotionError(f"unknown profile {profile!r}; expected one of {list(PROFILES)}")
    prof = PROFILES[profile]
    approvers = _unique_approvers(approved_by)
    if len(approvers) < prof.required_approvers:
        raise PromotionError(
            f"{prof.name} profile requires {prof.required_approvers} unique approver(s); "
            f"got {len(approvers)}"
        )

    shadow = pointer_store.get_pointer(env, STAGE_SHADOW)
    shadow_started_at = (
        shadow.shadow_started_at
        if shadow is not None and shadow.bundle_digest == bundle_digest
        else None
    )
    if shadow_age_override:
        if not override_reason or not override_reason.strip():
            raise PromotionError("--allow-shadow-age-override requires an explicit reason")
    else:
        if shadow_started_at is None:
            raise PromotionError(
                "bundle is not in shadow for this env; shadow it first or use "
                "--allow-shadow-age-override"
            )
        age_days = (now - shadow_started_at).total_seconds() / 86_400
        if age_days < prof.shadow_min_days:
            raise PromotionError(
                f"shadow soak {age_days:.2f}d < {prof.shadow_min_days}d required by the "
                f"{prof.name} profile; use --allow-shadow-age-override to bypass"
            )

    previous = pointer_store.get_pointer(env, STAGE_LIVE)
    return PromotionRecord(
        promotion_id=f"prm_{uuid4().hex}",
        action="promote",
        env=env,
        stage=STAGE_LIVE,
        bundle_digest=bundle_digest,
        previous_digest=previous.bundle_digest if previous else None,
        actor=actor,
        reason=reason,
        created_at=now,
        approved_by=approvers,
        profile=prof.name,
        shadow_started_at=shadow_started_at,
        shadow_age_override=shadow_age_override,
        override_reason=override_reason if shadow_age_override else None,
    )


def rollback(
    *,
    pointer_store: PointerStore,
    env: str,
    actor: str,
    reason: str,
    now: datetime,
    stage: str = STAGE_LIVE,
    to_previous: bool = False,
    bundle_digest: str | None = None,
    bundle_exists: bool | None = None,
) -> PromotionRecord:
    """Repoint (env, stage) to the previous or a specified digest — never a rebuild.

    ``bundle_exists`` (when the caller checks a specified digest against the bundle store)
    must be True; ``to_previous`` uses the current pointer's ``previous_digest``.
    """
    if not reason or not reason.strip():
        raise PromotionError("a rollback reason is required")
    if stage not in STAGES:
        raise PromotionError(f"target stage must be one of {list(STAGES)}, got {stage!r}")
    current = pointer_store.get_pointer(env, stage)
    if current is None:
        raise PromotionError(f"no current {stage} pointer for env {env!r} to roll back")

    if to_previous:
        target = current.previous_digest
        if target is None:
            raise PromotionError("no previous digest recorded to roll back to")
    else:
        if not bundle_digest:
            raise PromotionError("provide --bundle-digest or --to-previous")
        if bundle_exists is False:
            raise PromotionError(f"unknown bundle {bundle_digest!r} to roll back to")
        target = bundle_digest

    record = PromotionRecord(
        promotion_id=f"rbk_{uuid4().hex}",
        action="rollback",
        env=env,
        stage=stage,
        bundle_digest=target,
        previous_digest=current.bundle_digest,
        actor=actor,
        reason=reason,
        created_at=now,
        shadow_started_at=current.shadow_started_at if stage == STAGE_SHADOW else None,
    )
    pointer_store.set_pointer(
        EnvironmentPointer(
            env=env,
            stage=stage,
            bundle_digest=target,
            updated_at=now,
            shadow_started_at=record.shadow_started_at,
            previous_digest=current.bundle_digest,
        )
    )
    pointer_store.append_record(record)
    return record
