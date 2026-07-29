"""Shared D9 write guard: freshness tuple ordering + fingerprint tiebreak.

Encodes the D9 freshness rule in one pure place so the InMemory and Valkey online
stores (and the Valkey test fake) cannot drift apart. Write ordering is NOT bare
``data_ts``: the guard compares ``(data_ts, max_input_data_ts)`` lexicographically and,
on equal tuples, uses ``input_fingerprint`` to distinguish an idempotent replay (noop)
from a same-freshness recompute (write). For F1 results ``max_input_data_ts`` is absent
and degenerates to ``data_ts``, i.e. the pre-D9 CAS behavior.
"""

from __future__ import annotations

from datetime import datetime

WRITTEN = "written"
WRITTEN_RECOMPUTE = "written_recompute"
SKIPPED_STALE = "skipped_stale"
NOOP = "noop"

#: Outcomes that actually stored a value.
WRITE_OUTCOMES = frozenset({WRITTEN, WRITTEN_RECOMPUTE})


def guard_tuple(
    data_ts: datetime, max_input_data_ts: datetime | None
) -> tuple[datetime, datetime]:
    """The D9 ordering tuple; ``None`` max degenerates to ``data_ts`` (F1)."""
    return (data_ts, max_input_data_ts if max_input_data_ts is not None else data_ts)


def decide_write(
    incoming_tuple: tuple[datetime, datetime],
    incoming_fingerprint: str | None,
    current_tuple: tuple[datetime, datetime] | None,
    current_fingerprint: str | None,
    incoming_available_at: datetime | None = None,
    current_available_at: datetime | None = None,
) -> str:
    """Apply the D9 rule; missing fingerprints compare as empty (legacy records).

    Freshness ordering stays ``(data_ts, max_input_data_ts)`` — availability does not
    reorder writes. On an EQUAL tuple with an equal fingerprint, a differing
    ``available_at`` (both sides present — the same both-present rule as fingerprints,
    so legacy records replay as noop) is an auditable availability correction ->
    ``written_recompute``, never a silent noop.
    """
    if current_tuple is None:
        return WRITTEN
    if incoming_tuple > current_tuple:
        return WRITTEN
    if incoming_tuple < current_tuple:
        return SKIPPED_STALE
    if (incoming_fingerprint or "") == (current_fingerprint or ""):
        if (
            incoming_available_at is not None
            and current_available_at is not None
            and incoming_available_at != current_available_at
        ):
            return WRITTEN_RECOMPUTE
        return NOOP
    return WRITTEN_RECOMPUTE


def aggregate_online_status(outcomes) -> str:
    """Request-level ``online_write_status`` from per-feature D9 outcomes.

    Any actual write wins; otherwise a stale skip is reported; an all-noop request is
    an idempotent replay. A request with zero outcomes (write_online disabled) is the
    caller's concern — this returns ``noop`` for an empty iterable.
    """
    outcomes = list(outcomes)
    if any(outcome in WRITE_OUTCOMES for outcome in outcomes):
        return WRITTEN
    if any(outcome == SKIPPED_STALE for outcome in outcomes):
        return SKIPPED_STALE
    return NOOP
