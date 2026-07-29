"""Valkey-backed online feature store.

Implements the ``InMemoryOnlineStore`` shape (``write`` / ``write_many`` / ``get``)
against Valkey (via the ``redis`` client). Each entity+view is one Valkey hash:

    key:   fs:online:{view}:v{view_version}:{entity_key_encoded}
    field: {feature_name}:v{feature_version}            (== FeatureRef.encode())
    value: JSON {value, data_ts (iso), calc_ts (iso), data_ts_epoch_us (int),
                 max_input_data_ts (iso|null), max_input_data_ts_epoch_us (int),
                 input_fingerprint (str|null), value_hash (str|null)}

Writes apply the D9 guard atomically in Lua (the server-side twin of
``fs_core.write_guard.decide_write``): compare ``(data_ts, max_input_data_ts)``
lexicographically as epoch microseconds; on equal tuples compare ``input_fingerprint``
(noop when unchanged, write for a same-freshness recompute). Legacy records without the
D9 fields degenerate to the legacy CAS on ``data_ts`` (missing max -> data_ts,
missing fingerprint -> empty string). ``redis`` is imported lazily (only in
``connect_valkey``), so this module imports — and its unit tests run — without the
optional ``online`` extra. ``entity_type`` is not part of the key yet (the current
online-store seam does not carry it); see the task notes.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    trusted_available_at,
)
from fintech_feature_platform.fs_core.write_guard import (
    NOOP,
    SKIPPED_STALE,
    WRITTEN,
    WRITTEN_RECOMPUTE,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Atomic D9 write guard. ARGV: 1=field, 2=record json, 3=data_ts_epoch_us,
# 4=max_input_data_ts_epoch_us (degenerate = data_ts), 5=input_fingerprint ('' if none),
# 6=available_at iso ('' if none). Returns 0=skipped_stale, 1=written, 2=noop,
# 3=written_recompute. On an equal tuple with an equal fingerprint, a differing
# available_at (both sides present — legacy records keep replaying as noop) is an
# auditable availability correction -> written_recompute. cjson.decode
# raises on malformed stored JSON, which surfaces as a client error rather than a
# silent overwrite.
_D9_SCRIPT = """
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  local c = cjson.decode(cur)
  local cur_dts = tonumber(c.data_ts_epoch_us)
  local cur_max = tonumber(c.max_input_data_ts_epoch_us) or cur_dts
  local in_dts = tonumber(ARGV[3])
  local in_max = tonumber(ARGV[4])
  if in_dts < cur_dts or (in_dts == cur_dts and in_max < cur_max) then
    return 0
  end
  if in_dts == cur_dts and in_max == cur_max then
    local cur_fp = c.input_fingerprint
    if cur_fp == nil or cur_fp == cjson.null then cur_fp = '' end
    if ARGV[5] == cur_fp then
      local cur_av = ''
      if c.availability_source == 'source_provided' and c.available_at ~= nil
         and c.available_at ~= cjson.null then
        cur_av = c.available_at
      end
      if ARGV[6] ~= '' and cur_av ~= '' and ARGV[6] ~= cur_av then
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        return 3
      end
      return 2
    end
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    return 3
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
"""

_OUTCOME_BY_CODE = {0: SKIPPED_STALE, 1: WRITTEN, 2: NOOP, 3: WRITTEN_RECOMPUTE}


def connect_valkey(host: str = "localhost", port: int = 6379, *, db: int = 0):
    """Connect to Valkey. Imports the ``redis`` client lazily (``online`` extra)."""
    import redis

    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


def connect_valkey_probe(
    host: str, port: int, *, db: int = 0, timeout_s: float = 2.0
):
    """Readiness-probe client : SHORT deadline, NO retries.

    Built ONCE at backend construction, used only by the readiness ``ping`` probe.
    Deliberately separate from ``connect_valkey``: readiness needs a small operational
    deadline (connect + op each bounded by ``timeout_s``, single attempt), while
    data-plane clients keep their own timeout/retry policy — the two must not be
    conflated.
    """
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    return redis.Redis(
        host=host, port=port, db=db, decode_responses=True,
        socket_connect_timeout=timeout_s, socket_timeout=timeout_s,
        retry=Retry(NoBackoff(), 0),
    )


class ValkeyOnlineStore:
    """Latest online feature values with atomic D9 (freshness-tuple) writes."""

    def __init__(self, client) -> None:
        self._client = client

    def write(self, view: str, view_version: int, result: FeatureResult) -> str:
        epoch_us = _to_epoch_us(result.data_ts)
        max_epoch_us = (
            _to_epoch_us(result.max_input_data_ts)
            if result.max_input_data_ts is not None
            else epoch_us
        )
        value_json = _encode_record(result, epoch_us, max_epoch_us)
        key = _key(view, view_version, result.entity_key.encode())
        field = _field(result.ref.name, result.ref.version)
        code = self._client.eval(
            _D9_SCRIPT,
            1,
            key,
            field,
            value_json,
            epoch_us,
            max_epoch_us,
            result.input_fingerprint or "",
            (
                trusted.isoformat()
                if (trusted := trusted_available_at(result)) is not None
                else ""
            ),
        )
        return _OUTCOME_BY_CODE[int(code)]

    def write_many(
        self, view: str, view_version: int, results
    ) -> dict[str, str]:
        return {
            result.ref.encode(): self.write(view, view_version, result)
            for result in results
        }

    def get(
        self,
        view: str,
        view_version: int,
        entity_key: EntityKey,
        feature_name: str,
        feature_version: int,
    ) -> FeatureResult | None:
        key = _key(view, view_version, entity_key.encode())
        field = _field(feature_name, feature_version)
        raw = self._client.hget(key, field)
        if raw is None:
            return None
        return _decode_record(raw, entity_key, feature_name, feature_version)


def _key(view: str, view_version: int, entity_key_encoded: str) -> str:
    return f"fs:online:{view}:v{view_version}:{entity_key_encoded}"


def _field(feature_name: str, feature_version: int) -> str:
    return f"{feature_name}:v{feature_version}"


def _to_epoch_us(value: datetime) -> int:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("data_ts must be timezone-aware")
    delta = value.astimezone(UTC) - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _encode_record(result: FeatureResult, epoch_us: int, max_epoch_us: int) -> str:
    payload = {
        "value": result.value,
        "data_ts": result.data_ts.isoformat(),
        "calc_ts": result.calc_ts.isoformat(),
        "data_ts_epoch_us": epoch_us,
        "max_input_data_ts": (
            result.max_input_data_ts.isoformat()
            if result.max_input_data_ts is not None
            else None
        ),
        "max_input_data_ts_epoch_us": max_epoch_us,
        "input_fingerprint": result.input_fingerprint,
        "value_hash": result.value_hash,
        "available_at": (
            result.available_at.isoformat()
            if result.available_at is not None
            else None
        ),
        "availability_source": result.availability_source,
    }
    try:
        return json.dumps(payload)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"feature value is not JSON-serializable: {exc}") from exc


def _decode_record(
    raw: Any, entity_key: EntityKey, feature_name: str, feature_version: int
) -> FeatureResult:
    try:
        data = json.loads(raw)
        max_iso = data.get("max_input_data_ts")
        return FeatureResult(
            ref=FeatureRef(feature_name, feature_version),
            entity_key=entity_key,
            value=data["value"],
            data_ts=datetime.fromisoformat(data["data_ts"]),
            calc_ts=datetime.fromisoformat(data["calc_ts"]),
            max_input_data_ts=(
                datetime.fromisoformat(max_iso) if max_iso is not None else None
            ),
            input_fingerprint=data.get("input_fingerprint"),
            value_hash=data.get("value_hash"),
            available_at=(
                datetime.fromisoformat(data["available_at"])
                if data.get("available_at") is not None
                else None
            ),
            availability_source=data.get("availability_source"),
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"malformed online record: {exc}") from exc
