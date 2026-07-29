"""Deterministic encoding of entity keys.

An entity key is a small set of named string parts (for example
``user_id=123``). The platform passes around the encoded string form so the
same logical entity always maps to the same key in the offline and online
stores, regardless of how the parts were originally ordered.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

# Characters used to build the encoded string. They are not allowed inside
# part names or values so the encoding stays unambiguous.
_RESERVED = ("|", "=")


def encode_entity_key(
    parts: Mapping[str, str],
    key_order: Sequence[str] | None = None,
) -> str:
    """Encode entity key parts into a deterministic string.

    With ``key_order`` the parts are emitted in exactly that order; without it
    they fall back to alphabetical order. ``key_order`` must list exactly the
    same keys as ``parts`` (no missing, unknown, or duplicate keys).
    """
    if not parts:
        raise ValueError("entity key must have at least one part")

    for name, value in parts.items():
        _validate_part(name, value)

    if key_order is None:
        ordered_names = sorted(parts)
    else:
        ordered_names = list(key_order)
        _validate_key_order(parts, ordered_names)

    return "|".join(f"{name}={parts[name]}" for name in ordered_names)


def _validate_part(name: str, value: str) -> None:
    if not name or not name.strip():
        raise ValueError("entity key part name must be non-empty")
    if not value or not value.strip():
        raise ValueError(f"entity key value for {name!r} must be non-empty")
    for token, label in ((name, "name"), (value, "value")):
        for char in _RESERVED:
            if char in token:
                raise ValueError(
                    f"entity key {label} {token!r} must not contain {char!r}"
                )


def _validate_key_order(parts: Mapping[str, str], key_order: Sequence[str]) -> None:
    if len(key_order) != len(parts) or set(key_order) != set(parts):
        raise ValueError(
            f"key_order {list(key_order)!r} must contain exactly the keys "
            f"{sorted(parts)!r}"
        )
