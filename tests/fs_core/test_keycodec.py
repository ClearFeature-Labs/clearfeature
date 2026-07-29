import pytest

from fintech_feature_platform.fs_core.keycodec import encode_entity_key


def test_alphabetical_fallback_order():
    parts = {"user_id": "123", "application_id": "A1", "report_id": "R9"}
    assert encode_entity_key(parts) == "application_id=A1|report_id=R9|user_id=123"


def test_explicit_key_order_is_used():
    parts = {"user_id": "123", "application_id": "A1", "report_id": "R9"}
    encoded = encode_entity_key(
        parts, key_order=["user_id", "application_id", "report_id"]
    )
    assert encoded == "user_id=123|application_id=A1|report_id=R9"


def test_encoding_is_deterministic_regardless_of_insertion_order():
    first = encode_entity_key({"b": "2", "a": "1"})
    second = encode_entity_key({"a": "1", "b": "2"})
    assert first == second == "a=1|b=2"


def test_single_part():
    assert encode_entity_key({"user_id": "123"}) == "user_id=123"


def test_empty_mapping_rejected():
    with pytest.raises(ValueError):
        encode_entity_key({})


@pytest.mark.parametrize("parts", [{"": "1"}, {"user_id": ""}, {"user_id": "   "}])
def test_empty_name_or_value_rejected(parts):
    with pytest.raises(ValueError):
        encode_entity_key(parts)


@pytest.mark.parametrize("bad", ["a|b", "a=b"])
def test_reserved_characters_rejected_in_name(bad):
    with pytest.raises(ValueError):
        encode_entity_key({bad: "1"})


@pytest.mark.parametrize("bad", ["1|2", "1=2"])
def test_reserved_characters_rejected_in_value(bad):
    with pytest.raises(ValueError):
        encode_entity_key({"user_id": bad})


def test_key_order_missing_key_rejected():
    with pytest.raises(ValueError):
        encode_entity_key({"a": "1", "b": "2"}, key_order=["a"])


def test_key_order_unknown_key_rejected():
    with pytest.raises(ValueError):
        encode_entity_key({"a": "1"}, key_order=["a", "b"])


def test_key_order_duplicate_key_rejected():
    with pytest.raises(ValueError):
        encode_entity_key({"a": "1", "b": "2"}, key_order=["a", "a"])
