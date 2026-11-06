"""Unit tests for the shared id and continuation-token parsing (no database needed)."""

import pytest

from canopy.core.errors import BadRequest
from canopy.core.validation import parse_uuid, require_uuid, parse_continuation_token

U = "0cfc8773-53cb-4d04-8b6e-1a2b3c4d5e6f"


@pytest.mark.parametrize("form", [U, U.upper(), "{" + U + "}", "urn:uuid:" + U, U.replace("-", "")])
def test_parse_uuid_normalises_accepted_forms(form: str):
    assert parse_uuid(form) == U


@pytest.mark.parametrize("value", ["garbage", "", "0" * 40, "+" + "0" * 31, "0_" + "0" * 30, " " + "0" * 31])
def test_parse_uuid_rejects_non_uuids(value: str):
    """uuid.UUID() would accept the signed, underscored and padded forms through int(hex, 16)."""
    assert parse_uuid(value) is None


def test_require_uuid_returns_canonical_or_raises():
    assert require_uuid("{" + U + "}", BadRequest, "nope") == U
    with pytest.raises(BadRequest, match="nope"):
        require_uuid("garbage", BadRequest, "nope")


def test_continuation_token_absent_empty_valid_and_bad():
    assert parse_continuation_token(None) is None
    assert parse_continuation_token("") is None
    assert parse_continuation_token("urn:uuid:" + U) == U
    with pytest.raises(BadRequest, match="Invalid continuationToken"):
        parse_continuation_token("garbage")
