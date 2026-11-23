"""Request-input parsing shared by the module routers.

Primary keys are ``UUID(as_uuid=False)`` columns, and with asyncpg's native UUID support
SQLAlchemy installs no bind processor for them: whatever string a router passes on reaches
asyncpg unchanged. asyncpg encodes only the canonical and bare-hex spellings, so an id or
continuation token in any other form raises ``DataError`` inside the query and surfaces as
a 500. Routers parse ids here and pass on the canonical form, turning anything else into
their own 404 or 400 before a query is built.
"""

from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from canopy.core.errors import BadRequest, CanopyError

# pydantic's parser rather than uuid.UUID(): the latter ends in int(hex, 16), which accepts a
# leading sign, whitespace and underscores, so "+" followed by 31 zeros would parse as the nil
# UUID instead of being rejected.
_UUID = TypeAdapter(UUID)


def parse_uuid(value: str) -> str | None:
    """Canonical lowercase, hyphenated form of ``value``, or ``None`` when it is not a UUID.

    Braced, ``urn:uuid:``, bare-hex and uppercase spellings are accepted and normalised.
    """
    try:
        return str(_UUID.validate_python(value))
    except ValidationError:
        return None


def require_uuid(value: str, error: type[CanopyError], message: str) -> str:
    """``parse_uuid`` that raises ``error(message)`` instead of returning ``None``.

    Routers use it on path ids so a malformed id becomes the same 404 a missing row would.
    """
    canonical = parse_uuid(value)
    if canonical is None:
        raise error(message)
    return canonical


def parse_continuation_token(value: str | None) -> str | None:
    """Canonical form of a UUID keyset ``continuationToken``, or ``None`` for the first page.

    An absent or empty token means the first page. Anything else that is not a UUID raises
    ``BadRequest`` so the caller answers 400 rather than failing inside asyncpg.
    """
    if not value:
        return None
    return require_uuid(value, BadRequest, "Invalid continuationToken")
