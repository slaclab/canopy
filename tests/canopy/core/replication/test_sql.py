"""Quoting, redaction and naming helpers behind the replication commands (no database)."""

import pytest

from canopy.core.replication.sql import (
    Refused,
    redact,
    qualified,
    asyncpg_dsn,
    quote_ident,
    require_role,
    quote_literal,
)
from canopy.core.replication.subscriber import subscription_name


def test_quote_ident():
    assert quote_ident("canopy_history") == '"canopy_history"'
    assert quote_ident('we"ird') == '"we""ird"'
    for bad in ("", "a\x00b"):
        with pytest.raises(ValueError):
            quote_ident(bad)


def test_qualified():
    assert qualified("squirrel.pv") == '"squirrel"."pv"'


def test_quote_literal_doubles_quotes_and_keeps_backslashes():
    assert quote_literal("host=h password='it''s'") == "'host=h password=''it''''s'''"
    assert quote_literal(r"a\b") == r"'a\b'"
    with pytest.raises(ValueError):
        quote_literal("a\x00b")


def test_asyncpg_dsn_drops_the_driver_and_keeps_the_password():
    assert asyncpg_dsn("postgresql+asyncpg://u:p%40ss@db.example.org:5432/canopy") == (
        "postgresql://u:p%40ss@db.example.org:5432/canopy"
    )


@pytest.mark.parametrize(
    ("dsn", "password"),
    [
        ("host=db.example.org dbname=canopy user=replicator password=s3cret-pw sslmode=verify-full", "s3cret-pw"),
        (r"host=db.example.org password='it\'s s3cret' user=replicator", "it's s3cret"),
        ("host=db.example.org password=left unquoted user=replicator", "left unquoted"),
        ("postgresql://replicator:s3cret%2Fpw@db.example.org:5432/canopy?sslmode=require", "s3cret/pw"),
        ("postgresql://replicator@db.example.org/canopy?password=s3cret-pw", "s3cret-pw"),
    ],
)
def test_redact_removes_the_dsn_and_its_password(dsn, password):
    # libpq quotes the token it chokes on, so a password with a space can come back in pieces.
    fragment = password.split()[-1]
    message = f'could not connect using {dsn}: missing "=" after "{fragment}"; password was {password}'
    redacted = redact(message, [dsn])
    for secret in (dsn, password, fragment):
        assert secret not in redacted
    assert "***" in redacted


def test_redact_masks_password_shaped_text_without_being_told_the_secret():
    redacted = redact("conninfo host=h password=hunter22 and postgresql://u:hunter33@h/db", [None])
    assert "hunter22" not in redacted and "hunter33" not in redacted
    assert "password=***" in redacted and "u:***@h" in redacted


def test_redact_leaves_ordinary_messages_alone():
    assert redact('relation "squirrel.pv" does not exist', ["host=h password=pw"]) == (
        'relation "squirrel.pv" does not exist'
    )


def test_subscription_name():
    assert subscription_name("lab_a") == "canopy_history_lab_a"
    for bad in (None, "", "Lab", "lab-a", "lab a", "x" * 49):
        with pytest.raises(Refused):
            subscription_name(bad)


def test_require_role():
    require_role("publish", "primary", "primary")
    with pytest.raises(Refused, match="runs on a primary"):
        require_role("publish", "primary", "mirror")
