"""Quoting, redaction and naming helpers behind the replication commands (no database)."""

import pytest

from canopy.core.replication.sql import (
    Refused,
    redact,
    qualified,
    owner_join,
    asyncpg_dsn,
    quote_ident,
    require_role,
    quote_literal,
    conninfo_as_uri,
)
from canopy.core.replication.registry import (
    REGISTRY,
    OwnerPath,
    TableClass,
    ReplicationRegistry,
)


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


def test_require_role():
    require_role("publish", "primary", "primary")
    with pytest.raises(Refused, match="runs on a primary"):
        require_role("publish", "primary", "mirror")


CHAIN = ReplicationRegistry(
    tables={"s.root": TableClass.REFERENCE, "s.mid": TableClass.REFERENCE, "s.leaf": TableClass.TIME_SERIES},
    publications=REGISTRY.publications,
    owner_paths={
        "s.root": OwnerPath(column="owner_instance"),
        "s.mid": OwnerPath(parent="s.root", key="root_id"),
        "s.leaf": OwnerPath(parent="s.mid", key="mid_id"),
    },
)


def test_owner_join_for_a_root_child_and_grandchild():
    assert owner_join(CHAIN, "s.root", alias="t") == ("", 't."owner_instance"')
    assert owner_join(CHAIN, "s.mid", alias="t") == (
        'JOIN "s"."root" t_o1 ON t_o1.id = t."root_id"',
        't_o1."owner_instance"',
    )
    assert owner_join(CHAIN, "s.leaf", alias="v") == (
        'JOIN "s"."mid" v_o1 ON v_o1.id = v."mid_id" JOIN "s"."root" v_o2 ON v_o2.id = v_o1."root_id"',
        'v_o2."owner_instance"',
    )


def test_owner_join_refuses_a_table_without_a_path():
    with pytest.raises(ValueError, match="s.other has no owner path"):
        owner_join(CHAIN, "s.other", alias="t")


@pytest.mark.parametrize(
    ("conninfo", "uri"),
    [
        ("postgresql://u:p@h:5432/db?sslmode=require", "postgresql://u:p@h:5432/db?sslmode=require"),
        (
            "host=arm1.example.org port=5432 dbname=canopy user=replicator password=s3cret sslmode=verify-full",
            "postgresql://replicator:s3cret@arm1.example.org:5432/canopy?sslmode=verify-full",
        ),
        (r"host=h dbname=db user=u password='it\'s a/secret'", "postgresql://u:it%27s%20a%2Fsecret@h/db"),
        ("host=h dbname=db", "postgresql://h/db"),
    ],
)
def test_conninfo_as_uri(conninfo, uri):
    assert conninfo_as_uri(conninfo) == uri
