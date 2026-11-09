"""E-log posts on a history head: made there, with its own e-log app tokens.

A post goes to the external e-log, not Canopy's database or the machine, so a head
makes it itself, as an arm does, and never forwards it (``forwarding.routes.LOCAL``).
"""

from tests.conftest import READ_HEADERS, WRITE_HEADERS

ENTRY = {"logbookIds": ["lb-ops"], "title": "Snapshot saved", "bodyMarkdown": "**All** good"}


def _post_both(client, headers):
    entry = client.post("/elog/v1/apps/squirrel/entries", json=ENTRY, headers=headers)
    upload = client.post("/elog/v1/apps/squirrel/attachments", files={"file": ("a.txt", b"x")}, headers=headers)
    return entry, upload


def test_a_head_posts_itself(mirror_role, elog_client, squirrel_adapter):
    entry, upload = _post_both(elog_client, WRITE_HEADERS)

    assert entry.status_code == 200, entry.text
    assert upload.status_code == 200, upload.text
    assert [call[0] for call in squirrel_adapter.calls] == ["create_entry", "upload_attachment"]


def test_a_head_still_needs_write_scope(mirror_role, elog_client, squirrel_adapter):
    for resp in _post_both(elog_client, READ_HEADERS):
        assert resp.status_code == 403
        assert resp.json()["code"] == "insufficient_write_access"
    assert squirrel_adapter.calls == []
