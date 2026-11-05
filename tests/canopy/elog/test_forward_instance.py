"""E-log posts by instance role: a forward instance posts them itself, a mirror refuses them.

A post goes to the external e-log, not Canopy's database or the machine, so a forward
instance makes it with its own app tokens instead of sending it to its primary.
"""

from tests.conftest import READ_HEADERS, WRITE_HEADERS
from canopy.core.security import READ_ONLY_DETAIL

ENTRY = {"logbookIds": ["lb-ops"], "title": "Snapshot saved", "bodyMarkdown": "**All** good"}


def _post_both(client, headers):
    entry = client.post("/elog/v1/apps/squirrel/entries", json=ENTRY, headers=headers)
    upload = client.post("/elog/v1/apps/squirrel/attachments", files={"file": ("a.txt", b"x")}, headers=headers)
    return entry, upload


def test_a_forward_instance_posts_itself(forward_role, elog_client, squirrel_adapter):
    entry, upload = _post_both(elog_client, WRITE_HEADERS)

    assert entry.status_code == 200, entry.text
    assert upload.status_code == 200, upload.text
    assert [call[0] for call in squirrel_adapter.calls] == ["create_entry", "upload_attachment"]


def test_a_forward_instance_still_needs_write_scope(forward_role, elog_client, squirrel_adapter):
    for resp in _post_both(elog_client, READ_HEADERS):
        assert resp.status_code == 403
        assert resp.json()["code"] == "insufficient_write_access"
    assert squirrel_adapter.calls == []


def test_a_mirror_refuses(mirror_role, elog_client, squirrel_adapter):
    for resp in _post_both(elog_client, WRITE_HEADERS):
        assert resp.status_code == 403
        assert resp.json() == {"code": "read_only_instance", "detail": READ_ONLY_DETAIL}
    assert squirrel_adapter.calls == []


def test_a_primary_is_unchanged(elog_client, squirrel_adapter):
    entry, upload = _post_both(elog_client, WRITE_HEADERS)

    assert (entry.status_code, upload.status_code) == (200, 200)
