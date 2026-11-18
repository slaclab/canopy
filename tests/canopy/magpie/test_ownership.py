"""Magpie config owners: a config is written only by the instance that created it.

Another owner's config is created inside ``as_instance("arm2")``, the way a mirror
receives an arm's rows through replication.
"""

from tests.canopy.magpie.conftest import READ_HEADERS, WRITE_HEADERS


async def _config(client) -> dict:
    r = await client.post("/v1/configs/trace", headers=WRITE_HEADERS, json={"name": "c", "data": {"a": 1}})
    assert r.status_code == 200, r.text
    return r.json()


async def test_a_config_is_stamped_with_this_instance(client):
    assert (await _config(client))["ownerInstance"] == "arm1"


async def test_another_owners_config_is_read_only_here(client, as_instance):
    with as_instance("arm2"):
        theirs = await _config(client)
    url = f"/v1/configs/trace/{theirs['slug']}"

    assert (await client.get(url, headers=READ_HEADERS)).json()["ownerInstance"] == "arm2"
    for r in (
        await client.put(url, headers=WRITE_HEADERS, json={"name": "mine"}),
        await client.delete(url, headers=WRITE_HEADERS),
    ):
        assert r.status_code == 403, r.text
        assert r.json()["code"] == "not_owner"


async def test_a_config_for_another_owner_is_refused(client):
    r = await client.post("/v1/configs/trace?owner=arm2", headers=WRITE_HEADERS, json={"data": {}})

    assert r.status_code == 403, r.text
    assert r.json()["code"] == "not_owner"
