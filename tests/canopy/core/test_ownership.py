"""``canopy.core.ownership``'s checks. The suite runs as instance ``arm1``."""

import pytest

from canopy.core.errors import NotOwner, OwnerMismatch
from canopy.core.ownership import (
    one_owner,
    require_owned,
    local_instance,
    require_same_owner,
    require_create_owner,
)


def test_local_instance_is_canopy_instance():
    assert local_instance() == "arm1"


def test_require_owned():
    require_owned("arm1", "row")
    with pytest.raises(NotOwner, match="row is owned by 'arm2'; write it there"):
        require_owned("arm2", "row")


def test_one_owner():
    assert one_owner([], "rows") is None
    assert one_owner(["arm2", "arm2"], "rows") == "arm2"
    with pytest.raises(OwnerMismatch, match="arm1, arm2"):
        one_owner(["arm2", "arm1"], "rows")


@pytest.mark.parametrize(
    ("target", "requested"),
    [(None, None), (None, "arm1"), ("arm1", None), ("arm1", "arm1")],
)
def test_a_create_this_instance_would_own(target, requested):
    require_create_owner(target, requested, "row")


@pytest.mark.parametrize(("target", "requested"), [(None, "arm2"), ("arm2", None), ("arm2", "arm2")])
def test_a_create_another_instance_would_own(target, requested):
    with pytest.raises(NotOwner, match="'arm2'"):
        require_create_owner(target, requested, "row")


def test_a_requested_owner_that_disagrees_with_the_target():
    with pytest.raises(OwnerMismatch):
        require_create_owner("arm1", "arm2", "row")


def test_require_same_owner():
    require_same_owner("arm1", ["arm1"], "tags")
    with pytest.raises(OwnerMismatch, match="'arm2'"):
        require_same_owner("arm1", ["arm1", "arm2"], "tags")
