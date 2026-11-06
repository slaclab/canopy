"""Fixtures for the snapshot outbox: a facility with two PVs, and snapshots queued for an arm."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from canopy.core.config import get_settings
from canopy.squirrel.config import OutboxConfig
from canopy.squirrel.data.models import (
    Pv,
    Tag,
    MetaPv,
    TagGroup,
    SnapshotOutbox,
    pv_tag,
    pv_facility,
)
from canopy.core.facilities.models import Facility
from tests.canopy.squirrel.conftest import TEST_DATABASE_URL
from canopy.squirrel.outbox.delivery import enqueue
from canopy.squirrel.schemas.snapshot import (
    BUNDLE_FORMAT,
    BundleValueDTO,
    SnapshotBundleDTO,
)
from canopy.squirrel.data.repositories import OutboxRepository

CAPTURED = datetime(2026, 11, 6, 14, 2, tzinfo=UTC)
CALLER = {"on_behalf_of": "operator@head", "facilities": ["lab-b"]}


class Seeded:
    """``lab-b`` with a setpoint PV (tagged ``Area/North``, a meta-PV) and a readback-only PV.

    Ids are kept as plain strings, so a test's rollback can't expire them.
    """

    facility_id: str
    sp_id: str
    rb_id: str
    tag_id: str


@pytest_asyncio.fixture
async def seeded(db_session: AsyncSession) -> Seeded:
    facility = Facility(slug="lab-b", name="Lab B")
    sp = Pv(setpoint_address="LAB:B:SP", readback_address="LAB:B:RB", device="MAG-1")
    rb = Pv(readback_address="LAB:B:ONLY:RB")
    group = TagGroup(name="Area")
    db_session.add_all([facility, sp, rb, group])
    await db_session.flush()
    tag = Tag(name="North", group_id=group.id)
    db_session.add(tag)
    await db_session.flush()
    s = Seeded()
    s.facility_id, s.sp_id, s.rb_id, s.tag_id = facility.id, sp.id, rb.id, tag.id
    await db_session.execute(pv_tag.insert().values(pv_id=s.sp_id, tag_id=s.tag_id))
    await db_session.execute(
        pv_facility.insert(),
        [{"pv_id": s.sp_id, "facility_id": s.facility_id}, {"pv_id": s.rb_id, "facility_id": s.facility_id}],
    )
    db_session.add(MetaPv(pv_id=s.sp_id, display_name="Mag"))
    await db_session.commit()
    return s


def make_bundle(s: Seeded, *, title: str = "queued save", created: datetime = CAPTURED, sid: str | None = None):
    return SnapshotBundleDTO(
        format=BUNDLE_FORMAT,
        version=1,
        source_instance="head",
        id=sid or str(uuid.uuid4()),
        title=title,
        description="taken while arm2 was cut off",
        created_date=created,
        created_by="operator@head",
        facility="lab-b",
        values=[
            BundleValueDTO(
                pv_id=s.sp_id,
                pv_name="LAB:B:SP",
                setpoint_address="LAB:B:SP",
                readback_address="LAB:B:RB",
                setpoint_value={"value": 2.5, "units": "A", "precision": 2},
                readback_value={"value": 2.49},
                status=0,
                severity=0,
                timestamp=created,
            ),
            BundleValueDTO(
                pv_id=s.rb_id,
                pv_name="LAB:B:ONLY:RB",
                readback_address="LAB:B:ONLY:RB",
                readback_value={"value": [1, 2, 3]},
                status=3,
                severity=1,
                timestamp=created,
            ),
        ],
    )


async def queue(
    session: AsyncSession,
    s: Seeded,
    *,
    arm: str = "arm2",
    minutes: int = 0,
    title: str = "queued save",
    config: OutboxConfig | None = None,
    **state,
) -> str:
    """Queue one snapshot for ``arm``, captured ``minutes`` after ``CAPTURED``; ``state`` sets columns after."""
    bundle = make_bundle(s, title=title, created=CAPTURED + timedelta(minutes=minutes))
    await enqueue(
        OutboxRepository(session),
        arm=arm,
        bundle=bundle,
        facility_id=s.facility_id,
        caller=CALLER,
        config=config or OutboxConfig(),
    )
    if state:
        await session.execute(update(SnapshotOutbox).where(SnapshotOutbox.id == str(bundle.id)).values(**state))
    await session.commit()
    return str(bundle.id)


@pytest.fixture
def app_database(monkeypatch):
    """Point ``CANOPY_DATABASE_URL`` at the test database, for code that opens its own connection."""
    monkeypatch.setenv("CANOPY_DATABASE_URL", TEST_DATABASE_URL)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
