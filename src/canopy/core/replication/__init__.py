"""Postgres logical replication from a primary to its read-only history mirror.

``registry`` classifies every table (``reference``, ``time_series`` or ``local``)
and defines the publications built from those classes; ``python -m
canopy.core.replication`` (see ``__main__``) applies them:

* primary, every deploy: ``alembic upgrade head`` then ``publish``. Needs CREATE on
  the database and ownership of the tables. Grants the replication and reader roles
  SELECT on the published tables once those roles exist, so run it again after
  creating them.
* mirror, every deploy (as the tables' owner): ``alembic upgrade head``, ``grants``,
  ``refresh``. The mirror deploys before its primary, so it has every table the
  primary will publish.
* mirror, once: ``subscribe``, as the tables' owner with ``pg_create_subscription``
  (or a superuser), with ``CANOPY_REPLICATION__PUBLISHER_DSN`` from a secret.
* mirror, hourly after its backup, WAL-archive and drift checks: ``heartbeat``,
  which records them on the primary over ``CANOPY_REPLICATION__HEARTBEAT_DSN`` (a
  role that may write only ``core.history_heartbeat``; ``publish`` grants it).

``status`` reports either side as JSON. ``drift-check`` runs on a mirror, hourly
after ``refresh``, and compares each table with the primary over a read-only
connection (``CANOPY_REPLICATION__READER_DSN``).

A primary that prunes old rows (see ``gate``) first moves to the split layout:
``publish --split`` on the primary, ``switch`` on the mirror, ``publish --split`` on
the primary again. Afterwards time-series deletes no longer reach the mirror.
"""
