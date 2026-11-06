"""Postgres logical replication from arms (primaries) to their history head (a mirror).

``registry`` classifies every table (``reference``, ``time_series`` or ``local``)
and defines the publications built from those classes; ``python -m
canopy.core.replication`` (see ``__main__``) applies them:

* arm, every deploy: ``alembic upgrade head`` then ``publish``. Needs CREATE on
  the database and ownership of the tables. Grants the replication and reader roles
  SELECT on the published tables once those roles exist, so run it again after
  creating them.
* head, every deploy (as the tables' owner): ``alembic upgrade head``, ``grants``,
  ``refresh``. The head deploys before its arms, so it has every table an arm will
  publish. ``grants`` lets the head's API read every row and write only the head's
  own (row-level security on the root tables).
* the registry's owner paths say whose each row is; ``sql.owner_join`` turns them
  into SQL, for any command that has to tell one arm's rows from another's.
* head, once per arm: ``subscribe``, as the tables' owner with
  ``pg_create_subscription`` (or a superuser), with each arm's
  ``CANOPY_ARMS__<arm>__PUBLISHER_DSN`` from a secret (``canopy.core.arms``).

``status`` reports either side as JSON; on a head, per arm. ``drift-check`` runs on
a head, hourly after ``refresh``: it compares each arm's rows with the head's copy
over that arm's read-only connection (``CANOPY_ARMS__<arm>__READER_DSN``), then
checks every reference on the head (see ``drift``).
"""
