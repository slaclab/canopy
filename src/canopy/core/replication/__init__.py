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

``status`` reports either side as JSON.
"""
