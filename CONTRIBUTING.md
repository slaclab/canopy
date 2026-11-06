# Contributing

## Local Setup

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
git clone https://github.com/slaclab/Canopy
cd Canopy
./scripts/setup_dev.sh
```

This installs dev dependencies and sets up pre-commit hooks.

## Running Tests

```bash
uv run pytest tests/ -v
```

## Code Style

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [isort](https://pycqa.github.io/isort/) for import ordering. Pre-commit hooks run these automatically on every commit.

To run them manually:

```bash
uv run ruff check --fix .
uv run ruff format .
uv run isort .
```

## Database Migrations

Schema changes are Alembic revisions in `migrations/versions/`, generated from the
models:

```bash
uv run alembic revision --autogenerate -m "squirrel: add a column"
```

Keep the tree to one head: before merging, point your revision's `down_revision` at
the head on `main`. CI migrates an empty database and runs `alembic check`, which
fails when the models and the migrations disagree.

### Arms and the history head

A history head (`CANOPY_ROLE=mirror`) holds the rows of one or more arms (primaries)
and is kept in step with each of them by Postgres logical replication
(`canopy.core.replication`): one subscription per arm. It runs the same migrations,
and it migrates **before every arm**: in each release the head runs
`alembic upgrade head` while its arms are still on the previous release and still
writing, then each arm migrates in turn. One deploy may take the head, or an arm,
through several revisions at once, so arms can sit at different revisions. Apply from
an arm stops, and stays stopped until someone repairs it, when that arm sends a column
the head doesn't have or a row the head already has; the other arms keep streaming.
So every revision must work with a head any number of revisions ahead of each arm:

- **Add** a table, column or index in one release. An added column must be nullable or
  have a constant `server_default`, because the head fills it in for rows an arm sends
  before that arm migrates. Classify a new table in
  `canopy/core/replication/registry.py` in the same pull request; a test fails until
  you do.
- **Drop** a column or table on the arms first, over two releases. In release N, stop
  using it, and in one migration first make a column nullable on every side (if it
  isn't), then drop it under `if on_publisher():`. In a later release, drop it under
  `if on_subscriber():` in a revision that declares the release-N revision with a
  module-level `canopy_requires_primary_at = "<revision>"`, and deploy it with the check
  below.
- **Rename** nothing in place: add the new column or table, copy the data under
  `if on_publisher():`, move the code over, then drop the old one as above.
- **Change a type** only by widening it (`varchar(n)` to `text`, `integer` to `bigint`).
  Anything else is an add plus a drop.
- **Tighten a constraint** (`NOT NULL`, `CHECK`, `UNIQUE`) a release after the code that
  could violate it is gone and any backfill ran (under `if on_publisher():`), so no arm
  can send a row the head would reject. The tightening revision declares the
  backfill's revision in `canopy_requires_primary_at` and is deployed with the check
  below.
- **Change data** (`INSERT`, `UPDATE`, `DELETE` on a replicated table) under
  `if on_publisher():`. Replication brings each arm's result to the head; a step run on
  the head as well changes rows its arm changes too, and an `INSERT` there collides
  with the replicated row.
- **Index for history only** under `if on_subscriber():`, and keep the index out of the
  models so `alembic check` still passes on an arm. Never make it unique.
- **Retire a table** like a drop, and keep its entry in the replication registry with
  its name added to `RETIRED` in `canopy/core/replication/registry.py`: a database that
  hasn't dropped it yet still needs it classified. Remove both once no deployed database
  can hold it.

Local tables, which never replicate (`core.api_key`), need none of this.

**Deploying a revision that declares `canopy_requires_primary_at`.** Before the head
migrates past it, confirm on **each** arm that it runs the named revision or a later
one (`alembic current` there), and that the head has caught up with everything the arm
wrote before it: on that arm, `python -m canopy.core.replication status` shows the
slot `canopy_history_<arm>` `active` with `lag_bytes` at or near 0. A head that was
behind an arm would still be applying rows that carry the dropped column, or that
break the new constraint. There is no command for this; check every arm by hand.

```python
from canopy.core.replication.migration import on_publisher, on_subscriber
```

`migrations/env.py` takes the side from `CANOPY_ROLE`: a `primary` (an arm) is a
publisher, a `mirror` (the head) the subscriber.
`tests/canopy/core/replication/test_migration_order.py` walks every revision on a head
and its arms, each on its own Postgres server, twice: one release at a time, the head
first and then each arm, and with the head at head while each arm climbs from the
first revision (or from the newest `canopy_requires_primary_at`). After each step
every arm writes a row to every table it publishes from its own schema at that moment,
filling every column without a default, and every instance writes a row to each local
table. The head must then hold exactly each arm's rows, compared per arm by primary
key, including right after a migration and before anyone writes; and the head's own
migration may not write to any arm's rows. The walk fails on a migration that breaks
a rule. CI runs it. If a new column has a `CHECK` the writer's generic value fails
(one that limits it to a list of values is handled), add a value to `OVERRIDES` in that
test. To run it locally, start the servers with
`docker compose -f tests/canopy/core/replication/docker-compose.yml up -d --wait` and
set the variables listed in that file.

## Building Docs

```bash
uv run zensical serve
```

## Submitting Changes

1. Create a branch off `main` using the format `<username>/<description>` or `<username>/#<issue-number>-<description>`:
   ```
   zdomke/my-changes
   yektay/#10-bug-fix
   ```
2. Make your changes and ensure tests pass
3. Open a pull request using the provided template

### From a fork

If you don't have write access to this repository, fork it, clone your fork and
follow the setup above. Branch off `main` in your fork; the branch name format
above is only a convention for people who push here, so any descriptive name
works. Open the pull request against `slaclab/canopy:main`.

- For anything larger than a small fix, open an issue first so we can agree on
  the approach before you spend time on it.
- A maintainer has to approve the GitHub Actions run on a pull request from a
  fork before it starts.
- Pull requests need an approving review before they can be merged.

## Reporting Security Issues

Don't report vulnerabilities in public issues or pull requests. See
[SECURITY.md](SECURITY.md).

## License

Canopy is distributed under the terms in [LICENSE.md](LICENSE.md). By
contributing, you accept that the last paragraph of that file, which licenses
enhancements that you make available, applies to your contribution.
