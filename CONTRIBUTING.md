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

### History mirrors

A history mirror is a read-only copy of a primary's database, kept in step by Postgres
logical replication (`canopy.core.replication`). It runs the same migrations, and it
migrates **before** its primary: in each release the mirror runs `alembic upgrade head`
while the primary is still on the previous release and still writing. Apply stops, and
stays stopped until someone repairs it, when the primary sends a column the mirror
doesn't have or a row the mirror already has. Every migration therefore keeps one
release of compatibility in either direction:

- **Add** a table, column or index in one release. An added column must be nullable or
  have a `server_default`, because the mirror fills it in for rows the primary sends
  before it migrates. Classify a new table in `canopy/core/replication/registry.py` in
  the same pull request; a test fails until you do.
- **Drop** a column or table publisher first, over two releases. In release N, stop
  using it, and in one migration first make a column nullable on both sides (if it
  isn't), then drop it under `if on_publisher():`. In release N+1, drop it under
  `if on_subscriber():`, by which time the mirror has applied every row that still
  carried it.
- **Rename** nothing in place: add the new column or table, copy the data under
  `if on_publisher():`, move the code over, then drop the old one as above.
- **Change a type** only by widening it (`varchar(n)` to `text`, `integer` to `bigint`).
  Anything else is an add plus a drop.
- **Tighten a constraint** (`NOT NULL`, `CHECK`, `UNIQUE`) a release after the code that
  could violate it is gone, so the primary can't send a row the mirror would reject.
- **Change data** (`INSERT`, `UPDATE`, `DELETE` on a replicated table) under
  `if on_publisher():`. Replication brings the result to the mirror; an `INSERT` run on
  both sides collides with the replicated row.
- **Index for history only** under `if on_subscriber():`, and keep the index out of the
  models so `alembic check` still passes on a primary. Never make it unique.

Local tables, which never replicate (`core.api_key`), need none of this.

```python
from canopy.core.replication.migration import on_publisher, on_subscriber
```

`migrations/env.py` takes the side from `CANOPY_ROLE`: `primary` is the publisher, any
other role a subscriber. `tests/canopy/core/replication/test_migration_order.py` walks
every revision in this order on two Postgres servers and fails on a migration that
breaks a rule; CI runs it. To run it locally, start the servers with
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
