# API keys

Every data endpoint in Canopy is gated by an API key. This page is the
operator's runbook: how to mint, list, and revoke keys, and how the two
credential layers behind the scenes fit together.

## How keys work

Every key carries three independent scopes:

- **`read`** — GET requests and WebSocket connections, plus the handful of
  POSTs that only read (Converter's conversions, Fox's evaluate/validate).
- **`write`** — endpoints that change something: setting a PV, saving a
  snapshot, storing a config.
- **`admin`** — key management (this page's own HTTP surface) and other
  operational-not-data actions, such as dropping a Vole ring buffer.

`admin` is **orthogonal**, not a level above `read`/`write` — an admin key that
cannot itself read a PV is a better credential for a provisioning tool than one
that can do everything. A `write` key cannot mint or revoke keys; only `admin`
can.

Independently, every key also carries a **module scope** — see
[Module scoping](#module-scoping) below.

Everything else — the working set of keys for local testing — is easiest to
manage through the CLI once `CANOPY_DATABASE_URL` points somewhere real:

```bash
# Mint. -r/-w/-a add scopes; at least one is required.
uv run python scripts/create_key.py trace -r
uv run python scripts/create_key.py operator-tools -r -w
uv run python scripts/create_key.py ops-cli -a

# List (add -a for active-only).
uv run python scripts/list_keys.py

# Revoke.
uv run python scripts/deactivate_key.py <key-id>
```

`create_key.py` prints the plaintext token once — it is not recoverable, only
its hash is stored. These scripts talk to the database directly, so they work
with only `CANOPY_DATABASE_URL` set: no existing key is needed to bootstrap the
first one, which is what makes them the right tool for a fresh instance.

Without a local `uv` environment, run the same commands inside the `api`
container instead: `docker compose run --rm api python scripts/create_key.py ...`.

### Production instances

The env layer's two long-lived keys live in the instance's `CANOPY_API_KEYS`,
in the `.env` beside the production compose file (see
[configuration](production.md#configuration)):

```bash
CANOPY_API_KEYS=<display-read-token>:read@*,<ops-admin-token>:admin@*
```

Everything else for that instance — per-tool keys, CI keys — is minted with
the CLI against the instance's database, not through `.env`. The CLI ships
in the image (`Dockerfile.prod` copies `scripts/`), so run it in the running
`api` container rather than from a workstation — the instance's Postgres is
often bound to the host's `127.0.0.1` and isn't reachable from outside it:

```bash
docker compose run --rm api python scripts/create_key.py "<app>" -r
```

Rotating an env key is still a deploy; rotating anything else is a CLI call.

## Managing keys over HTTP

The same operations are available at `/v1/api-keys` on the parent app, gated on
`admin` (never `write` — see [Scope design](#why-admin-not-write) below):

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/v1/api-keys` | Mint a key. Body: `appName`, and any of `read`/`write`/`admin`. Returns the plaintext `token` once. |
| `GET` | `/v1/api-keys` | List keys (`?activeOnly=true`, `?appName=<name>` to filter). Never returns a token. |
| `GET` | `/v1/api-keys/count` | Count keys (`?activeOnly=true` to filter). |
| `DELETE` | `/v1/api-keys/{id}` | Deactivate a key. |

```bash
curl -X POST -H "X-API-Key: <admin-token>" -H "Content-Type: application/json" \
  -d '{"appName": "trace", "read": true}' \
  http://localhost:<API_PORT>/v1/api-keys
```

!!! note "Deactivating the key you're using"
    `DELETE /v1/api-keys/{id}` refuses to deactivate the key that authenticated
    the request itself — `400` `api_key_self_deactivation` — since doing so
    would lock that session out mid-use. Pass `?force=true` to override, or use
    `scripts/deactivate_key.py`, which has no such guard (it isn't a request
    that can lock itself out).

### Module scoping

A key can be restricted to one or more mounted sub-apps — `mole`, `hare`,
`raccoon`, `vole`, `fox`, `squirrel`, `magpie`, `jobs`, `converter`
— or `core`, for the parent app's own routes (`/health`, `/v1/api-keys`).
Minting a key via the CLI or `POST /v1/api-keys` without naming any modules
grants every module, an explicit default of `["*"]` at that layer. There is no
such default for the env layer — see below.

This constrains which **surface** a key can call, not which **data** it can
see through that surface — a `squirrel`-scoped key can reach every route
`squirrel` exposes, the same as an unscoped key would. Facility-level data
scoping is a separate, Squirrel-side mechanism and is not expressible here.

Set it with `-m`/`--module` (repeatable) on `scripts/create_key.py`, or the
`modules` field on `POST /v1/api-keys`:

```bash
uv run python scripts/create_key.py trace -r -m fox -m vole
```

```bash
curl -X POST -H "X-API-Key: <admin-token>" -H "Content-Type: application/json" \
  -d '{"appName": "trace", "read": true, "modules": ["fox", "vole"]}' \
  http://localhost:<API_PORT>/v1/api-keys
```

For an env-layer key, the `@<module>[+<module>]` suffix is **required** — an
entry with no `@` at all is skipped as malformed, the same as an empty token.
Use `@*` for every module:

```bash
CANOPY_API_KEYS="trace:read@fox+vole,ops:admin@*"
```

A key that resolves but isn't scoped to the module a route belongs to gets
`403` `insufficient_module_access` — checked before the `read`/`write`/`admin`
scope check, so an out-of-scope, no-access key is still reported as
out-of-scope, not out-of-read.

- [ ] Tokens are freshly generated per instance — not shared between DEV and a
      facility, and not shared with another SLAC app.
- [ ] Only one `admin` key exists in the env layer (for ops), and it is not
      also used as a `read`/`write` credential anywhere else.
- [ ] `CANOPY_DEBUG` is `false` (verbose errors otherwise).
- [ ] The env tokens are recorded in a secrets manager; database-backed tokens
      are recorded wherever the owning team tracks its own credentials.

## Verifying a deployment

The startup log reports what the env layer loaded, without ever printing a
token:

```
Registered 2 API key(s) from CANOPY_API_KEYS (1 read, 0 write, 1 admin, 0 module-scoped)
```

An empty env layer is not itself a problem — it just means every key comes
from the database — and logs an `INFO` line saying so rather than a warning. A
malformed entry, an empty token, or an unrecognized scope (`read+wrote`, say)
each warn and name their position, never their content:

```bash
# docker-compose.prod.yml sets no container_name; `compose logs` resolves the
# service name for you rather than requiring you to guess the container's.
docker compose logs api --since 10m | grep CANOPY_API_KEYS
```

The container's healthcheck hits `/health` every 10s, so a plain `--tail 50`
scrolls the startup summary out of view within minutes of boot — use
`--since` (anchored to the deploy time) instead, or reach for Actions ->
`Container Host Maintenance` -> `app-logs` (raise `log_tail`) if you're
working from CI rather than the host.

Then confirm end to end, from a client's point of view:

```bash
# Ungated — proves the process is up, says nothing about keys:
curl http://localhost:<API_PORT>/health

# Read-gated — 200 means the key is valid and has read scope:
curl -H "X-API-Key: <token>" http://localhost:<API_PORT>/hare/v1/health/summary

# Expect 401 {"code": "invalid_api_key", ...}:
curl -H "X-API-Key: not-a-real-token" http://localhost:<API_PORT>/hare/v1/health/summary

# Admin-gated key management — 200 with the admin token, 403 with anything else:
curl -H "X-API-Key: <admin-token>" http://localhost:<API_PORT>/v1/api-keys
```

A key with the wrong scope returns `403` with code `insufficient_read_access`,
`insufficient_write_access`, `insufficient_admin_access`, or (see
[Module scoping](#module-scoping)) `insufficient_module_access` —
distinguishing "unknown token" (`401`) from "known token, not allowed here"
(`403`).

## Known limitations

- Keys have no expiration (`expires_at`) — nothing enforces one today.
