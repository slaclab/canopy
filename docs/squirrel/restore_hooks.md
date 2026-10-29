# Restore hooks

A **restore hook** adds extra writes to a snapshot restore. For each setpoint
being restored, a hook can name further PVs to write and the values to write
to them. Hooks exist for writes that depend on what is being restored but
aren't part of the snapshot, typically to PVs Squirrel doesn't track. For
example, a magnet doesn't act on a new setpoint until it is told to trim, so a
hook sends that command.

Hooks are site-specific, so none run unless you configure them.

## How hooks run

1. **Planning.** Before anything is written, each configured hook looks at
   every setpoint in the restore and lists its extra writes. A hook can also
   reject a setpoint it recognises but can't handle, such as a magnet PV that
   doesn't end in `BDES`. A rejected PV is not written at all, by the restore
   or by any hook, and is reported as a failure in the `Setpoints` phase.
2. **Setpoints.** All the setpoints are written as one batch.
3. **Hooks, in order.** Each hook's extra writes go out as their own batch,
   in the order the hooks are configured, and each hook reports its own
   progress phase, named after its class (`WriteToConHook`, `MagnetTrimHook`).
   A hook only writes for PVs whose setpoint write, and every earlier hook's
   writes, succeeded.

If a hook's write fails, the restore reports that PV as failed, with the
hook's phase name, and later hooks skip it. That makes the order meaningful:
list a hook before any hook that should only run once it has succeeded.

## Configuration

List the hooks in `CANOPY_SQUIRREL__RESTORE_HOOKS`, comma-separated, in the
order they should run:

```bash
CANOPY_SQUIRREL__RESTORE_HOOKS=write_to_con,magnet_trim
```

- Empty or unset means no hook runs.
- A name listed twice runs once, in its first position.
- A name that matches no hook is skipped with a warning in the log, and the
  other hooks still run, so a typo disables only that hook.
- Restores run in the worker, so **the worker must have this setting**.
  Restart the worker after changing it: it reports the hooks it loaded when
  it starts (see [Checking the configuration](#checking-the-configuration)).

The older singular setting, `CANOPY_SQUIRREL__RESTORE_HOOK`, is no longer
read. If it is still set, the worker logs a warning at startup.

## Built-in hooks

Both built-in hooks act only on magnet setpoints. An address is a magnet
setpoint when its first colon-separated segment is `BEND`, `QUAD`, `SEXT`,
`SOLN`, `BTRM`, `XCOR`, or `YCOR`, and its last segment is `BDES`. Matching is
case-sensitive, and a `ca://` or `pva://` prefix is ignored for matching and
kept on the derived address.

An address with a magnet first segment but a different last segment, such as
`XCOR:LI21:101:BCTRL`, is rejected during planning: neither it nor its hook
writes are written, and the restore reports why.

### `magnet_trim`

A magnet doesn't act on a new `BDES` setpoint until it is **trimmed**. For
each magnet setpoint restored, this hook writes `TRIM` to the magnet's `CTRL`
PV, for example `QUAD:LI21:201:CTRL` for `QUAD:LI21:201:BDES`.

### `write_to_con`

Operators often tune magnets by hand after a restore, and it helps to compare
the live setpoint against the value that was restored. For each magnet
setpoint restored, this hook writes the same value to the magnet's `BCON` PV,
for example `QUAD:LI21:201:BCON`. `BCON` PVs exist only to hold that value:
Squirrel doesn't track or capture them.

### Recommended order

```bash
CANOPY_SQUIRREL__RESTORE_HOOKS=write_to_con,magnet_trim
```

With `write_to_con` first, a magnet is only trimmed once its `BCON` holds the
value being restored. If the `BCON` write fails, the magnet is not trimmed,
and the restore reports it under `WriteToConHook`.

## Checking the configuration

Because the setting has to reach the worker, a hook can be configured on the
API and still never run. To check without running a restore:

```
GET /squirrel/v1/health/restore-hooks
```

This needs a `read` key. It reports the API's view, computed when you call it,
next to the worker's, which the worker publishes when it starts:

```json
{
  "available": ["magnet_trim", "write_to_con"],
  "api": {
    "status": "active",
    "configured": ["write_to_con", "magnet_trim"],
    "loaded": ["write_to_con", "magnet_trim"],
    "unknown": [],
    "warnings": []
  },
  "worker": {
    "status": "disabled",
    "configured": [],
    "loaded": [],
    "unknown": [],
    "warnings": [],
    "reportedAt": "2026-10-29T16:58:03Z",
    "host": "3f9c2d1e0b4a"
  },
  "workerError": null,
  "matchesApi": false
}
```

Here the API has both hooks, but the worker started without the setting, so
restores run no hooks.

| Field | Meaning |
|-------|---------|
| `available` | Every hook name that can be configured |
| `status` | `disabled` (nothing configured), `active` (every configured name is a known hook), `partial` (some are), or `unknown` (none are) |
| `configured` | The names configured, in order |
| `loaded` | The hooks that will run, in order |
| `unknown` | Configured names that match no hook |
| `warnings` | Problems that change nothing but need attention, such as the retired singular setting still being set |
| `reportedAt`, `host` | When and where the worker published its view. An old time means the report may be from a worker that is no longer running |
| `worker` | `null` until a worker has started, or if Redis can't be read |
| `workerError` | Why Redis couldn't be read, if it couldn't |
| `matchesApi` | Whether the API and the worker load the same hooks; `null` without a worker report |

## Adding your own hook

A facility can write its own hook in Python and register it under a new
name. See [Write a custom restore hook](../how_to/custom_restore_hook.md).
