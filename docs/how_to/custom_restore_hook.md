# Custom restore hook

A [restore hook](../squirrel/restore_hooks.md) adds extra writes to a Squirrel
restore. Canopy ships two, for SLAC magnets. A facility with its own
conventions writes its own hook and registers it under a name it can list in
`CANOPY_SQUIRREL__RESTORE_HOOKS`.

Note the division of labour: a hook only decides which extra writes a
setpoint needs. Squirrel does everything else. It writes the setpoints first,
sends each hook's writes as a batch, skips PVs whose earlier writes failed,
reports progress per hook, and records failures. A hook never touches EPICS
itself.

!!! note "Hooks live in Canopy's source tree"
    A hook must be added to Canopy itself and registered in its code (step 2).
    There is not yet a way to load a hook from a facility's own package, so a
    facility-specific hook currently means maintaining a change to Canopy.

The two built-in hooks are short and make good templates:

- `src/canopy/squirrel/restore_hooks/magnet_trim.py`: `MagnetTrimHook`, plus
  the `derive_magnet_address` helper both hooks use to recognise magnet
  setpoints and build the derived address.
- `src/canopy/squirrel/restore_hooks/write_to_con.py`: `WriteToConHook`.

## 1. Implement the hook

Subclass `canopy.squirrel.restore_hooks.base.RestoreHook` and implement one
method:

```python
def derive_extra_writes(self, address: str, value: Any) -> list[tuple[str, Any]]:
```

- `address` is a setpoint address being restored, exactly as it is stored,
  including any `ca://` or `pva://` prefix.
- `value` is the value being written to it.
- Return the extra writes for that setpoint as `(address, value)` pairs, or
  an empty list if the hook doesn't apply to it. Most setpoints get an empty
  list.

`WriteToConHook`, for instance, returns `[("QUAD:LI21:201:BCON", 3.0)]` for
`("QUAD:LI21:201:BDES", 3.0)`, and `[]` for any PV that isn't a magnet
setpoint.

Squirrel calls the method once per setpoint, for every setpoint in the
restore, before anything is written. Keep it fast and free of side effects:
it shouldn't do I/O, and it may be called for setpoints that end up not being
written.

To match on PV names, split off any `ca://` or `pva://` prefix with
`canopy.epics.pv_protocol.parse_pv_name`, and put the prefix back on the
addresses you return, so the extra writes go over the same protocol.
`derive_magnet_address` shows how.

### Rejecting a setpoint

Raise `canopy.squirrel.restore_hooks.base.RestoreHookError` when the hook
recognises a setpoint but can't work out its writes. The built-in hooks do
this for a magnet PV that doesn't end in `BDES`. Squirrel then writes
**nothing** for that PV, neither the setpoint nor any hook's extra writes,
and reports it as a failure with the error message. That avoids leaving a
device half-restored.

Any other exception is not caught and fails the whole restore job, so raise
`RestoreHookError` for anything about a single PV.

### Order and failures

Each hook's writes go out as one batch, after the setpoints and after every
hook listed before it. A hook only writes for PVs whose setpoint, and every
earlier hook's writes, succeeded. If one of its writes fails, the restore
reports the PV as failed under the hook's class name, and later hooks skip
it. If your hook must only run after another hook succeeds, say so in its
docstring, so whoever configures it lists them in that order.

## 2. Register it

Put the hook's module next to the built-in ones, in
`src/canopy/squirrel/restore_hooks/`, then register it in
`canopy.squirrel.restore_hooks.registry`. Each hook has a builder function
and an entry in `RESTORE_HOOK_PROVIDERS`; the built-in hooks are registered
like this:

```python
def _build_magnet_trim(_config: SquirrelConfig) -> RestoreHook:
    return MagnetTrimHook()


def _build_write_to_con(_config: SquirrelConfig) -> RestoreHook:
    return WriteToConHook()


RESTORE_HOOK_PROVIDERS: dict[str, Callable[[SquirrelConfig], RestoreHook]] = {
    "magnet_trim": _build_magnet_trim,
    "write_to_con": _build_write_to_con,
}
```

Add a builder for your hook and an entry under a new key. The key is the name
used in configuration. The builder gets Squirrel's configuration in case the
hook needs settings of its own, and runs once, the first time the hook is
loaded. Squirrel keeps the instance and reuses it for every restore.

## 3. Enable it

Add the new key to `CANOPY_SQUIRREL__RESTORE_HOOKS`, in the position it
should run. Set it for the worker as well as the API, restart the worker, and
check that the worker loaded it:

```
GET /squirrel/v1/health/restore-hooks
```

`worker.loaded` should list the new key. See
[Checking the configuration](../squirrel/restore_hooks.md#checking-the-configuration).

## 4. Test it

A hook is a plain class, so its rules can be unit-tested without a database
or EPICS. `tests/canopy/squirrel/test_restore_hooks.py` tests the built-in
hooks this way, for example:

```python
trim_hook = MagnetTrimHook()


def test_bdes_derives_ctrl_trim():
    assert trim_hook.derive_extra_writes("QUAD:LI21:201:BDES", 1.5) == [("QUAD:LI21:201:CTRL", "TRIM")]


def test_non_magnet_pv_yields_no_extra_writes():
    assert trim_hook.derive_extra_writes("KLYS:LI22:31:KVAC", 3.0) == []


def test_magnet_without_trailing_bdes_raises():
    with pytest.raises(RestoreHookError):
        trim_hook.derive_extra_writes("QUAD:LI21:201:BCON", 3.0)
```

Cover the setpoints the hook acts on, the ones it ignores, the ones it
rejects, and a `ca://` or `pva://` prefix.

To check how the hook behaves inside a whole restore, pass it straight to
`SnapshotService(..., restore_hooks=[YourHook()])`, as the restore tests in
`tests/canopy/squirrel/test_services_snapshot.py` do with a fake EPICS
layer. Those tests need Postgres; see [Testing](../squirrel/testing.md).
