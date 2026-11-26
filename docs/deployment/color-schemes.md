# Color schemes

Converted screens do not hard-code their named colors. Where the source panel
used a named color (a Phoebus named color, or an EDM `colors.list` entry), the
converted widget carries a **color role** instead, and the viewer paints the role
from the active **color scheme** in light or dark mode.

Every deployment ships the built-in scheme `canopy-default`. Its source is
`src/canopy/core/data/canopy-default.json`; the PyDM converter package carries a
byte copy of it. A facility can layer its own scheme file on top of it to change
any role, add roles, or change how source color names map onto roles.

Canopy flattens scheme files itself (`canopy.core.color_scheme`). That needs no
optional extra, so a canopy install without the `convert` extra can still
flatten and serve the default scheme or a facility scheme.

## Color roles

A colored property holds either a plain hex string or a role object:

```json
{ "$role": "MAJOR", "fallback": "#da1e28", "fallbackDark": "#ff8389" }
```

| Key | Required | Meaning |
|-----|----------|---------|
| `$role` | yes | Role name, 1-64 characters |
| `fallback` | yes | Hex color (`#rrggbb` or `#rrggbbaa`) used when the scheme has no such role |
| `fallbackDark` | no | Hex color used in dark mode when the scheme has no such role |

The viewer resolves a role in this order: the scheme's value for the role in the
active mode, then `fallbackDark` (dark mode only), then `fallback`. A role the
scheme does not define therefore still paints its fallback. A converted screen
that contains a role is stamped `schemaVersion` `1.1`, otherwise `1.0`.

`canopy-default` defines these roles: `Transparent`, `OK`, `MINOR`, `MAJOR`,
`INVALID`, `DISCONNECTED`, `Text`, `ActiveText`, `Background`,
`Read_Background`, `Write_Background`, `Button_Background`, `Grid`, `Grid_Fill`,
`Display_Background`, `On`, `Off`, `Header_Background`, `Header_ForeGround`,
`Attention`, `STOP`.

## The scheme file format

A scheme file is JSON in the `canopy-color-scheme/1` format, defined by
canopy-spec's `screen-ir/color-scheme.schema.json` (canopy carries a copy as
`src/canopy/core/data/color-scheme.schema.json`):

```json
{
  "format": "canopy-color-scheme/1",
  "id": "my-facility",
  "extends": "canopy-default",
  "roles": {
    "OK": "#00a000",
    "Display_Background": { "light": "#f0f0f0" },
    "Beam_On": { "light": "#ffcc00", "dark": "#e0b000" },
    "Read_Background": "@Background"
  },
  "import": {
    "phoebus": { "Beam On": "Beam_On" },
    "edm": { "invisible": null }
  }
}
```

| Key | Required | Meaning |
|-----|----------|---------|
| `id` | yes | Non-empty scheme name, reported in `meta.id`. Give your facility its own name: `canopy-default` is the built-in scheme's, and a file that keeps it reads like the fallback (it is served, with a `WARNING` line logged) |
| `format` | no | `canopy-color-scheme/1`, the only value allowed. A file without it is read as that format |
| `$schema` | no | For editors; ignored |
| `extends` | no | Only `canopy-default` is allowed, and it is the default when omitted. There are no deeper chains |
| `roles` | no | Role name (1-64 characters) to role value |
| `import` | no | `phoebus` and/or `edm`: source color name to role name |

No other top-level keys are allowed.

A **role value** is one of:

- a hex color, `#rgb`, `#rrggbb` or `#rrggbbaa`, used for both modes;
- an alias `"@Other"`, meaning "the same color as role `Other`", for both modes;
- an object `{ "light": ..., "dark": ... }` holding a hex or an alias per mode.
  A mode you omit is inherited from `canopy-default`. The object may also carry
  `phoebus`, the Phoebus color (a hex) the role restyles: the converter then
  writes the role only where a screen's color equals that value.

Your file is merged onto `canopy-default` **per role and per mode**: roles you do
not mention keep their default colors, and new roles are added. Every role must
end up with both a light and a dark value after inheritance. Hex values are
canonicalised (lower case, `#rgb` expanded, a fully opaque `ff` alpha dropped).

The **import maps** say which role a source color name becomes during
conversion. A target is a role name, or a slot object such as
`{ "role": "Grid", "background_color": "Grid_Fill" }` that picks the role by the
source property the color is used for (`role` is the default). The maps merge
onto the defaults the same way: an entry replaces the inherited one whole,
mapping a name to `null` deletes the inherited mapping, and every target must be
a role that exists after the merge.

### Errors reject the whole file

If the file cannot be read, is not valid JSON, fails the format, has a dangling or
cyclic alias, imports onto a role that does not exist, or leaves a role without a
light or dark value, the **whole file is rejected** and `canopy-default` is served
instead. Every problem found is listed in `meta.errors`, each process logs one
`ERROR` line, the converter's `GET /converter/v1/health/summary` reports
`color-scheme` in `degraded`, and every conversion carries one warning naming the
first error. [Check the file](#checking-a-scheme-file) before deploying it.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CANOPY_THEME_FILE` | unset (`canopy-default`) | Path to the deployment's scheme file. The converter uses it unless `CANOPY_CONVERTER__SCHEME_PATH` is set; any other process that serves a scheme through `canopy.core.color_scheme` reads it too |
| `CANOPY_CONVERTER__SCHEME_PATH` | unset | Scheme file for the converter only. Wins over `CANOPY_THEME_FILE` |
| `CANOPY_CONVERTER__COLOR_DEF_PATH` | unset | Opt-in Phoebus `color.def` the converter resolves named colors from. See [Matching a facility color.def](#matching-a-facility-colordef) |

All three are settings like any other `CANOPY_` variable, so they can also be set
in the `.env` file. An empty or whitespace-only value counts as unset. The
converter's precedence is `CANOPY_CONVERTER__SCHEME_PATH`, then
`CANOPY_THEME_FILE`, then `canopy-default`.

```bash
CANOPY_THEME_FILE=/etc/canopy/my-facility-colors.json
```

Viewers get their scheme from the server that serves them, not from this API.
Point that server ([canopy-mcp](https://github.com/slaclab/canopy-mcp)) at the
**same file**, so converted screens and the viewer agree on every role.

## Changing a scheme: restart

!!! warning "There is no live reload"
    Each process reads and flattens the scheme file **once, at startup**, and
    keeps it for the life of the process. After you edit the file:

    1. [Check it](#checking-a-scheme-file).
    2. Restart **canopy-api** and the **canopy-worker** (batch conversions run
       there, from the same image), and any other server that serves the
       scheme to viewers.
    3. Reload every open viewer so it fetches the new scheme.

    A process you do not restart keeps converting or serving the old scheme.

## Checking what is used

The converter's summary reports the scheme canopy-api loaded at startup, the one
its conversions use (an edited environment changes neither until a restart). It
asks for a read key:

| Route | Healthy | Scheme rejected |
|-------|---------|-----------------|
| `GET /health` | `{"status": "healthy"}` | `{"status": "healthy"}` |
| `GET /converter/v1/health/summary` | `{"status": "ok", "degraded": []}` | `{"status": "degraded", "degraded": ["color-scheme"]}` |

Responses of `POST /converter/v1/conversions` and
`POST /converter/v1/conversions/validate` carry `scheme`:
`{ "id", "served", "sha256", "errors" }`, describing the scheme that conversion
used. A non-empty `errors` is what shows the file was rejected: it lists why,
each as `{ "path", "message" }`, and `served` is then `canopy-default`. A file
that loaded has `errors: []` and `served` equal to its own `id`. Batch rows
carry only the warning.

The flattened form, which a server hands to viewers, is what
`canopy.core.color_scheme.get_color_scheme().flat_json` returns:

```json
{
  "roles": ["Transparent", "OK", "..."],
  "light": ["#ffffff00", "#24a148", "..."],
  "dark": ["#16161600", "#42be65", "..."],
  "meta": {
    "id": "my-facility",
    "sha256": "<digest of the file bytes>",
    "flatHash": "<digest of the flat scheme served>",
    "served": "my-facility",
    "errors": []
  }
}
```

`roles`, `light` and `dark` are parallel lists: the `canopy-default` roles in
their order, then the facility's new roles sorted by name.

## Checking a scheme file

Canopy's `canopy-scheme` command checks a file with the same flattener canopy
serves with. It needs no optional extra (`python -m canopy.core.color_scheme_cli`
runs the same command):

```bash
# Prints every error and every role without a dark value, and exits 1 if
# there is any; otherwise prints "<file>: ok (N roles)".
canopy-scheme check my-facility-colors.json
```

The tools that read Phoebus files come with the converter package (PyDMConverter)
as its `pydmconverter-scheme` command:

```bash
# Count the named colors a .bob corpus uses, per (name, slot, saved RGB),
# and with --scheme say what each becomes.
pydmconverter-scheme survey panels/ --scheme my-facility-colors.json [--corpus-root panels/]

# Write the light value of every name in a color table into a scheme file,
# keeping each role's dark value. Without --color-def the built-in Phoebus
# table is used. --id is required when the target file does not exist yet.
pydmconverter-scheme from-colordef --color-def color.def --extends canopy-default \
    --into my-facility-colors.json --id my-facility
```

## Matching a facility color.def

By default the converter resolves Phoebus named colors from its built-in table,
which keeps the light look of converted screens as it is today.
`CANOPY_CONVERTER__COLOR_DEF_PATH` is an **opt-in** switch for a facility that
wants its own `color.def` instead. To switch:

1. Regenerate the scheme's light values from the same file:
   `pydmconverter-scheme from-colordef --color-def <color.def> --extends canopy-default --into <scheme file>`.
2. Set `CANOPY_CONVERTER__COLOR_DEF_PATH=<color.def>` for canopy-api and the
   worker.
3. Check the scheme, restart and reload viewers, as
   [above](#changing-a-scheme-restart).
