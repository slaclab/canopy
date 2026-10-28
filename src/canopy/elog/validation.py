"""The ``{app}`` path-segment rule.

Mirrors magpie's app-name rule (``canopy.magpie.api.v1._validation``) so one
client app name works in both modules, without importing another module's
internals. Keep the two in sync.
"""

import re

# URL/index-friendly app names.
_APP_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
# Prose form of _APP_RE, for error messages. Kept adjacent so the two stay in sync.
APP_NAME_RULE = "allowed: lowercase letters, digits, '_', '-'; 1-64 characters"


def is_app(value: str) -> bool:
    return bool(_APP_RE.fullmatch(value))
