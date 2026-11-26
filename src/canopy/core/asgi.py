"""Reading an ASGI scope the way the app's router does."""

from __future__ import annotations

from starlette.types import Scope


def route_path(scope: Scope) -> str:
    """The path the router matches: ``scope["path"]`` without the ``root_path`` prefix.

    Behind a path-prefixed ingress (``uvicorn --root-path /canopy``) the server puts the
    prefix in front of ``path``. This is Starlette's own rule, so the closed-meta gate,
    the history head's router and the app agree on every request: the prefix is
    stripped only at a path boundary, and a ``root_path`` with a trailing slash
    (``/canopy/``) strips nothing, for the router as for these.
    """
    path, root_path = scope["path"], scope.get("root_path", "")
    if not root_path or not path.startswith(root_path):
        return path
    if path == root_path:
        return ""
    if path[len(root_path)] == "/":
        return path[len(root_path) :]
    return path
