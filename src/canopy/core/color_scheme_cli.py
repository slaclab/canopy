"""``canopy-scheme check <file>``: validate a color scheme file with canopy's flattener.

``check`` flattens the file onto ``canopy-default`` exactly as a canopy process serving it would, prints every error
(``error: <path>: <message>``) and every role left without a dark value (``no dark value: <role>``), and exits 1 when
there is any; a clean file prints ``<file>: ok (<n> roles)`` and exits 0. ``python -m canopy.core.color_scheme_cli``
runs the same command.

The converter package keeps the corpus tools that read Phoebus files, ``survey`` and ``from-colordef``, in its own
``pydmconverter-scheme`` command.
"""

from __future__ import annotations

import sys
import argparse
from typing import TextIO
from pathlib import Path

from canopy.core.color_scheme_flatten import load_scheme


def check(path: Path, out: TextIO) -> int:
    """Print the problems of the scheme file at ``path`` to ``out``; 1 when there is any, else 0."""
    result = load_scheme(path)
    errors = result.meta["errors"]
    no_dark = sorted(
        {e["path"].split("/")[-2] for e in errors if e["path"].endswith("/dark") and "no dark value" in e["message"]}
    )
    for err in errors:
        print(f"error: {err['path'] or '<file>'}: {err['message']}", file=out)
    for name in no_dark:
        print(f"no dark value: {name}", file=out)
    if errors:
        print(f"{path}: {len(errors)} error(s)", file=out)
        return 1
    print(f"{path}: ok ({len(result.flat['roles'])} roles)", file=out)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="canopy-scheme", description="Check a canopy-color-scheme/1 file.")
    sub = parser.add_subparsers(dest="command", required=True)
    p_check = sub.add_parser("check", help="validate a scheme file the way canopy flattens it")
    p_check.add_argument("file", type=Path)
    args = parser.parse_args(argv)
    return check(args.file, sys.stdout)


if __name__ == "__main__":
    raise SystemExit(main())
