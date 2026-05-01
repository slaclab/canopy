#!/usr/bin/env bash
set -e

uv sync --group dev
uv run pre-commit install
uv lock
