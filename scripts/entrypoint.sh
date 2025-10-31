#!/usr/bin/env bash
set -euo pipefail

exec uvicorn bioterms.asgi:application "$@"
