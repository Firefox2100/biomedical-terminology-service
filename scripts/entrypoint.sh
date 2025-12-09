#!/usr/bin/env bash
set -euo pipefail

CONFIG_DIR="/app/conf"
DEFAULT_DIR="/app/conf.defaults"

mkdir -p "$CONFIG_DIR"

# List of config files we care about
FILES=(
  ".env"
  "uvicorn-log.config.yaml"
)

for f in "${FILES[@]}"; do
    if [ ! -f "${CONFIG_DIR}/${f}" ]; then
        echo "Config ${f} not found in ${CONFIG_DIR}, copying default."
        cp "${DEFAULT_DIR}/${f}" "${CONFIG_DIR}/${f}"
    fi
done

case "${1:-web}" in
    web)
        [ "$1" = "web" ] && shift
        exec uvicorn bioterms.asgi:application "$@"
        ;;
    worker)
        shift
        exec celery -A bioterms worker "$@"
        ;;
    *)
        echo "Unknown command: $1"
        exit 1
        ;;
esac
