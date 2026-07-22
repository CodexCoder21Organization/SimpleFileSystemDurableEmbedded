#!/bin/bash
set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$SCRIPT_PATH/../jars"
JAR_PATH="$SCRIPT_PATH/../jars/KompileCli.jar"

if [ ! -f "$JAR_PATH" ]; then
  "$SCRIPT_PATH/build.bash" simplefilesystem.durable.buildMaven >/dev/null
fi

REPO_PATH=$(cd "$SCRIPT_PATH/.." && pwd)
CACHE_PATH="$HOME/.aibuildcaches/$(echo "$REPO_PATH" | sed 's|/|_|g')"
"$JAR_PATH" --cache-location "$CACHE_PATH" "$@"
