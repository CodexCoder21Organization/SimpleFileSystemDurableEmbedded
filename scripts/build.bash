#!/bin/bash
set -euo pipefail

if [ -z "${1:-}" ]; then
  echo "Builds must specify a buildrule as an argument." >&2
  echo "Usage: $(basename "$0") <build-rule> [output-file]" >&2
  exit 1
fi

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$SCRIPT_PATH/../jars"
JAR_PATH="$SCRIPT_PATH/../jars/KompileCli.jar"

if [ ! -f "$JAR_PATH" ]; then
  COURSIER_PATH="$SCRIPT_PATH/../jars/coursier"
  if [ ! -f "$COURSIER_PATH" ]; then
    curl -fLo "$COURSIER_PATH" "https://github.com/coursier/launchers/raw/master/coursier"
    chmod +x "$COURSIER_PATH"
  fi
  CLASSPATH=$("$COURSIER_PATH" fetch --repository https://kotlin.directory/ --repository central kompile.cli:kompile-cli:0.0.93 --classpath)
  apply_launcher="$SCRIPT_PATH/../jars/.KompileCli.launcher"
  printf '%s\n' '#!/bin/bash' "CLASSPATH='$CLASSPATH'" 'exec java $JAVA_OPTS -cp "$CLASSPATH" kompile.cli.CliKt "$@"' > "$apply_launcher"
  chmod +x "$apply_launcher"
  mv "$apply_launcher" "$JAR_PATH"
fi

REPO_PATH=$(cd "$SCRIPT_PATH/.." && pwd)
DEFAULT_CACHE_PATH="$HOME/.aibuildcaches/$(echo "$REPO_PATH" | sed 's|/|_|g')"
CACHE_PATH="${SIMPLE_FILESYSTEM_DURABLE_TEST_CACHE_PATH:-$DEFAULT_CACHE_PATH}"
"$JAR_PATH" --cache-location "$CACHE_PATH" -w "$REPO_PATH" "$@"
