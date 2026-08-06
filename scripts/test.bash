#!/bin/bash
set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$SCRIPT_PATH/../jars"
JAR_PATH="$SCRIPT_PATH/../jars/KompileCli.jar"

if [ ! -f "$JAR_PATH" ]; then
  "$SCRIPT_PATH/build.bash" simplefilesystem.durable.buildMaven >/dev/null
fi

REPO_PATH=$(cd "$SCRIPT_PATH/.." && pwd)
DEFAULT_CACHE_PATH="$HOME/.aibuildcaches/$(echo "$REPO_PATH" | sed 's|/|_|g')"
CACHE_PATH="${SIMPLE_FILESYSTEM_DURABLE_TEST_CACHE_PATH:-$DEFAULT_CACHE_PATH}"
if [ "$#" -eq 0 ]; then
  set -- --test .
fi

FIXTURE_DIRECTORY=$(mktemp -d)
FIXTURE_JAR="$FIXTURE_DIRECTORY/cockroach-suite-fixture.jar"
FIXTURE_READY_FILE="$FIXTURE_DIRECTORY/jdbc-url"
FIXTURE_LOG="$FIXTURE_DIRECTORY/fixture.log"
FIXTURE_PID=""
# SHA-256 cache key for the process-owning fixture build rule. Remove only this lane-local result
# index; assembled artifacts stay cached.
FIXTURE_BUILD_RULE_CACHE_KEYS=(
  "6c6010b439cafd36cf71e51b5b01af46a2e46dcd791949f8853ef9463528c7d1"
)

cleanup_fixture() {
  if [ -n "$FIXTURE_PID" ] && kill -0 "$FIXTURE_PID" 2>/dev/null; then
    kill "$FIXTURE_PID"
    wait "$FIXTURE_PID" 2>/dev/null || true
  fi
  for CACHE_KEY in "${FIXTURE_BUILD_RULE_CACHE_KEYS[@]}"; do
    rm -f -- "$CACHE_PATH/buildRuleResultIndex/$CACHE_KEY.json"
  done
  rm -rf "$FIXTURE_DIRECTORY"
}
trap cleanup_fixture EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$JAR_PATH" \
  --cache-location "$CACHE_PATH" \
  -w "$REPO_PATH" \
  simplefilesystem.durable.buildCockroachSuiteFixtureFatJar \
  "$FIXTURE_JAR" >/dev/null

java -Xmx128m -jar "$FIXTURE_JAR" "$FIXTURE_READY_FILE" "$$" - >"$FIXTURE_LOG" 2>&1 &
FIXTURE_PID=$!

for _ in $(seq 1 1200); do
  if [ -s "$FIXTURE_READY_FILE" ]; then
    break
  fi
  if ! kill -0 "$FIXTURE_PID" 2>/dev/null; then
    echo "The shared CockroachDB suite fixture exited before becoming ready:" >&2
    sed -n '1,240p' "$FIXTURE_LOG" >&2
    exit 1
  fi
  sleep 0.1
done

if [ ! -s "$FIXTURE_READY_FILE" ]; then
  echo "The shared CockroachDB suite fixture did not become ready within 120 seconds:" >&2
  sed -n '1,240p' "$FIXTURE_LOG" >&2
  exit 1
fi

export SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL
SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL=$(tr -d '\r\n' <"$FIXTURE_READY_FILE")

# A directory selector dispatches through a fixed four-test pool. On the two-core CI worker,
# four simultaneous real database scenarios can consume their individual 30-second deadlines
# while competing for CPU. Kompile 0.0.93 preserves sequential dispatch for explicit file
# selectors, so expand only the canonical full-suite request. The legacy conformance script
# contains eight scenarios and is therefore selected once per function as well.
REQUESTED_ARGS=("$@")
PASSTHROUGH_ARGS=()
FULL_SUITE_SELECTOR_COUNT=0
OTHER_TEST_SELECTOR_COUNT=0
REQUESTED_LOG_PATH=""
ARGUMENT_INDEX=0
while [ "$ARGUMENT_INDEX" -lt "${#REQUESTED_ARGS[@]}" ]; do
  ARGUMENT="${REQUESTED_ARGS[$ARGUMENT_INDEX]}"
  case "$ARGUMENT" in
    --test)
      ARGUMENT_INDEX=$((ARGUMENT_INDEX + 1))
      if [ "$ARGUMENT_INDEX" -ge "${#REQUESTED_ARGS[@]}" ]; then
        echo "--test requires a selector" >&2
        exit 2
      fi
      TEST_SELECTOR="${REQUESTED_ARGS[$ARGUMENT_INDEX]}"
      if [ "$TEST_SELECTOR" = "." ]; then
        FULL_SUITE_SELECTOR_COUNT=$((FULL_SUITE_SELECTOR_COUNT + 1))
      else
        OTHER_TEST_SELECTOR_COUNT=$((OTHER_TEST_SELECTOR_COUNT + 1))
        PASSTHROUGH_ARGS+=("--test" "$TEST_SELECTOR")
      fi
      ;;
    --log)
      ARGUMENT_INDEX=$((ARGUMENT_INDEX + 1))
      if [ "$ARGUMENT_INDEX" -ge "${#REQUESTED_ARGS[@]}" ]; then
        echo "--log requires a path" >&2
        exit 2
      fi
      REQUESTED_LOG_PATH="${REQUESTED_ARGS[$ARGUMENT_INDEX]}"
      ;;
    *)
      PASSTHROUGH_ARGS+=("$ARGUMENT")
      ;;
  esac
  ARGUMENT_INDEX=$((ARGUMENT_INDEX + 1))
done

if [ "$FULL_SUITE_SELECTOR_COUNT" -ne 1 ] || [ "$OTHER_TEST_SELECTOR_COUNT" -ne 0 ]; then
  "$JAR_PATH" --cache-location "$CACHE_PATH" "$@"
  exit $?
fi

SUITE_LOG_DIRECTORY="$FIXTURE_DIRECTORY/suite-logs"
mkdir -p "$SUITE_LOG_DIRECTORY"
SUITE_STATUS=0
PRIORITY_TEST_ARGS=()
REMAINING_TEST_ARGS=()
CONFORMANCE_TEST_FILE="$REPO_PATH/tests/testConformanceSuiteCockroachBlobstore.kts"
PRIORITY_TEST_FILES=(
  "tests/testBoundedMaintenanceAndLargeTransitions.kts"
  "tests/testCockroachRetryAndGlobalLockOrder.kts"
)
for TEST_FILE in "${PRIORITY_TEST_FILES[@]}"; do
  PRIORITY_TEST_ARGS+=("--test" "$TEST_FILE")
done
while IFS= read -r TEST_FILE; do
  RELATIVE_TEST_FILE="${TEST_FILE#"$REPO_PATH/"}"
  if [ "$TEST_FILE" = "$CONFORMANCE_TEST_FILE" ]; then
    continue
  fi
  for PRIORITY_TEST_FILE in "${PRIORITY_TEST_FILES[@]}"; do
    if [ "$RELATIVE_TEST_FILE" = "$PRIORITY_TEST_FILE" ]; then
      continue 2
    fi
  done
  REMAINING_TEST_ARGS+=("--test" "$RELATIVE_TEST_FILE")
done < <(find "$REPO_PATH/tests" -maxdepth 1 -type f -name 'test*.kts' | sort)

if ! "$JAR_PATH" \
  --cache-location "$CACHE_PATH" \
  "${PASSTHROUGH_ARGS[@]}" \
  "${PRIORITY_TEST_ARGS[@]}" \
  --log "$SUITE_LOG_DIRECTORY/priority-tests.xml"; then
  SUITE_STATUS=1
fi

if ! "$JAR_PATH" \
  --cache-location "$CACHE_PATH" \
  "${PASSTHROUGH_ARGS[@]}" \
  "${REMAINING_TEST_ARGS[@]}" \
  --log "$SUITE_LOG_DIRECTORY/remaining-tests.xml"; then
  SUITE_STATUS=1
fi

CONFORMANCE_LOG_INDEX=0
while IFS= read -r TEST_METHOD; do
  CONFORMANCE_LOG_INDEX=$((CONFORMANCE_LOG_INDEX + 1))
  if ! "$JAR_PATH" \
    --cache-location "$CACHE_PATH" \
    "${PASSTHROUGH_ARGS[@]}" \
    --test "simplefilesystem.durable.$TEST_METHOD" \
    --log "$SUITE_LOG_DIRECTORY/conformance-$CONFORMANCE_LOG_INDEX.xml"; then
    SUITE_STATUS=1
  fi
done < <(sed -n 's/^fun \(test[A-Za-z0-9_]*\)(.*/\1/p' "$CONFORMANCE_TEST_FILE")

MERGED_LOG_PATH="$FIXTURE_DIRECTORY/full-suite.xml"
{
  printf '<tests>\n'
  while IFS= read -r BATCH_LOG; do
    sed '1d;$d' "$BATCH_LOG"
  done < <(find "$SUITE_LOG_DIRECTORY" -maxdepth 1 -type f -name '*.xml' | sort)
  printf '</tests>\n'
} >"$MERGED_LOG_PATH"

if [ -n "$REQUESTED_LOG_PATH" ]; then
  if [[ "$REQUESTED_LOG_PATH" != /* ]]; then
    REQUESTED_LOG_PATH="$PWD/$REQUESTED_LOG_PATH"
  fi
  cp "$MERGED_LOG_PATH" "$REQUESTED_LOG_PATH"
fi

exit "$SUITE_STATUS"
