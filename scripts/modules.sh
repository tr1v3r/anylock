#!/bin/sh
# Run a command from every module root, without silently skipping nested drivers.
set -eu
cd "$(dirname "$0")/.."
if [ "$#" -eq 0 ]; then
    printf 'usage: %s command [args...]\n' "$0" >&2
    exit 2
fi
find . -name go.mod -not -path './vendor/*' -not -path './docs/experiments/*' | sort |
while IFS= read -r module_file; do
    module_dir=${module_file%/go.mod}
    printf 'module: %s\n' "$module_dir"
    (cd "$module_dir" && "$@")
done
