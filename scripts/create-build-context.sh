#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
	echo "usage: $0 OUTPUT_DIR EXPECTED_GIT_SHA" >&2
	exit 2
fi

output=$1
expected=$2
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)

case "$expected" in
	????????????????????????????????????????)
		case "$expected" in
			*[!0-9a-f]*) echo "expected revision is not a lowercase full Git SHA" >&2; exit 1 ;;
		esac
		;;
	*) echo "expected revision is not a full Git SHA" >&2; exit 1 ;;
esac

actual=$(git -C "$root" rev-parse --verify HEAD)
test "$actual" = "$expected" || {
	echo "expected revision $expected does not match HEAD $actual" >&2
	exit 1
}
test -z "$(git -C "$root" status --porcelain --untracked-files=all)" || {
	echo "refusing to export a dirty or untracked worktree" >&2
	git -C "$root" status --short >&2
	exit 1
}
test ! -e "$output" || {
	echo "build context output already exists: $output" >&2
	exit 1
}
mkdir -p "$output"

# The production binaries are pure Go and use no go:embed, cgo, assembly, or
# generated runtime assets. Export only tracked Go/module inputs from the exact
# commit. Local .git, secrets, evidence, state, docs, and untracked files never
# become candidates for Docker context transfer.
test -z "$(git -C "$root" grep -l '^//go:embed' "$expected" -- '*.go')" || {
	echo "go:embed requires an explicit review and build-context allowlist update" >&2
	exit 1
}
git -C "$root" archive --format=tar "$expected" \
	go.mod go.sum ':(glob)**/*.go' |
	tar -C "$output" -xf -

test -f "$output/go.mod"
test -f "$output/go.sum"
test -f "$output/cmd/bloard/main.go"
if find "$output" ! -type f ! -type d -print -quit | grep . >/dev/null; then
	echo "sanitized build context contains an unexpected path type" >&2
	exit 1
fi
if find "$output" -type f \
	! -name '*.go' \
	! -path "$output/go.mod" \
	! -path "$output/go.sum" \
	-print -quit | grep . >/dev/null; then
	echo "sanitized build context contains an unexpected file" >&2
	exit 1
fi
test ! -e "$output/.git"
