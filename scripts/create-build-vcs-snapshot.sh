#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
	echo "usage: $0 OUTPUT_TAR EXPECTED_GIT_SHA" >&2
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
	echo "refusing to snapshot a dirty or untracked worktree" >&2
	git -C "$root" status --short >&2
	exit 1
}

snapshot_root=$(mktemp -d)
cleanup() {
	rm -rf -- "$snapshot_root"
}
trap cleanup EXIT HUP INT TERM

git -C "$snapshot_root" init --quiet repo

# Pack only the exact commit plus its recursive tree objects. Blob contents
# arrive through the separately allowlisted Docker context. Parent commits and
# historical blobs are neither needed by Go's VCS status check nor admitted.
objects=$snapshot_root/objects
{
	printf '%s\n' "$expected"
	git -C "$root" rev-parse "$expected^{tree}"
	git -C "$root" ls-tree -r -t "$expected" |
		awk '$2 == "tree" {print $3}'
} | sort -u >"$objects"
git -C "$root" pack-objects --stdout <"$objects" |
	git -C "$snapshot_root/repo" index-pack --stdin >/dev/null
printf '%s\n' "$expected" >"$snapshot_root/repo/.git/HEAD"
printf '%s\n' "$expected" >"$snapshot_root/repo/.git/shallow"
git -C "$snapshot_root/repo" read-tree "$expected"

# The Docker context contains only module files and tracked Go source. Mark
# every other tracked path sparse so its deliberate absence is not mistaken
# for a dirty build, while any missing/altered build input still fails status.
git -C "$snapshot_root/repo" ls-files -z |
	git -C "$snapshot_root/repo" update-index -z --skip-worktree --stdin
git -C "$snapshot_root/repo" ls-files -z -- \
	go.mod go.sum ':(glob)**/*.go' |
	git -C "$snapshot_root/repo" update-index -z --no-skip-worktree --stdin
git -C "$root" archive --format=tar "$expected" \
	go.mod go.sum ':(glob)**/*.go' |
	tar -C "$snapshot_root/repo" -xf -

test "$(git -C "$snapshot_root/repo" rev-parse --verify HEAD)" = "$expected"
test "$(git -C "$snapshot_root/repo" rev-list --count HEAD)" -eq 1
test -z "$(git -C "$snapshot_root/repo" status --porcelain --untracked-files=all)"
test -f "$snapshot_root/repo/.git/shallow" || {
	echo "build VCS snapshot is not shallow" >&2
	exit 1
}

# Only the metadata Go/git need for this one exact checkout enters the secret:
# no remotes, config credentials, refs, reflogs, hooks, blobs, or prior history.
tar -C "$snapshot_root/repo" -cf "$output" \
	.git/HEAD \
	.git/config \
	.git/index \
	.git/shallow \
	.git/objects \
	.git/refs
