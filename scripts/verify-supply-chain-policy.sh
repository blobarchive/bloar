#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
set --
for workflow in .github/workflows/*.yml .github/workflows/*.yaml; do
	[ -f "$workflow" ] && set -- "$@" "$workflow"
done
[ "$#" -gt 0 ] || {
	echo "supply-chain policy: no workflow files found" >&2
	exit 1
}

fail() {
	echo "supply-chain policy: $*" >&2
	exit 1
}

# Every third-party Action must be bound to one reviewed commit, never a
# mutable major tag. Local actions (./path) would be exempt, but none exist.
uses_count=0
while IFS= read -r use; do
	uses_count=$((uses_count + 1))
	case "$use" in
		./*) ;;
		*@????????????????????????????????????????)
			sha=${use##*@}
			case "$sha" in
				*[!0-9a-f]*) fail "Action is not pinned to a lowercase full commit SHA: $use" ;;
			esac
			;;
		*) fail "Action is not pinned to a full commit SHA: $use" ;;
	esac
done <<EOF
$(awk '
	match($0, /uses:[[:space:]]*[^[:space:]#]+/) {
		value = substr($0, RSTART, RLENGTH)
		sub(/^uses:[[:space:]]*/, "", value)
		print value
	}
' "$@")
EOF
test "$uses_count" -gt 0 || fail "no Actions found"

checkout_count=$(grep -h -c 'uses: actions/checkout@' "$@" | awk '{s += $1} END {print s + 0}')
credential_count=$(grep -h -c 'persist-credentials: false' "$@" | awk '{s += $1} END {print s + 0}')
test "$checkout_count" -eq "$credential_count" ||
	fail "every checkout must set persist-credentials:false ($checkout_count checkouts, $credential_count guards)"
if grep -h -E 'runs-on:[[:space:]]+ubuntu-latest' "$@" >/dev/null; then
	fail "GitHub-hosted runners must name an Ubuntu release, not ubuntu-latest"
fi

# Every FROM line must bind the named builder/runtime image to an immutable
# sha256 manifest. The BuildKit daemon and QEMU helper are checked separately
# because they are workflow inputs rather than Dockerfile stages.
while IFS= read -r from; do
	image=$(printf '%s\n' "$from" | awk '{for (i=2; i<=NF; i++) if ($i !~ /^--/) {print $i; exit}}')
	case "$image" in
		*@sha256:????????????????????????????????????????????????????????????????) ;;
		*) fail "Dockerfile base is not digest-pinned: $from" ;;
	esac
done <<EOF
$(grep -E '^[[:space:]]*FROM[[:space:]]' deploy/Dockerfile)
EOF

grep -Eq 'image:[[:space:]]+docker\.io/tonistiigi/binfmt:[^[:space:]]+@sha256:[0-9a-f]{64}' .github/workflows/release.yml ||
	fail "QEMU helper image is not digest-pinned"
grep -Eq 'image=docker\.io/moby/buildkit:[^[:space:]]+@sha256:[0-9a-f]{64}' .github/workflows/release.yml ||
	fail "BuildKit daemon image is not digest-pinned"

first_rule=$(awk 'NF && $1 !~ /^#/ {print; exit}' .dockerignore)
test "$first_rule" = "**" || fail ".dockerignore must start from a deny-all rule"
test -x scripts/create-build-context.sh ||
	fail "sanitized build-context generator is absent or not executable"
test -x scripts/create-build-vcs-snapshot.sh ||
	fail "minimal VCS snapshot generator is absent or not executable"
# These are literal workflow expressions, not shell interpolation.
# shellcheck disable=SC2016
grep -Fq 'context: ${{ steps.release.outputs.build_context }}' .github/workflows/release.yml ||
	fail "release build does not use the sanitized generated context"
# shellcheck disable=SC2016
grep -Fq 'vcs_snapshot=${{ steps.release.outputs.vcs_snapshot }}' .github/workflows/release.yml ||
	fail "release build does not require the minimal VCS snapshot secret"
build_count=$(grep -c 'uses: docker/build-push-action@' .github/workflows/release.yml)
no_cache_count=$(grep -c 'no-cache: true' .github/workflows/release.yml)
test "$build_count" -eq 2 ||
	fail "release must build exactly the Bloar and public-edge artifacts"
test "$no_cache_count" -eq "$build_count" ||
	fail "every release build must reevaluate the required VCS secret"
grep -Fq 'EDGE_IMAGE_NAME: blobarchive/bloar-edge' .github/workflows/release.yml ||
	fail "release has no independent immutable public-edge artifact"
grep -Fq 'target: edge' .github/workflows/release.yml ||
	fail "public-edge release does not select the hardened edge target"
grep -Fq 'BLOAR_EDGE_IMAGE_DIGEST=' .github/workflows/release.yml ||
	fail "release handoff omits the immutable public-edge digest"
grep -Fq 'release-evidence/edge edge' .github/workflows/release.yml ||
	fail "release does not verify the public-edge binary and metadata"
attest_count=$(grep -c 'uses: actions/attest@' .github/workflows/release.yml)
test "$attest_count" -eq 6 ||
	fail "release must attest both indexes and all four platform SBOMs"
grep -Fq 'group: bloar-stable-release' .github/workflows/release.yml ||
	fail "stable release workflows are not serialized"
test "$(grep -cF 'is not the newest stable v0 tag' .github/workflows/release.yml)" -eq 1 ||
	fail "release monotonicity is not checked before human approval"
test "$(grep -cF 'is no longer the newest stable v0 tag' .github/workflows/release.yml)" -eq 1 ||
	fail "release monotonicity must be checked before and after human approval"
grep -Fq 'environment:' .github/workflows/release.yml ||
	fail "release publish job has no protected environment"
grep -Fq 'name: release' .github/workflows/release.yml ||
	fail "release environment must be named release"
# This is a literal workflow expression, not shell interpolation.
# shellcheck disable=SC2016
grep -Fq 'git merge-base --is-ancestor "$GITHUB_SHA" refs/remotes/origin/main' .github/workflows/release.yml ||
	fail "release workflow does not enforce protected-main ancestry"

for compose in deploy/quickstart/follower/compose.yaml deploy/quickstart/kubo-replica/compose.yaml; do
	# shellcheck disable=SC2016
	grep -Fq 'image: ghcr.io/blobarchive/bloar@${BLOAR_IMAGE_DIGEST:?' "$compose" ||
		fail "$compose does not fail closed on a release digest"
done

echo "supply-chain policy: OK"
