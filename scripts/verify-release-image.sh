#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
	echo "usage: $0 IMAGE@sha256:DIGEST EXPECTED_GIT_SHA OUTPUT_DIR bloar|edge" >&2
	exit 2
fi

image_ref=$1
expected_revision=$2
output_dir=$3
image_kind=$4
expected_source=https://github.com/blobarchive/bloar

case "$image_kind" in
	bloar)
		binaries="bloard bloar-index bloar-kubo-replica bloar-swarm-inspect"
		;;
	edge)
		binaries="bloar-edge"
		;;
	*)
		echo "image kind must be bloar or edge, got: $image_kind" >&2
		exit 2
		;;
esac

case "$image_ref" in
	*@sha256:????????????????????????????????????????????????????????????????) ;;
	*)
		echo "release verification requires an immutable image digest: $image_ref" >&2
		exit 1
		;;
esac
expected_digest=${image_ref#*@}
case "${expected_digest#sha256:}" in
	*[!0-9a-f]*)
		echo "release verification requires a lowercase sha256 digest: $image_ref" >&2
		exit 1
		;;
esac
case "$expected_revision" in
	????????????????????????????????????????)
		case "$expected_revision" in
			*[!0-9a-f]*)
				echo "expected revision is not a lowercase full Git SHA" >&2
				exit 1
				;;
		esac
		;;
	*)
		echo "expected revision is not a full Git SHA" >&2
		exit 1
		;;
esac

image_name=${image_ref%@sha256:*}
test ! -e "$output_dir" || {
	echo "release evidence output already exists: $output_dir" >&2
	exit 1
}
mkdir -p "$output_dir"

manifest_json=$output_dir/image-manifest.json
sbom_map=$output_dir/image-sbom-map.json
provenance_map=$output_dir/image-provenance-map.json

docker buildx imagetools inspect "$image_ref" --format '{{ json .Manifest }}' >"$manifest_json"
actual_digest=$(jq -er '.digest' "$manifest_json")
test "$actual_digest" = "$expected_digest" || {
	echo "registry digest $actual_digest does not match workflow digest $expected_digest" >&2
	exit 1
}

platforms=$(jq -c '
	[.manifests[]
	  | select(.platform.os != "unknown")
	  | [.platform.os, .platform.architecture]]
	| sort
' "$manifest_json")
test "$platforms" = '[["linux","amd64"],["linux","arm64"]]' || {
	echo "release index has unexpected runnable platforms: $platforms" >&2
	exit 1
}

amd64_digest=$(jq -er '
	.manifests[]
	| select(.platform.os == "linux" and .platform.architecture == "amd64")
	| .digest
' "$manifest_json")
arm64_digest=$(jq -er '
	.manifests[]
	| select(.platform.os == "linux" and .platform.architecture == "arm64")
	| .digest
' "$manifest_json")
for child_digest in "$amd64_digest" "$arm64_digest"; do
	case "$child_digest" in
		sha256:????????????????????????????????????????????????????????????????) ;;
		*)
			echo "registry returned an invalid child digest: $child_digest" >&2
			exit 1
			;;
	esac
	case "${child_digest#sha256:}" in
		*[!0-9a-f]*)
			echo "registry returned a non-hex child digest: $child_digest" >&2
			exit 1
			;;
	esac
done

cat >"$output_dir/child-digests.env" <<EOF
AMD64_DIGEST=$amd64_digest
ARM64_DIGEST=$arm64_digest
EOF

docker buildx imagetools inspect "$image_ref" --format '{{ json .SBOM }}' >"$sbom_map"
docker buildx imagetools inspect "$image_ref" --format '{{ json .Provenance }}' >"$provenance_map"

containers=
cleanup() {
	for container in $containers; do
		docker rm -f "$container" >/dev/null 2>&1 || true
	done
}
trap cleanup EXIT HUP INT TERM

for arch in amd64 arm64; do
	platform=linux/$arch
	case "$arch" in
		amd64) child_digest=$amd64_digest ;;
		arm64) child_digest=$arm64_digest ;;
	esac

	jq -e --arg platform "$platform" '
		.[$platform].SPDX
		| (.spdxVersion | startswith("SPDX-"))
		  and (.SPDXID == "SPDXRef-DOCUMENT")
	' "$sbom_map" >/dev/null
	jq -e --arg platform "$platform" '
		.[$platform].SLSA
		| (.buildDefinition.buildType | type == "string")
		  and (.runDetails.builder.id | type == "string")
	' "$provenance_map" >/dev/null
	jq --arg platform "$platform" '.[$platform].SPDX' "$sbom_map" \
		>"$output_dir/sbom-$arch.spdx.json"

	child_ref=$image_name@$child_digest
	docker pull --platform "$platform" "$child_ref" >/dev/null
	container=$(docker create --platform "$platform" "$child_ref")
	containers="$containers $container"
	extract_dir=$output_dir/extract-$arch
	mkdir -p "$extract_dir"

	revision=$(docker image inspect "$child_ref" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')
	source=$(docker image inspect "$child_ref" --format '{{ index .Config.Labels "org.opencontainers.image.source" }}')
	created=$(docker image inspect "$child_ref" --format '{{ index .Config.Labels "org.opencontainers.image.created" }}')
	test "$revision" = "$expected_revision" || {
		echo "$platform OCI revision $revision does not match $expected_revision" >&2
		exit 1
	}
	test "$source" = "$expected_source" || {
		echo "$platform OCI source $source does not match $expected_source" >&2
		exit 1
	}
	test -n "$created" || {
		echo "$platform OCI created label is empty" >&2
		exit 1
	}

	for binary in $binaries; do
		docker cp "$container:/$binary" "$extract_dir/$binary"
		build_info=$(go version -m "$extract_dir/$binary")
		module_path=$(printf '%s\n' "$build_info" | awk '$1 == "mod" { print $2; exit }')
		test "$module_path" = "github.com/blobarchive/bloar" || {
			echo "$platform/$binary embeds unexpected module ${module_path:-<absent>}" >&2
			exit 1
		}
		printf '%s\n' "$build_info" | grep -F "vcs.revision=$expected_revision" >/dev/null || {
			echo "$platform/$binary does not embed $expected_revision" >&2
			exit 1
		}
		printf '%s\n' "$build_info" | grep -F 'vcs.modified=false' >/dev/null || {
			echo "$platform/$binary is dirty or lacks clean VCS metadata" >&2
			exit 1
		}
	done

	docker rm "$container" >/dev/null
	containers=$(printf '%s\n' "$containers" | sed "s/ $container//")
done

echo "$image_kind release image verified: $image_ref"
