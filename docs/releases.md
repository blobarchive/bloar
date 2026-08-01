# Release authority and verification

Bloar's release workflow is fail-closed around two immutable container-image
digests: `ghcr.io/blobarchive/bloar` for the follower/indexer/replica tools, and
`ghcr.io/blobarchive/bloar-edge` for the isolated Internet-facing edge. It
builds and pushes only their `v0.X.Y` tags first, verifies both complete
multi-platform images, signs provenance and per-platform SBOM attestations, and
only then points both `latest` tags at the already-verified digests. A GitHub
Release is created last and carries `bloar-v0.X.Y-image.env`, whose
`BLOAR_IMAGE_DIGEST=sha256:...` value is the input required by both distributed
Compose quickstarts and whose `BLOAR_EDGE_IMAGE_DIGEST=sha256:...` value binds
the public-edge artifact to the same reviewed source.

Tags remain mutable registry pointers. The two digests are the deployment
identities.

## Repository release controls

Workflow code cannot configure or prove every GitHub policy that authorizes
itself. These controls must remain configured and be reviewed before every
public release tag:

1. Protect `main` with a ruleset that requires pull requests, review, the full
   CI status set, and linear/non-force-pushed history; restrict bypass and
   direct pushes to the minimum maintainer set. The workflow independently
   refuses a release SHA that is not an ancestor of GitHub's protected `main`.
2. Create an Actions environment named **`release`**. Require interactive
   approval from the designated human release maintainer, restrict deployment
   to the protected release-tag pattern, and add no stored environment secret
   the build does not require. The maintainer may also have initiated the
   workflow; this is a single-human gate, not an independent-review guarantee.
   `publish` cannot start until approval, and the release job can only follow a
   successful approved publish.
3. Protect `v0.*.*` tags against update and deletion and restrict their
   creation. A release tag must be an annotated tag whose signature GitHub
   reports as verified. The workflow rejects lightweight, unsigned/unverified,
   non-semantic, non-monotonic, off-main, or wrong-target tags.
4. Keep GitHub Actions restricted to the pinned Actions in this repository.
   Every `uses:` line is a reviewed full commit SHA. Updating an Action means
   reviewing the old-to-new upstream diff and changing the readable version
   comment and SHA together.
5. Make both GHCR packages public before distribution and disallow manual
   retagging as an operational practice. If a tag and digest disagree, the
   digest in the signed release evidence wins and the mismatch is an incident.

The `release` environment is a human control, but the workflow can prove only
that it names the environment. Reviewer assignment, tag restrictions,
interactive approval, and effective external-automation permissions live in
GitHub and must be verified there.

Automation credentials must have no `actions:write` and no standing
`contents:write` on the repository. Verify effective token permissions rather
than inferring them from workflow YAML. Any temporary write grant requires
fresh human approval and immediate revocation after use.

## What the workflow verifies

Before `latest` or a GitHub Release can move, the workflow checks:

- stable, newest-on-the-v0-line tag syntax; annotated verified tag signature;
  exact tag target; protected-main ancestry;
- build, tests, lint, Nitro conformance, P2P smoke tests, both module graphs'
  exact quic-go v0.59.1 pin, and reachable `govulncheck`;
- a generated Docker context containing only tracked Go/module inputs plus a
  clean one-commit tree-only Git snapshot secret, with checkout credentials
  disabled and layer-cache reuse forbidden so this release must reevaluate the
  VCS snapshot;
- digest-pinned BuildKit, QEMU, Go builder, and distroless runtime inputs;
- both the default and edge artifacts; exactly `linux/amd64` and `linux/arm64`
  for each; both immutable registry index digests; OCI
  source/revision/created labels on every child image; and every shipped
  binary's public module path plus exact clean embedded VCS revision;
- BuildKit SBOM and provenance predicates for both platforms of both images;
- GitHub/Sigstore provenance for both image indexes and SPDX SBOM attestations
  for all four child manifests, verified back to this repository, workflow,
  tag ref, and source commit.

The workflow never places `.git`, local secrets, evidence, state, or checkout
credentials in the ordinary Docker context or image. A minimal Git snapshot
with no remotes, credentials, refs, reflogs, hooks, blobs, or prior history is
mounted as a BuildKit secret and extracted only into an ephemeral tmpfs mount
for the single build step; it cannot enter a committed layer.
Layer-cache reuse is disabled because BuildKit does not include secret contents
or required-secret presence in its cache key. Go module and compiler cache
mounts remain enabled.

## Verify as an operator

After downloading `bloar-v0.X.Y-image.env` from the GitHub Release, inspect it
and use the immutable reference:

```sh
set -a
. ./bloar-v0.X.Y-image.env
set +a

for digest in "$BLOAR_IMAGE_DIGEST" "$BLOAR_EDGE_IMAGE_DIGEST"; do
  case "$digest" in
    sha256:????????????????????????????????????????????????????????????????) ;;
    *) echo "invalid release digest" >&2; exit 1 ;;
  esac
  case "${digest#sha256:}" in
    *[!0-9a-f]*) echo "invalid release digest" >&2; exit 1 ;;
  esac
done

IMAGE="ghcr.io/blobarchive/bloar@$BLOAR_IMAGE_DIGEST"
EDGE_IMAGE="ghcr.io/blobarchive/bloar-edge@$BLOAR_EDGE_IMAGE_DIGEST"
docker pull "$IMAGE"
docker pull "$EDGE_IMAGE"
for image in "$IMAGE" "$EDGE_IMAGE"; do
  gh attestation verify "oci://$image" \
    --repo blobarchive/bloar \
    --signer-workflow blobarchive/bloar/.github/workflows/release.yml
done
```

Then run the chosen quickstart from its directory with
`BLOAR_IMAGE_DIGEST` still exported. Compose refuses to substitute a tag or an
empty default.
