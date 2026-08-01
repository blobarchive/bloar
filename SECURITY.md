# Security policy

## Supported versions

Security fixes are made on the current release line. During the `v0.x` period,
only the latest published version is supported.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting for this repository:

1. open the repository's **Security** tab;
2. choose **Advisories**; and
3. select **Report a vulnerability**.

Do not open a public issue for an undisclosed vulnerability. Include affected
versions, deployment assumptions, impact, reproduction steps, and any proposed
mitigation. Avoid accessing data or systems you do not own while reproducing a
problem.

The maintainers will acknowledge a complete report within three business days
and will coordinate validation, remediation, release, and disclosure. This is
a response target, not a vulnerability-reward program or an uptime SLA.

## Scope boundaries

The public BlobArchive service is libp2p/Bitswap plus authenticated
DNSLink/IPNS publication. A raw public writer HTTP ingest endpoint is not part
of the supported deployment. Reports which require exposing private ingest,
Kubo RPC, or metrics should explain why that exposure is expected rather than a
deployment-policy violation.

Correctness claims, trust boundaries, and supported operating procedures are
specified in `docs/spec.md`, `docs/follow-profiles.md`, and
`docs/operations.md`.

Release authority, required GitHub repository controls, immutable image
identity, SBOM/provenance checks, and operator verification are documented in
`docs/releases.md`.

## Accepted upstream availability advisory

The release vulnerability gate has one explicit reachable allowlist entry:
`GO-2024-3218`, the upstream Kademlia DHT censorship/eclipse advisory. It has no
fixed `go-libp2p-kad-dht` version. This is an availability threat, not an
authorization or content-integrity bypass: a hostile routing-table view can
delay or suppress discovery, while DNSLink/IPNS signatures and CID verification
still reject forged authority and bytes.

BlobArchive does not treat the public DHT as its trust root. Operators can pin
independent writer authorities, retain last-good generations, use reviewed
static/out-of-band peer hints, and monitor each source independently. The
allowlist is exact and machine-checked; any other reachable advisory fails CI
and release. Remove the entry when upstream ships a complete fix rather than
expanding it to a module-wide exemption.
