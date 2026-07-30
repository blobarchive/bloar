#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
	echo "usage: $0 GOVULNCHECK_VERSION" >&2
	exit 2
fi

version=$1
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
allow_file=$script_dir/../security/govulncheck.allow
module=$(go list -m -f '{{.Path}}')
report=$(mktemp)
allowed=$(mktemp)
called=$(mktemp)
unexpected=$(mktemp)

cleanup() {
	rm -f -- "$report" "$allowed" "$called" "$unexpected"
}
trap cleanup EXIT HUP INT TERM

# JSON mode is parsed rather than trusting the presentation-oriented exit code:
# it contains module-, package-, and symbol-level findings. Only a trace that
# reaches this module is a called/reachable vulnerability.
go run "golang.org/x/vuln/cmd/govulncheck@$version" -json ./... >"$report"

awk 'NF && $1 !~ /^#/ {print $1}' "$allow_file" | sort -u >"$allowed"
jq -r --arg module "$module" '
	select(.finding != null)
	| select([.finding.trace[].module] | index($module))
	| .finding.osv
' "$report" | sort -u >"$called"
comm -23 "$called" "$allowed" >"$unexpected"

if [ -s "$unexpected" ]; then
	echo "govulncheck: unexpected reachable vulnerabilities in $module:" >&2
	sed 's/^/  /' "$unexpected" >&2
	echo "full JSON report: $report" >&2
	# Keep the report when failing so a CI artifact/logging wrapper can inspect
	# it; the explicit trap is disabled only on this path.
	trap - EXIT HUP INT TERM
	exit 1
fi

if [ -s "$called" ]; then
	echo "govulncheck $module: only explicitly accepted reachable advisories:"
	sed 's/^/  /' "$called"
else
	echo "govulncheck $module: no reachable vulnerabilities"
fi
