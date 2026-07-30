#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the verifier's duration knobs
# must be validated with a STRICT positive-integer grammar, for two reasons. A bare
# `[ "$val" -gt 0 ]` accepts values like "1 ", " 1", and "007": some ("1 ", "-5", "1+1")
# make the watchdog's `sleep "$WALL_CLOCK_MAX"` FAIL ("invalid time interval"/"invalid
# option") and silently disarm the terminal bound; the rest (" 1", "007", "0", "1.5",
# oversized) sleep would accept but are refused for strictness -- an ambiguous, zero, or
# absurd bound is not a meaningful deadline. This drives the REAL validate_durations with
# whitespace / leading-zero / expression / overflow / zero / negative / non-numeric /
# empty controls, and ALSO drives the REAL executable entry with a bad WALL_CLOCK_MAX to
# prove it aborts (exit 2, naming the knob) BEFORE arming anything. Needs neither root
# nor systemd. Exit 0 = the strict grammar holds.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/verify-token-credentials.sh"

# Run-scoped scratch: the real-entry check below makes the waiter create a bloar-hs.XXXXXX DIR
# under TMPDIR. Point TMPDIR at our OWN tree so it is removed with the tree at exit.
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
trap 'rm -rf "$_TESTTMP" 2>/dev/null' EXIT

fails=0
ok()  { printf 'ok   %s\n' "$*"; }
bad() { printf 'BAD  %s\n' "$*"; fails=$((fails + 1)); }

# --- (1) the REAL validate_durations rejects every malformed control ------------
# shellcheck source=/dev/null
source "$SCRIPT" # guarded main does not run
set +e

# reject_val VAR BADVALUE LABEL : validate_durations must die (nonzero) with VAR=BADVALUE
# and every other knob left at its valid default.
reject_val() {
	local var=$1 val=$2 label=$3
	(
		eval "$var=\$val"
		validate_durations
	) >/dev/null 2>&1
	[ $? -ne 0 ] && ok "rejects $label ($var='$val')" || bad "ACCEPTED $label ($var='$val') -- a malformed or ambiguous bound must be refused"
}
reject_val WALL_CLOCK_MAX '1 ' 'a trailing space'
reject_val WALL_CLOCK_MAX ' 1' 'a leading space'
reject_val CLEANUP_MAX '1+1' 'an arithmetic expression'
reject_val CURL_MAX '-5' 'a negative'
reject_val WALL_CLOCK_MAX '0' 'zero'
reject_val CLEANUP_MAX 'abc' 'a non-numeric'
reject_val CURL_MAX '1.5' 'a decimal'
reject_val WALL_CLOCK_MAX '007' 'a leading zero (octal ambiguity)'
reject_val CLEANUP_MAX '9999999999999999999' 'an overflowing number'
reject_val WALL_CLOCK_MAX '' 'an empty value'

# A strictly-valid set passes.
(validate_durations) >/dev/null 2>&1 && ok "accepts the shipped default durations" || bad "rejected the shipped defaults"
(
	WALL_CLOCK_MAX=1 CLEANUP_MAX=1 CURL_MAX=999999
	validate_durations
) >/dev/null 2>&1 && ok "accepts strict positive integers (1 and 999999)" || bad "rejected strict positive integers"

# --- (2) the REAL executable entry aborts on a bad duration BEFORE arming -------
# Run the actual script (which re-execs under setsid); a malformed WALL_CLOCK_MAX must
# make validate_durations die with exit 2 and name the knob, before any watchdog/work.
if command -v setsid >/dev/null 2>&1; then
	out="$(WALL_CLOCK_MAX='1 ' bash "$SCRIPT" 2>&1)"
	rc=$?
	set +e
	{ [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q "WALL_CLOCK_MAX='1 '"; } &&
		ok "the real entry aborts (exit 2) on a bad WALL_CLOCK_MAX, naming the knob, before arming" ||
		bad "the real entry did not cleanly reject a bad WALL_CLOCK_MAX (rc=$rc, out='$out')"
else
	ok "SKIP real-entry abort subtest (setsid not available)"
fi

if [ "$fails" -eq 0 ]; then
	echo "DURATION-GRAMMAR REGRESSION: PASS"
	exit 0
fi
echo "DURATION-GRAMMAR REGRESSION: $fails FAILED"
exit 1
