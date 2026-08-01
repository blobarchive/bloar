#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the executable entry
# must not trust caller-controlled inputs -- neither an arithmetic-injecting duration knob, nor an
# inherited wall deadline that would EXTEND the bound, nor inherited protocol/test hooks on a fresh
# isolated entry.
#  A) arithmetic injection: a WALL_CLOCK_MAX like `BASH_SOURCE[$(cmd)]` must be REJECTED by the
#     strict-regex validation that runs BEFORE any `$(( ))`, so the subscript command-sub never
#     executes. (Remove the guard validate_durations and the marker is touched -> injection.)
#  B) inherited deadline: an isolated entry handed a HUGE _BLOAR_WALL_DEADLINE must CLAMP it to its
#     own now+WALL_CLOCK_MAX, so the watchdog still bounds the run at wall+cleanup, not the caller's
#     deadline (the clamp min(inherited, now+WALL) preserves the genuine
#     child's earlier outer deadline but bounds any forged value). (Consume it as-is and the run outlives.)
#  C) forged fd, NO marker: a direct isolated caller passing _BLOAR_HS_FD=9 on its OWN write-only
#     file (`9>file`) must NEVER get its identity record written there -- the mandatory nonce read
#     fails on a write-only fd, so the child does not write. (fd type is not authenticity.)
#  E) forged fd WITH a spoofed _BLOAR_VERIFY_ISOLATED=1: the marker is spoofable
#     and must gate NOTHING. Even with it exported, the forged +60 deadline is CLAMPED (the run does
#     not outlive its ceiling) AND the identity record is NOT written to the caller's forged fd (the
#     nonce read still gates it). This is the exact marker-spoof regression
#     names. (Restore the marker-gated consume/scrub and both go RED: 24 bytes leak, run outlives.)
#  D) ordinary hooks: an ordinary invocation with a caller hold/fake must scrub them and follow
#     the ordinary path -- the outer must not pass caller control state to the child.
# Needs neither root nor systemd; SKIPs without setsid.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/verify-token-credentials.sh"
command -v setsid >/dev/null 2>&1 || { echo "SKIP: setsid not available"; exit 0; }
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'BAD  %s\n' "$*"; fails=$((fails + 1)); }
trap 'rm -rf "$_TESTTMP" 2>/dev/null' EXIT

# --- A) arithmetic injection via a duration knob ----------------------------------------------
markA="$_TESTTMP/injected"
rm -f "$markA"
WALL_CLOCK_MAX="BASH_SOURCE[\$(touch $markA)]" timeout -k 3 15 bash "$SCRIPT" >"$_TESTTMP/a.out" 2>&1
rcA=$?
[ ! -e "$markA" ] && ok "arith-injection: a BASH_SOURCE-subscript WALL_CLOCK_MAX did NOT execute (rejected before any arithmetic)" || bad "arith-injection: the subscript command-sub EXECUTED (validation ran too late)"
[ "$rcA" = 2 ] && ok "arith-injection: the entry aborted (status 2) on the malformed knob" || bad "arith-injection: status $rcA (expected a status-2 abort)"
# ARCHIVE_MPB is a COUNT knob: it too must be canonicalized EARLY, before its range
# check evaluates it as arithmetic (that check is root-gated, so the tell that the early
# canonicalization ran is that a malformed ARCHIVE_MPB aborts with the ARCHIVE_MPB message BEFORE
# the root check -- not "must run as root"). The subscript never executes (a regex match, not
# arithmetic). Reverting the early check lets the run reach the root check instead.
markMPB="$_TESTTMP/injected_mpb"
rm -f "$markMPB"
ARCHIVE_MPB="BASH_SOURCE[\$(touch $markMPB)]" timeout -k 3 15 bash "$SCRIPT" >"$_TESTTMP/mpb.out" 2>&1
rcMPB=$?
[ ! -e "$markMPB" ] && ok "arith-injection: a BASH_SOURCE-subscript ARCHIVE_MPB did NOT execute (regex match, no arithmetic)" || bad "arith-injection: the ARCHIVE_MPB subscript command-sub EXECUTED"
if [ "$rcMPB" = 2 ] && grep -q 'ARCHIVE_MPB' "$_TESTTMP/mpb.out"; then
	ok "arith-injection: a malformed ARCHIVE_MPB is canonicalized EARLY (aborts with the ARCHIVE_MPB message, before the root check)"
else
	bad "arith-injection: ARCHIVE_MPB not caught early (status '$rcMPB', msg='$(tr '\n' ' ' <"$_TESTTMP/mpb.out" | head -c 80)')"
fi

# --- B) inherited wall deadline is clamped ----------------------------------------------------
WCMB=3 CUMB=2
mockB="$(mktemp -d)"
printf '#!/bin/bash\nsleep 300\n' >"$mockB/id" # interruptible post-arm hang -> the watchdog must fire
chmod +x "$mockB/id"
huge=$((EPOCHSECONDS + 99999)) # a caller trying to extend the bound far into the future
startB=$(date +%s)
# the run is timeout-wrapped ONLY so that a clamp regression (which would honour the huge deadline
# and hang on the 300s mock id) fails cleanly instead of stalling this test; the fix terminates
# well within it, via its own watchdog, at wall+cleanup.
PATH="$mockB:$PATH" _BLOAR_WALL_DEADLINE=$huge WALL_CLOCK_MAX=$WCMB CLEANUP_MAX=$CUMB CURL_MAX=5 \
	timeout -k 3 "$((WCMB + CUMB + 12))" setsid bash "$SCRIPT" >/dev/null 2>&1
rcB=$?
elapsedB=$(($(date +%s) - startB))
if [ "$elapsedB" -le "$((WCMB + CUMB + 6))" ] && [ "$elapsedB" -ge "$((WCMB - 1))" ]; then
	ok "inherited-deadline: an isolated entry CLAMPED a huge inherited deadline to its own wall+cleanup (~${elapsedB}s)"
else
	bad "inherited-deadline: elapsed ${elapsedB}s -- the caller's huge deadline was not clamped to ~$((WCMB + CUMB))s"
fi
pkill -KILL -f "$mockB/id" 2>/dev/null || true
rm -rf "$mockB"

# --- C) a caller-FORGED write-only fd (9>file) never elicits the identity write.
# A DIRECT isolated caller opens fd 9 on its OWN file and passes _BLOAR_HS_FD=9 (with a hostile hold
# and a far-future deadline). fd type/writability is NOT authenticity -- the child must first READ
# the waiter's nonce back from the transport inode, and a write-only fd 9 cannot be read, so the
# child NEVER writes its identity to the caller's file and NEVER stays pre-arm; it runs the ordinary
# path and exits status 2 at the (non-root) root check. Restoring the forgeable-fd case the reviewer asked for.
mockC="$(mktemp -d)"
printf '#!/bin/bash\nexit 0\n' >"$mockC/id"
chmod +x "$mockC/id"
callerfile="$_TESTTMP/callerC"
: >"$callerfile"
holdfifo="$_TESTTMP/holdC"
mkfifo "$holdfifo"
future=$((EPOCHSECONDS + 60)) # a caller trying to extend the bound ~60s ahead
startC=$(date +%s)
PATH="$mockC:$PATH" _BLOAR_HS_FD=9 _BLOAR_HS_HOLD="$holdfifo" _BLOAR_WALL_DEADLINE=$future \
	WALL_CLOCK_MAX=1 CLEANUP_MAX=1 CURL_MAX=5 \
	timeout -k 3 15 setsid bash "$SCRIPT" 9>"$callerfile" >/dev/null 2>&1
rcC=$?
elapsedC=$(($(date +%s) - startC))
leaked=$(wc -c <"$callerfile" 2>/dev/null)
[ "${leaked:-1}" -eq 0 ] && ok "forged-fd: NO identity record written to the caller's file (the forged 9>file fd was scrubbed)" || bad "forged-fd: ${leaked} bytes leaked to the caller's file -- the forged fd was honoured"
[ "$rcC" = 2 ] && ok "forged-fd: followed the ordinary path (status 2), not a pre-arm hang to the external timeout" || bad "forged-fd: status '$rcC' (expected 2; 124/137 = stayed pre-arm to the timeout)"
[ "$elapsedC" -lt 6 ] && ok "forged-fd: terminated promptly (~${elapsedC}s), never stayed pre-arm" || bad "forged-fd: took ~${elapsedC}s -- stayed pre-arm"
rm -rf "$mockC"

# --- E) the exact marker-spoof of the regression case: a direct isolated caller
# EXPORTS the spoofable _BLOAR_VERIFY_ISOLATED=1 alongside a forged write-only fd 9 (`9>file`), a
# +60 deadline, and a POST-arm hang (a TERM/INT-ignoring id). The marker must gate NOTHING: the +60
# deadline is CLAMPED to now+wall so the watchdog ends the run at ~wall+cleanup (NOT +60), AND the
# identity record is NOT written to the caller's fd (the nonce read fails on the write-only fd).
# This is the original exact probe that previously false-passed. Reverting either the clamp
# or the nonce gate goes RED: the run outlives its ceiling to the external timeout, or 24 bytes leak.
mockE="$(mktemp -d)"
printf '#!/bin/bash\ntrap "" TERM INT\nwhile :; do sleep 1; done\n' >"$mockE/id" # post-arm hang: only the watchdog ends it
chmod +x "$mockE/id"
callerE="$_TESTTMP/callerE"
: >"$callerE"
futureE=$((EPOCHSECONDS + 60))
startE=$(date +%s)
PATH="$mockE:$PATH" _BLOAR_VERIFY_ISOLATED=1 _BLOAR_HS_FD=9 _BLOAR_WALL_DEADLINE=$futureE \
	WALL_CLOCK_MAX=1 CLEANUP_MAX=1 CURL_MAX=5 \
	timeout -k 3 15 setsid bash "$SCRIPT" 9>"$callerE" >/dev/null 2>&1
rcE=$?
elapsedE=$(($(date +%s) - startE))
leakedE=$(wc -c <"$callerE" 2>/dev/null)
[ "${leakedE:-1}" -eq 0 ] && ok "marker-spoof: NO identity record written despite _BLOAR_VERIFY_ISOLATED=1 (the nonce read still gates the forged fd)" || bad "marker-spoof: ${leakedE} bytes leaked to the caller's file -- the spoofed marker bypassed the nonce gate"
[ "$elapsedE" -lt 6 ] && ok "marker-spoof: the forged +60 deadline was CLAMPED -- the run ended at ~${elapsedE}s, not its ceiling+ (spoofed marker did not extend the bound)" || bad "marker-spoof: took ~${elapsedE}s -- the spoofed marker let the forged deadline extend the bound"
pkill -KILL -f "$mockE/id" 2>/dev/null || true
rm -rf "$mockE"

# --- D) an ORDINARY (non-isolated) invocation with a caller-supplied hold must SCRUB it at
# the outer boundary and follow the ordinary path -- the outer must not pass caller control state to
# the child. With WALL=5/CLEANUP=2 and a writer-less hold fifo the run must finish promptly (status
# 2), not stall ~6s toward the ceiling.
mockD="$(mktemp -d)"
printf '#!/bin/bash\nexit 0\n' >"$mockD/id"
chmod +x "$mockD/id"
holdD="$_TESTTMP/holdD"
mkfifo "$holdD"
startD=$(date +%s)
PATH="$mockD:$PATH" _BLOAR_HS_HOLD="$holdD" _BLOAR_HS_FAKE="9 9 9" WALL_CLOCK_MAX=5 CLEANUP_MAX=2 CURL_MAX=5 \
	timeout -k 3 20 bash "$SCRIPT" >/dev/null 2>&1
rcD=$?
elapsedD=$(($(date +%s) - startD))
if [ "$rcD" = 2 ] && [ "$elapsedD" -lt 5 ]; then
	ok "ordinary-hook: an ordinary invocation SCRUBBED the caller hold/fake and followed the ordinary path (status 2, ~${elapsedD}s)"
else
	bad "ordinary-hook: status '$rcD' after ~${elapsedD}s -- the outer passed caller control state to the child"
fi
rm -rf "$mockD"

if [ "$fails" -eq 0 ]; then
	echo "CALLER-TRUST REGRESSION: PASS"
	exit 0
fi
echo "CALLER-TRUST REGRESSION: $fails FAILED"
exit 1
