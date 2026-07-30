#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the verifier's watchdog must
# HARD-terminate a run even when a foreground child has deferred the main shell's
# TERM trap -- bash does exactly that, so a watchdog that only `kill -TERM $$`s the
# parent (the old design) can be blocked behind a wedged child indefinitely. The
# fix escalates SIGTERM->SIGKILL on the whole process GROUP independently.
#
# The victim sources the verifier, arms the watchdog with short bounds, and then
# blocks in a child that IGNORES TERM and loops, so ONLY the watchdog's independent
# group-SIGKILL can end the run. It must die within WALL_CLOCK_MAX + CLEANUP_MAX +
# margin. The victim runs under setsid (its own session/group) so the group-kill
# cannot reach this harness. Needs neither root nor systemd.
#
# the regression case: the oracle must actually pin down what a
# GROUP-SIGKILL means, or two watchdog mutations still false-pass. So we now (a) capture
# the victim's EXIT STATUS via `setsid -w` and REQUIRE 137 (SIGKILL) -- a KILL->SEGV
# mutation gives 139 and fails; and (b) require ZERO LIVE non-zombie descendants in the
# victim's process GROUP before the EXIT trap's emergency cleanup -- a group-KILL->leader-
# only-KILL mutation leaves the TERM-ignoring child alive and fails. Both are asserted
# from the victim's real /proc, before any harness force-clean that would mask them.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

command -v setsid >/dev/null 2>&1 || { echo "SKIP: setsid not available"; exit 0; }

WCM=2      # WALL_CLOCK_MAX
CUM=2      # CLEANUP_MAX
MARGIN=6
DEADLINE=$((WCM + CUM + MARGIN))

PIDFILE="$(mktemp)"
VICTIM="$(mktemp)"
MARK="$(mktemp -u)" # dropped when the victim REACHES the trap-deferred child (post-arm path)
cleanup_harness() { kill -KILL "-$(cat "$PIDFILE" 2>/dev/null)" 2>/dev/null; rm -f "$PIDFILE" "$VICTIM" "$MARK"; }
trap cleanup_harness EXIT

# group_survivors PGID -> count of LIVE (non-zombie) processes in process-group PGID, EXCLUDING the
# group leader itself (its own death is the status check). A correct terminal group-KILL leaves
# none; a leader-only-KILL mutation leaves the TERM-ignoring child (in the group) alive. /proc +
# builtins only.
group_survivors() {
	local pg=$1 n=0 st line pid state pgid
	for st in /proc/[0-9]*/stat; do
		read -r line <"$st" 2>/dev/null || continue
		pid="${line%% *}"
		line="${line##*) }" # drop "pid (comm) "; remaining: state(1) ppid(2) pgrp(3) ...
		# shellcheck disable=SC2086
		set -- $line
		state="${1:-}" pgid="${3:-}"
		[ "$pgid" = "$pg" ] || continue
		[ "$pid" = "$pg" ] && continue # the leader itself
		[ "$state" = Z ] && continue   # a zombie is not a live survivor
		n=$((n + 1))
	done
	printf '%s' "$n"
}

# The victim sources the verifier and arms the watchdog. arm_watchdog derives its remaining budget
# from the ABSOLUTE _BLOAR_WALL_DEADLINE, so the victim MUST set it -- else `set -u`
# aborts the victim at once and this test would false-pass at ~0s. It drops a
# marker the instant it enters the trap-deferred child, proving the watchdog's group-KILL -- not an
# early crash -- is what ends the run.
cat >"$VICTIM" <<EOF
#!/bin/bash
set -uo pipefail
echo \$\$ >"$PIDFILE"                       # \$\$ = this session/group leader (under setsid)
export WALL_CLOCK_MAX=$WCM CLEANUP_MAX=$CUM CURL_MAX=5
export _BLOAR_WALL_DEADLINE=\$((EPOCHSECONDS + $WCM)) # the absolute wall deadline arm_watchdog reads
# shellcheck source=/dev/null
source "$HERE/verify-token-credentials.sh" # guarded main does not run
trap cleanup EXIT
SCRATCH="\$(mktemp -d)"
arm_watchdog
: >"$MARK"                                   # reached the post-arm path: only the watchdog ends us now
# A foreground child that ignores TERM and loops: the watchdog's group-TERM cannot
# end it (it defers the main shell's trap too), so only the group-KILL terminates.
bash -c 'trap "" TERM INT; while :; do sleep 1; done'
EOF
chmod +x "$VICTIM"

# `setsid -w`: run the victim in its own session AND WAIT, so the wrapper's exit status is the
# victim's -- 137 for a group SIGKILL, 139 for a SIGSEGV (the KILL->SEGV mutation).
setsid -w bash "$VICTIM" &
vwrap=$!
for _ in $(seq 1 50); do [ -s "$PIDFILE" ] && break; sleep 0.1; done
vpid="$(cat "$PIDFILE" 2>/dev/null)"
[ -n "$vpid" ] || { echo "HARD-BOUND REGRESSION: FAIL (victim never started)"; exit 1; }

start="$(date +%s)"
alive=1
for _ in $(seq 1 $((DEADLINE * 2))); do
	kill -0 "$vpid" 2>/dev/null || { alive=0; break; }
	sleep 0.5
done
elapsed=$(($(date +%s) - start))
reached=no
[ -e "$MARK" ] && reached=yes

# The victim's process GROUP must have ZERO live non-zombie descendants (the TERM-ignoring child).
# Poll briefly: a correct group-KILL reaps it asynchronously, so it converges to 0; a leader-only
# KILL never touches it, so it stays >= 1. Asserted from real /proc BEFORE the EXIT trap force-cleans.
survivors=1
for _ in $(seq 1 60); do # ~3s
	survivors="$(group_survivors "$vpid")"
	[ "$survivors" -eq 0 ] && break
	sleep 0.05
done

# The victim's EXIT STATUS: 137 == SIGKILL. Only reap once it is actually gone (else `wait` blocks).
status=""
[ "$alive" -eq 0 ] && { wait "$vwrap" 2>/dev/null; status=$?; }

# PASS iff: the victim self-terminated within the deadline; the watchdog actually had to fire
#; the post-arm
# trap-deferred child was reached (marker); the terminal signal was SIGKILL (status 137, not a SEGV
# mutation's 139); AND no live group descendant remained (not a leader-only-KILL mutation's orphan).
if [ "$alive" -eq 0 ] && [ "$elapsed" -le "$DEADLINE" ] && [ "$elapsed" -ge "$WCM" ] \
	&& [ "$reached" = yes ] && [ "$status" = 137 ] && [ "$survivors" -eq 0 ]; then
	echo "HARD-BOUND REGRESSION: PASS (watchdog group-SIGKILLed a trap-deferred run in ~${elapsed}s, status 137, no group descendant survived, post-arm path reached, within ${DEADLINE}s)"
	exit 0
fi
echo "HARD-BOUND REGRESSION: FAIL (alive=$alive elapsed=${elapsed}s deadline=${DEADLINE}s reached_child=$reached status=${status:-none} survivors=$survivors: the watchdog did not group-SIGKILL a trap-deferred run cleanly, or the victim exited early)"
exit 1
