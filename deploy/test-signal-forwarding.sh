#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: a terminal interrupt delivered to
# the outer waiter must terminate the DETACHED run. The verifier re-execs under setsid as
# an ASYNC child, so it inherits SIGINT set to IGNORE (bash's job-control default) and,
# as a non-interactive shell, can neither trap nor un-ignore it -- a raw forwarded INT
# would vanish. The waiter therefore TRANSLATES INT to TERM (the verifier's handled
# termination path). This runs the REAL entry with an INTERRUPTIBLE verifier (a plain
# `sleep` at the root check), delivers first TERM then INT to the OUTER WAITER, and
# asserts BOTH terminate the detached run, leave no child in its process group, and
# return a defined status. Reverting the translation to forwarding raw INT leaves the run
# alive on the INT case. Needs neither root nor systemd; SKIPs without setsid.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/verify-token-credentials.sh"
command -v setsid >/dev/null 2>&1 || { echo "SKIP: setsid not available"; exit 0; }

# session_child WAITER -> the child of WAITER that is its OWN session leader (sid==pid): the
# isolated verifier, NEVER the coproc pacer (a bash subshell that shares the waiter's session,
# and is spawned first so a plain `pgrep -P|head -1` would pick it). Empty if none has isolated
# yet.
session_child() {
	local w=$1 p line
	for p in $(pgrep -P "$w" 2>/dev/null); do
		read -r line <"/proc/$p/stat" 2>/dev/null || continue
		line="${line##*)}"
		# shellcheck disable=SC2086
		set -- $line # state(1) ppid(2) pgrp(3) session(4)
		[ "${4:-}" = "$p" ] && { printf '%s' "$p"; return 0; }
	done
	return 1
}

# Enable job control so the launched verifier starts with SIGINT at its DEFAULT
# disposition, exactly as a foreground/terminal invocation would. Without this, a
# background child of a non-interactive shell inherits SIGINT set to IGNORE (and a
# non-interactive shell cannot un-ignore it) -- so the OUTER WAITER itself could not trap
# INT, which is a property of the harness, not of the code under test.
set -m

# Run-scoped scratch: the verifier now creates its handshake transport as a bloar-hs.XXXXXX DIR
# under TMPDIR. Point TMPDIR at our OWN tree so MOCK and every bloar-hs.* dir are removed with it.
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
MOCK="$(mktemp -d)"
# id blocks INTERRUPTIBLY at the root check (a plain sleep, NOT trapping) -- so a forwarded
# TERM reaches the verifier and fires its handled termination trap.
printf '#!/bin/bash\nsleep 300\n' >"$MOCK/id"
chmod +x "$MOCK/id"
trap 'rm -rf "$_TESTTMP"' EXIT

fails=0

# poll_group_empty PGID -> echo the group's remaining LIVE members after polling up to ~40s
# for it to drain. SIGKILL delivery/reaping is asynchronous, so the contract is "no
# descendant REMAINS", not "reaped within 0ms": under whole-suite load a swept process can
# sit as a ZOMBIE (state Z: dead, awaiting reap by init) for a while. A zombie is not a
# survivor, so it does not count; a real orphan is a LIVE process (running sleep) that never
# dies without the sweep, so it persists and the check stays load-bearing.
poll_group_empty() {
	local pg=$1 pid st live i line
	for i in $(seq 1 800); do
		live=""
		for pid in $(pgrep -g "$pg" 2>/dev/null); do
			read -r line < "/proc/$pid/stat" 2>/dev/null || continue # gone -> not a survivor
			line="${line##*) }" # drop pid+comm; state is the first remaining field
			st="${line%% *}"
			[ "$st" = Z ] || live="$live${live:+ }$pid"
		done
		[ -z "$live" ] && { printf ''; return 0; }
		sleep 0.05
	done
	printf '%s' "$live"
}

# run_case SIGNAME: deliver SIGNAME to the outer waiter; the detached run must terminate,
# leave no survivor in its group, and yield a defined status.
run_case() {
	local sig=$1 owpid vsid i alive=1 rc=999 survivors
	PATH="$MOCK:$PATH" WALL_CLOCK_MAX=300 CLEANUP_MAX=5 CURL_MAX=5 \
		bash "$SCRIPT" >/dev/null 2>&1 &
	owpid=$!
	vsid=""
	for i in $(seq 1 50); do
		vsid="$(session_child "$owpid")"
		[ -n "$vsid" ] && break
		sleep 0.1
	done
	if [ -z "$vsid" ]; then
		echo "FAIL ($sig): the detached verifier session never appeared"
		kill -KILL "$owpid" 2>/dev/null
		fails=$((fails + 1))
		return
	fi

	kill -"$sig" "$owpid" # as a terminal would deliver it to the foreground waiter
	for i in $(seq 1 20); do
		kill -0 "$owpid" 2>/dev/null || { alive=0; break; }
		sleep 0.5
	done
	if [ "$alive" -eq 0 ]; then
		wait "$owpid" 2>/dev/null
		rc=$?
	fi
	survivors="$(poll_group_empty "$vsid")" # poll: reaping is async, "no survivor REMAINS"
	kill -KILL "-$vsid" 2>/dev/null
	kill -KILL "$owpid" 2>/dev/null

	# Assert the EXACT documented status 2 for BOTH TERM and INT: the child
	# dies via the forwarded TERM -> its watchdog trap -> die -> exit 2, which the waiter
	# propagates. A signal death (143) or any other value is a failure, not merely rc==999.
	if [ "$alive" -eq 0 ] && [ -z "$survivors" ] && [ "$rc" = 2 ]; then
		echo "ok   ($sig): outer waiter delivered it; detached run terminated with the exact status 2, no survivor in its group"
	else
		echo "FAIL ($sig): alive=$alive survivors='$survivors' rc=$rc (expected exact rc==2; a forwarded $sig did not cleanly terminate the detached run)"
		fails=$((fails + 1))
	fi
}

run_case TERM
run_case INT

if [ "$fails" -eq 0 ]; then
	echo "SIGNAL-FORWARDING REGRESSION: PASS"
	exit 0
fi
echo "SIGNAL-FORWARDING REGRESSION: $fails FAILED"
exit 1
