#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: every external is
# bounded, so no wedged binary can hang the run past wall+cleanup. `mkfifo` is GONE from the path
# entirely (the FIFO handshake was replaced by an fd protocol) -- a hostile mkfifo mock here proves
# it stays UNused. `rm` IS used in the waiter's cleanup, `timeout`-bounded by the REMAINING budget
# a hostile TERM/INT-ignoring rm mock proves BOTH that it cannot outlive
# the deadline AND that the cleanup still REMOVES the
# transport -- the fix resolves rm to an ABSOLUTE path a PATH override cannot shadow, so we assert
# the bloar-hs.* dir/file are ABSENT after the run, in the body, BEFORE the EXIT trap force-cleans
# the private TMPDIR with the harness's own real rm (which would mask the residue). A hanging `id`
# at the (post-arm) root check is the hang the run rides out via the
# watchdog; it drops a marker when reached, proving the post-arm path was exercised. We time from
# the ENTRY invocation, pin the deadline to the ACTUAL operational ceiling (EXACTLY wall+cleanup,
# no slack), require a lower time bound (the watchdog really had to fire, so the
# run did not die early for a fluke), and assert ZERO survivor via a bounded poll rather than
# force-cleaning. (The mktemp pre-spawn external is bounded too; test-outer-boundary.sh drives its
# hostile case; the exact-status oracle lives in test-hostile-proc.sh.) Needs neither root nor
# systemd; SKIPs without setsid.

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

WCM=2 # WALL_CLOCK_MAX
CUM=2 # CLEANUP_MAX
CEILING=$((WCM + CUM)) # the ACTUAL operational ceiling: EXACTLY wall+cleanup, no runtime slack
MARGIN=1               # tight tolerance: catches a cleanup overshoot past wall+cleanup
DEADLINE=$((CEILING + MARGIN))

# Run-scoped scratch: the verifier creates its handshake transport as a bloar-hs.XXXXXX DIR under
# TMPDIR. Point TMPDIR at our OWN tree so it is removed run-scoped, never via a global glob.
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
MOCK="$(mktemp -d)"
MARK="$MOCK/reached_root_check"
# mkfifo + rm: removed pre-arm external commands. Both hang while IGNORING TERM/INT -- if
# the entry exec's EITHER before arming (the old FIFO handshake did), the run wedges forever.
for ext in mkfifo rm; do
	cat >"$MOCK/$ext" <<'EOF'
#!/bin/bash
trap '' TERM INT
while :; do sleep 1; done
EOF
done
# id: a POST-arm hang (the root check). It first drops a marker, proving the child reached the
# post-arm path (so we know mkfifo/rm were skipped, not that the run died earlier for a fluke).
cat >"$MOCK/id" <<EOF
#!/bin/bash
: >"$MARK"
trap '' TERM INT
while :; do sleep 1; done
EOF
chmod +x "$MOCK/mkfifo" "$MOCK/rm" "$MOCK/id"

start="$(date +%s)" # time from the ENTRY invocation
PATH="$MOCK:$PATH" WALL_CLOCK_MAX=$WCM CLEANUP_MAX=$CUM CURL_MAX=5 \
	bash "$SCRIPT" >/dev/null 2>&1 &
owpid=$!
vsid=""
for _ in $(seq 1 50); do
	vsid="$(session_child "$owpid")"
	[ -n "$vsid" ] && break
	sleep 0.1
done

cleanup_harness() {
	[ -n "$vsid" ] && kill -KILL "-$vsid" 2>/dev/null
	kill -KILL "$owpid" 2>/dev/null
	rm -rf "$_TESTTMP" 2>/dev/null # our OWN per-run tree (holds MOCK and the bloar-hs.* dir)
}
trap cleanup_harness EXIT

alive=1
for _ in $(seq 1 $((DEADLINE * 2))); do
	kill -0 "$owpid" 2>/dev/null || { alive=0; break; }
	sleep 0.5
done
elapsed=$(($(date +%s) - start))

# The verifier group must have NO survivor -- assert it by bounded poll, never force-clean.
survivors=""
live=""
if [ -n "$vsid" ]; then
	for _ in $(seq 1 200); do # ~10s: SIGKILL delivery/reap is async under suite load
		live=""
		for p in $(pgrep -g "$vsid" 2>/dev/null); do
			read -r line <"/proc/$p/stat" 2>/dev/null || continue # gone -> not a survivor
			line="${line##*) }"
			[ "${line%% *}" = Z ] || live="$live${live:+ }$p" # a zombie is not a survivor
		done
		[ -z "$live" ] && break
		sleep 0.05
	done
	survivors="$live"
fi

reached=no
[ -e "$MARK" ] && reached=yes

# The verifier's OWN transport dir+file MUST be gone. With the
# hostile TERM/INT-ignoring `rm` on PATH, a bare `rm` in the cleanup is killed by the timeout before
# it unlinks anything, leaving the bloar-hs.XXXXXX/ dir (0700) and its hs file (0600). Assert their
# absence HERE -- BEFORE the EXIT trap force-cleans TMPDIR with the harness's own real rm, which
# would mask it. The fix resolves rm to an absolute path the hostile PATH entry cannot shadow.
residue="$(find "$_TESTTMP" -maxdepth 1 -name 'bloar-hs.*' 2>/dev/null | tr '\n' ' ')"

# PASS iff: the run self-terminated (alive=0) within the REAL ceiling+margin, measured from
# entry; the watchdog actually had to fire (elapsed >= WCM, so it was not a fluke-fast exit);
# the post-arm path was reached (marker); no survivor ever remained; AND the transport dir/file
# were removed despite the hostile PATH rm (no residue).
if [ "$alive" -eq 0 ] && [ "$elapsed" -le "$DEADLINE" ] && [ "$elapsed" -ge "$WCM" ] \
	&& [ "$reached" = yes ] && [ -z "$survivors" ] && [ -z "$residue" ]; then
	echo "BOOTSTRAP-BOUND REGRESSION: PASS (real entry self-terminated in ~${elapsed}s within the configured ceiling ${CEILING}s+${MARGIN}s; post-arm path reached; hostile mkfifo stayed unused and the hostile cleanup rm stayed bounded AND still removed the transport; no survivor; no residue)"
	exit 0
fi
echo "BOOTSTRAP-BOUND REGRESSION: FAIL (alive=$alive elapsed=${elapsed}s ceiling=${CEILING}s deadline=${DEADLINE}s reached_root=$reached survivors='$survivors' residue='$residue')"
exit 1
