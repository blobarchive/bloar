#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the
# cleanup path must NOT cancel the terminal deadline. The OLD code SIGKILLed the watchdog
# on cleanup entry, disarming the pending group-KILL before teardown ran -- so a wedged
# `systemctl stop` could stretch teardown to N*(OP_MAX+OP_KILL_GRACE), far past the
# advertised wall+cleanup ceiling. The fix arms a fresh ABSOLUTE cleanup killer that
# group-KILLs after CLEANUP_MAX unless teardown finishes first.
#
# This drives the NORMAL executable entry (guard -> setsid re-exec -> outer waiter ->
# main), mocking JUST enough (id, mktemp, install, systemd-run, systemctl) to reach
# start_archive so exactly ONE transient unit is registered, then forcing a normal
# die -> cleanup whose `systemctl stop` HANGS while IGNORING TERM. Only the cleanup
# killer's SIGKILL can end that manager child (rider 2), and the whole run must die
# within CLEANUP_MAX + margin (rider 1: a hard elapsed upper bound on the real entry).
# Needs neither root nor systemd; SKIPs without setsid.

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

WCM=30 # WALL_CLOCK_MAX -- deliberately long; cleanup here is triggered by a fast die,
CUM=2  # CLEANUP_MAX     -- so the run must die by CLEANUP_MAX + margin regardless of WCM.
MARGIN=8
DEADLINE=$((CUM + MARGIN))

# poll_group_empty PGID -> echo the group's remaining LIVE members after polling up to ~40s
# for it to drain. SIGKILL delivery/reaping is asynchronous, so a swept process can sit as a
# ZOMBIE (state Z: dead, awaiting reap) for a while under load -- a zombie is not a survivor.
# A real survivor (the wedged manager, a running sleep) never dies without the escalation, so
# it persists and the check stays load-bearing.
poll_group_empty() {
	local pg=$1 pid st live i line
	for i in $(seq 1 800); do
		live=""
		for pid in $(pgrep -g "$pg" 2>/dev/null); do
			read -r line < "/proc/$pid/stat" 2>/dev/null || continue
			line="${line##*) }"
			st="${line%% *}"
			[ "$st" = Z ] || live="$live${live:+ }$pid"
		done
		[ -z "$live" ] && { printf ''; return 0; }
		sleep 0.05
	done
	printf '%s' "$live"
}

# Run-scoped scratch: the verifier creates its handshake transport as a bloar-hs.XXXXXX DIR
# under TMPDIR. Point TMPDIR at our OWN tree so every bloar-hs.* dir is removed with it.
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
MOCK="$(mktemp -d)"
TMPBASE="$(mktemp -d)"
BINTRUE="$(type -P true)" # the on-disk /usr/bin/true, not the shell builtin
[ -x "$BINTRUE" ] || { echo "SKIP: no on-disk 'true' binary for the stub daemon"; rm -rf "$_TESTTMP"; exit 0; }

# id: fake root for the root check, and distinct uids for the daemon/unpriv users so
# preflight's distinctness checks pass.
cat >"$MOCK/id" <<'EOF'
#!/bin/bash
if [ "${1:-}" = "-u" ]; then
	case "${2:-}" in "") echo 0 ;; bloar) echo 4001 ;; *) echo 4002 ;; esac
	exit 0
fi
exit 0
EOF
# mktemp -d <template>: the real /run is root-only, so redirect SCRATCH under a writable
# base baked in here.
cat >"$MOCK/mktemp" <<EOF
#!/bin/bash
d="$TMPBASE/scratch.\$\$-\$RANDOM"
mkdir -p "\$d" && printf '%s\n' "\$d"
EOF
# install -m .. -o root -g root SRC DEST: just create DEST (owner root would need root);
# the verifier's own hygiene check will note it is not root-owned, which is fine -- we
# only need the run to reach start_archive and then die.
cat >"$MOCK/install" <<'EOF'
#!/bin/bash
for dest; do :; done # dest = last arg = DEST
: >"$dest" && chmod 0600 "$dest" 2>/dev/null
exit 0
EOF
# systemd-run: the archive fails to start, so start_archive registers its unit (via
# alloc_unit, before systemd-run) and then returns 1.
cat >"$MOCK/systemd-run" <<'EOF'
#!/bin/bash
exit 1
EOF
# systemctl: `stop`/`reset-failed` (the cleanup calls) HANG while ignoring TERM -- a
# TERM-ignoring manager child that only SIGKILL can end. Every other `show` answers so
# preflight and the journal helpers pass.
cat >"$MOCK/systemctl" <<'EOF'
#!/bin/bash
case " $* " in
*" stop "*|*" reset-failed "*)
	trap '' TERM INT
	while :; do sleep 1; done ;;
esac
echo CURRENTINV
exit 0
EOF
chmod +x "$MOCK"/id "$MOCK"/mktemp "$MOCK"/install "$MOCK"/systemd-run "$MOCK"/systemctl

# Launch the REAL executable; it re-execs under setsid, so the verifier runs detached in
# its own session. Discover that session so we can hard-clean it on failure.
PATH="$MOCK:$PATH" \
	BLOARD_BIN="$BINTRUE" INDEX_BIN="$BINTRUE" \
	WALL_CLOCK_MAX=$WCM CLEANUP_MAX=$CUM CURL_MAX=5 \
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
	rm -rf "$_TESTTMP" # our OWN per-run tree (holds MOCK, TMPBASE and the bloar-hs.* dir)
}
trap cleanup_harness EXIT

start="$(date +%s)"
alive=1
for _ in $(seq 1 $((DEADLINE * 2 + 4))); do
	kill -0 "$owpid" 2>/dev/null || { alive=0; break; }
	sleep 0.5
done
elapsed=$(($(date +%s) - start))

# Rider 2: after the run ends, confirm no TERM-ignoring manager child survived in the
# verifier's process group -- the cleanup killer's SIGKILL must have reaped it. Poll (and
# ignore zombies) so a slow async reap is not mistaken for a survivor.
survivors="?"
if [ "$alive" -eq 0 ] && [ -n "$vsid" ]; then
	survivors="$(poll_group_empty "$vsid")"
fi

if [ "$alive" -eq 0 ] && [ "$elapsed" -le "$DEADLINE" ] && [ -z "$survivors" ]; then
	echo "CLEANUP-BOUND REGRESSION: PASS (real entry died in ~${elapsed}s within ${DEADLINE}s; TERM-ignoring manager reaped by the cleanup escalation)"
	exit 0
fi
echo "CLEANUP-BOUND REGRESSION: FAIL (alive=$alive elapsed=${elapsed}s deadline=${DEADLINE}s survivors='$survivors': cleanup did not hard-bound a wedged TERM-ignoring manager)"
exit 1
