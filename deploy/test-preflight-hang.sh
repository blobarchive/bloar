#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the
# verifier must ARM its terminal watchdog BEFORE any NSS/`id` lookup, exercised through
# the NORMAL executable entry -- guard -> setsid re-exec -> outer waiter -> main -> arm.
# The old order armed only AFTER the root check and preflight, so a wedged NSS lookup
# (LDAP/SSSD) before arming survived forever. This runs the REAL script with `id` mocked
# to HANG while ignoring TERM at the root check, and proves the whole lifecycle still
# self-terminates within WALL_CLOCK_MAX + CLEANUP_MAX. Because the mocked id ignores
# TERM, only the watchdog's group-SIGKILL can end it. The verifier runs in its own
# setsid session; the outer waiter/harness are NOT in the killed group. Needs neither
# root nor systemd; SKIPs without setsid.

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

WCM=2      # WALL_CLOCK_MAX
CUM=2      # CLEANUP_MAX
MARGIN=8
DEADLINE=$((WCM + CUM + MARGIN))

# Run-scoped scratch: the verifier creates its handshake transport as a bloar-hs.XXXXXX DIR under
# TMPDIR. Point TMPDIR at our OWN tree so MOCK and every bloar-hs.* dir are removed with it.
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
MOCK="$(mktemp -d)"
cat >"$MOCK/id" <<'EOF'
#!/bin/bash
# Simulate a wedged NSS lookup: `id -u` (the root check, the first pre-arm-era call)
# hangs, ignoring TERM, so ONLY the watchdog's SIGKILL can end it.
trap '' TERM INT
while :; do sleep 1; done
EOF
chmod +x "$MOCK/id"

# Launch the REAL executable; it re-execs under setsid, so the verifier runs detached in
# its own session. Discover that session (the outer waiter's child) so we can hard-clean
# it if the fix is absent and it hangs.
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
	rm -rf "$_TESTTMP" # our OWN per-run tree (holds MOCK and the bloar-hs.* dir)
}
trap cleanup_harness EXIT

start="$(date +%s)"
alive=1
for _ in $(seq 1 $((DEADLINE * 2))); do
	kill -0 "$owpid" 2>/dev/null || { alive=0; break; }
	sleep 0.5
done
elapsed=$(($(date +%s) - start))

if [ "$alive" -eq 0 ] && [ "$elapsed" -le "$DEADLINE" ]; then
	echo "PREFLIGHT-HANG REGRESSION: PASS (real entry self-terminated in ~${elapsed}s, within the ${DEADLINE}s deadline)"
	exit 0
fi
echo "PREFLIGHT-HANG REGRESSION: FAIL (alive=$alive elapsed=${elapsed}s deadline=${DEADLINE}s: a pre-arm id hang was not bounded by the watchdog)"
exit 1
