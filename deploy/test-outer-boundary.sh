#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the OUTER boundary of
# the real entry, driven with a mocked `setsid` -- the two things hostile mkfifo/rm mocks cannot
# prove.
#  A) HUNG, TERM-IGNORING setsid: the child-before-handshake path. The waiter must not
#     wedge -- it times out at the EXACT wall+cleanup deadline measured from invocation, refuses
#     to forward (no verified target), reaps the in-our-group pid, and exits the documented
#     status 2. The caller canary survives and ZERO verifier/pacer descendants remain.
#  B) SLOW setsid: proves the inner child arms its watchdog to the REMAINING budget, not
#     a fresh full WALL_CLOCK_MAX. With an interruptible post-arm hang the run ends GRACEFULLY --
#     the child's OWN watchdog TERM fires at wall-from-outer-entry, so it dies with status 2. A
#     fresh full wall would fire only at entry+delay+wall, so the waiter backstop blunt-KILLs the
#     group first: status 137. Reverting arm_watchdog to `sleep "$WALL_CLOCK_MAX"` yields 137.
#  C) HOSTILE mktemp: the transport-dir external is `timeout`-bounded under the deadline,
#     so a TERM/INT-ignoring mktemp aborts the run by wall+cleanup (status 2), canary alive, no
#     pacer leak. Dropping the timeout wrapper wedges the bootstrap.
#  D) REPEATED-run residue: 3 failing runs leave ZERO transport dirs -- the waiter's
#     bounded cleanup rm removes each. Dropping the cleanup rm leaves residue behind.
# Needs neither root nor systemd; SKIPs without setsid.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/verify-token-credentials.sh"
command -v setsid >/dev/null 2>&1 || { echo "SKIP: setsid not available"; exit 0; }
REAL_SETSID="$(command -v setsid)"
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
fails=0
ok() { printf 'ok   %s\n' "$*"; }
bad() { printf 'BAD  %s\n' "$*"; fails=$((fails + 1)); }
trap 'rm -rf "$_TESTTMP" 2>/dev/null' EXIT

# poll_all_gone PIDS -> "" once every pid is gone (or a zombie awaiting reap), else the survivors.
poll_all_gone() {
	local want="$*" p live i line
	for i in $(seq 1 200); do # ~10s: SIGKILL delivery/reap is async under suite load
		live=""
		for p in $want; do
			kill -0 "$p" 2>/dev/null || continue
			read -r line <"/proc/$p/stat" 2>/dev/null || continue
			line="${line##*) }"
			[ "${line%% *}" = Z ] || live="$live${live:+ }$p"
		done
		[ -z "$live" ] && {
			printf ''
			return 0
		}
		sleep 0.05
	done
	printf '%s' "$live"
}

# ---- A) hung, TERM-ignoring setsid: child-before-handshake path -------------------------------
WCM=3 CUM=2 CEIL=$((WCM + CUM)) TOL=6
MOCKA="$(mktemp -d)" SYNCA="$(mktemp -d)"
mkfifo "$SYNCA/hang"
cat >"$MOCKA/setsid" <<EOF
#!/bin/bash
trap '' TERM INT
read -r _ < "$SYNCA/hang"   # blocks on the open forever (no writer, no child) -> never isolates
EOF
chmod +x "$MOCKA/setsid"
cat >"$SYNCA/caller.sh" <<EOF
#!/bin/bash
sleep 300 & echo \$! > "$SYNCA/canary"
echo \$(date +%s) > "$SYNCA/start"
PATH="$MOCKA:\$PATH" WALL_CLOCK_MAX=$WCM CLEANUP_MAX=$CUM CURL_MAX=5 bash "$SCRIPT" >/dev/null 2>&1 &
w=\$!
echo \$w > "$SYNCA/waiter"
wait "\$w"
echo \$? > "$SYNCA/wstat"
echo \$(date +%s) > "$SYNCA/end"
EOF
chmod +x "$SYNCA/caller.sh"
setsid bash "$SYNCA/caller.sh" &
outerA=$!
for _ in $(seq 1 50); do [ -s "$SYNCA/waiter" ] && break; sleep 0.1; done
waiterA="$(cat "$SYNCA/waiter" 2>/dev/null)"
canaryA="$(cat "$SYNCA/canary" 2>/dev/null)"
sleep 0.5
kidsA="$(pgrep -P "$waiterA" 2>/dev/null | tr '\n' ' ')" # the verifier setsid pid + the pacer coproc
wstatA=""
for _ in $(seq 1 $(((CEIL + TOL) * 20))); do
	wstatA="$(cat "$SYNCA/wstat" 2>/dev/null)"
	[ -n "$wstatA" ] && break
	sleep 0.05
done
startA="$(cat "$SYNCA/start" 2>/dev/null)" endA="$(cat "$SYNCA/end" 2>/dev/null)"
elapsedA=$((${endA:-0} - ${startA:-0}))
[ "$wstatA" = 2 ] && ok "hung-setsid: the run self-terminated with the DOCUMENTED status 2" || bad "hung-setsid: status '${wstatA:-none}' (expected 2)"
if [ -n "$wstatA" ] && [ "$elapsedA" -ge "$WCM" ] && [ "$elapsedA" -le "$((CEIL + TOL))" ]; then
	ok "hung-setsid: terminated at the wall+cleanup deadline (~${elapsedA}s, ceiling ${CEIL}s) from invocation"
else
	bad "hung-setsid: elapsed ${elapsedA}s outside [${WCM}, $((CEIL + TOL))]s"
fi
kill -0 "$canaryA" 2>/dev/null && ok "hung-setsid: the caller canary SURVIVED" || bad "hung-setsid: the caller canary was killed"
survA="$(poll_all_gone $kidsA)"
[ -z "$survA" ] && ok "hung-setsid: zero verifier/pacer descendants remain (waiter children ${kidsA:-none} all reaped)" || bad "hung-setsid: survivors among waiter children: $survA"
kill -KILL $kidsA "$canaryA" 2>/dev/null
[ -n "$waiterA" ] && kill -KILL "$waiterA" 2>/dev/null
kill -KILL "$outerA" 2>/dev/null
rm -rf "$MOCKA" "$SYNCA"

# ---- C) hostile mktemp: the transport-dir external is bounded ----------------
# the original exact repro: WALL_CLOCK_MAX=2 CLEANUP_MAX=2 with a TERM/INT-ignoring mktemp was
# STILL RUNNING at 8 seconds and then died rc=137 (an external kill, not the run's own bound). The
# fix creates the secure dir with mktemp UNDER the active deadline (`timeout`-wrapped by the
# remaining budget), so a wedged mktemp is dead by wall+cleanup: the run aborts with the DOCUMENTED
# status 2 well before 8s, canary alive, no pacer descendant. (The transport FILE is created by a
# builtin O_EXCL `exec {fd}>` redirect -- eliminated with builtins, not a supervised external.)
WCM3=2 CUM3=2 CEIL3=$((WCM3 + CUM3)) TOL3=6 SURVIVE_LIMIT=8
MOCKC="$(mktemp -d)" SYNCC="$(mktemp -d)"
cat >"$MOCKC/mktemp" <<'EOF'
#!/bin/bash
trap '' TERM INT
while :; do sleep 1; done
EOF
chmod +x "$MOCKC/mktemp"
cat >"$SYNCC/caller.sh" <<EOF
#!/bin/bash
sleep 300 & echo \$! > "$SYNCC/canary"
echo \$(date +%s) > "$SYNCC/start"
PATH="$MOCKC:\$PATH" OP_KILL_GRACE=1 WALL_CLOCK_MAX=$WCM3 CLEANUP_MAX=$CUM3 CURL_MAX=5 bash "$SCRIPT" >/dev/null 2>&1 &
w=\$!
echo \$w > "$SYNCC/waiter"
wait "\$w"
echo \$? > "$SYNCC/wstat"
echo \$(date +%s) > "$SYNCC/end"
EOF
chmod +x "$SYNCC/caller.sh"
setsid bash "$SYNCC/caller.sh" &
outerC=$!
for _ in $(seq 1 50); do [ -s "$SYNCC/waiter" ] && break; sleep 0.1; done
waiterC="$(cat "$SYNCC/waiter" 2>/dev/null)"
canaryC="$(cat "$SYNCC/canary" 2>/dev/null)"
sleep 0.5
kidsC="$(pgrep -P "$waiterC" 2>/dev/null | tr '\n' ' ')" # the pacer coproc (the verifier never spawns)
wstatC=""
for _ in $(seq 1 $(((CEIL3 + TOL3) * 20))); do
	wstatC="$(cat "$SYNCC/wstat" 2>/dev/null)"
	[ -n "$wstatC" ] && break
	sleep 0.05
done
startC="$(cat "$SYNCC/start" 2>/dev/null)" endC="$(cat "$SYNCC/end" 2>/dev/null)"
elapsedC=$((${endC:-0} - ${startC:-0}))
[ "$wstatC" = 2 ] && ok "hostile-mktemp: the run aborted with the DOCUMENTED status 2 (not the broken rc=137)" || bad "hostile-mktemp: status '${wstatC:-none}' (expected 2, not 137)"
if [ -n "$wstatC" ] && [ "$elapsedC" -lt "$SURVIVE_LIMIT" ] && [ "$elapsedC" -le "$((CEIL3 + TOL3))" ]; then
	ok "hostile-mktemp: bounded by wall+cleanup (~${elapsedC}s) -- did NOT survive to ${SURVIVE_LIMIT}s"
else
	bad "hostile-mktemp: elapsed ${elapsedC}s -- survived to/past ${SURVIVE_LIMIT}s (mktemp was not bounded by wall+cleanup)"
fi
kill -0 "$canaryC" 2>/dev/null && ok "hostile-mktemp: the caller canary SURVIVED" || bad "hostile-mktemp: the caller canary was killed"
survC="$(poll_all_gone $kidsC)"
[ -z "$survC" ] && ok "hostile-mktemp: zero pacer descendants remain (${kidsC:-none} reaped)" || bad "hostile-mktemp: survivors among waiter children: $survC"
kill -KILL $kidsC "$canaryC" 2>/dev/null
[ -n "$waiterC" ] && kill -KILL "$waiterC" 2>/dev/null
kill -KILL "$outerC" 2>/dev/null
pkill -KILL -f "$MOCKC/mktemp" 2>/dev/null || true
rm -rf "$MOCKC" "$SYNCC"

# ---- B) slow setsid: the inner child arms the REMAINING budget --------------------------------
WCM2=8 CUM2=2 D=4 CEIL2=$((WCM2 + CUM2)) TOL2=6
MOCKB="$(mktemp -d)" SYNCB="$(mktemp -d)"
cat >"$MOCKB/setsid" <<EOF
#!/bin/bash
sleep $D
exec "$REAL_SETSID" "\$@"
EOF
printf '#!/bin/bash\nsleep 300\n' >"$MOCKB/id" # interruptible post-arm hang: the child's OWN watchdog TERM must end it
chmod +x "$MOCKB/setsid" "$MOCKB/id"
cat >"$SYNCB/caller.sh" <<EOF
#!/bin/bash
echo \$(date +%s) > "$SYNCB/start"
PATH="$MOCKB:\$PATH" WALL_CLOCK_MAX=$WCM2 CLEANUP_MAX=$CUM2 CURL_MAX=5 bash "$SCRIPT" >/dev/null 2>&1 &
w=\$!
echo \$w > "$SYNCB/waiter"
wait "\$w"
echo \$? > "$SYNCB/wstat"
echo \$(date +%s) > "$SYNCB/end"
EOF
chmod +x "$SYNCB/caller.sh"
setsid bash "$SYNCB/caller.sh" &
outerB=$!
for _ in $(seq 1 50); do [ -s "$SYNCB/waiter" ] && break; sleep 0.1; done
waiterB="$(cat "$SYNCB/waiter" 2>/dev/null)"
wstatB=""
for _ in $(seq 1 $(((CEIL2 + TOL2 + D) * 20))); do
	wstatB="$(cat "$SYNCB/wstat" 2>/dev/null)"
	[ -n "$wstatB" ] && break
	sleep 0.05
done
startB="$(cat "$SYNCB/start" 2>/dev/null)" endB="$(cat "$SYNCB/end" 2>/dev/null)"
elapsedB=$((${endB:-0} - ${startB:-0}))
[ "$wstatB" = 2 ] && ok "budget-passdown: the inner child self-terminated GRACEFULLY (status 2 via its OWN watchdog)" || bad "budget-passdown: status '${wstatB:-none}' (expected 2; a fresh full wall is blunt-KILLed 137)"
if [ -n "$wstatB" ] && [ "$elapsedB" -le "$((CEIL2 + TOL2))" ]; then
	ok "budget-passdown: terminated within wall+cleanup (~${elapsedB}s, ceiling ${CEIL2}s) from OUTER entry, not entry+delay+wall"
else
	bad "budget-passdown: elapsed ${elapsedB}s exceeds $((CEIL2 + TOL2))s (the inner child likely restarted the wall clock)"
fi
[ -n "$waiterB" ] && kill -KILL "$waiterB" 2>/dev/null
kill -KILL "$outerB" 2>/dev/null
rm -rf "$MOCKB" "$SYNCB"

# ---- D) repeated-run residue: 3 FAILING runs leave ZERO transport residue -----
# Each non-root run fails at the root check (status 2) AFTER the waiter created its transport (a
# 0700 DIR holding a 0600 FILE); the waiter's bounded cleanup rm must remove EXACTLY that owned
# tree, so NOTHING -- neither dir nor file -- accumulates. the original broken repro left three
# 0700 dirs AND three 0600 files; we assert both are zero (any entry under RESD is residue).
RESD="$(mktemp -d)"
for _ in 1 2 3; do
	TMPDIR="$RESD" WALL_CLOCK_MAX=10 CLEANUP_MAX=2 CURL_MAX=5 timeout -k 3 20 bash "$SCRIPT" >/dev/null 2>&1
done
resdirs=$(find "$RESD" -mindepth 1 -type d -name 'bloar-hs.*' 2>/dev/null | wc -l)
resfiles=$(find "$RESD" -mindepth 1 -type f 2>/dev/null | wc -l)
resany=$(find "$RESD" -mindepth 1 2>/dev/null | wc -l)
if [ "$resany" -eq 0 ]; then
	ok "residue: 3 failing runs left ZERO transport residue (0 dirs AND 0 files -- the bounded cleanup removed each owned tree)"
else
	bad "residue: residue left behind after 3 runs (dirs=$resdirs files=$resfiles total-entries=$resany)"
fi
rm -rf "$RESD"

if [ "$fails" -eq 0 ]; then
	echo "OUTER-BOUNDARY REGRESSION: PASS"
	exit 0
fi
echo "OUTER-BOUNDARY REGRESSION: $fails FAILED"
exit 1
