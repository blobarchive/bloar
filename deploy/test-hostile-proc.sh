#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: a hostile PROC env must be IGNORED by
# the EXECUTABLE entry, which pins PROC=/proc before any /proc read. Two spoof vectors are covered:
# both reproduced here by exec'ing the real entry with a fake proc tree built for the entry's own
# pid (exec preserves the pid, so $PROC/$$/stat resolves to what we planted):
#   1) PROC/<pid>/stat is a FIFO -> an unpinned pre-bound stat read BLOCKS with nothing to bound
#      it (the vulnerable entry stayed alive >5s under a 2s ceiling). Pinned: ignored, run self-bounds.
#   2) PROC/<pid>/stat is a fake REGULAR stat claiming sid==pgid==pid -> an unpinned entry believes
#      it is already isolated, SKIPS the setsid re-exec, and arms its group-killer on a bogus group
#      so a post-"arm" hang is never bounded. Pinned: it reads real /proc, re-execs under setsid,
#      and the watchdog bounds the hang.
# Both sections assert the run self-terminates within wall+cleanup from invocation; removing the
# PROC=/proc pin makes each hang past the deadline. A third section confirms the DUAL mode: when
# SOURCED (the parser/unit path), PROC injection below the entry boundary STILL works. SKIPs
# without setsid.

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

# run_hostile LABEL PLANT: launch the real entry through an exec-wrapper that plants a hostile
# $PROC/<pid>/stat (PLANT is 'fifo' or 'fakestat') and hangs `id` at the post-arm root check. With
# the pin the run self-terminates within wall+cleanup; echo "elapsed status" (status empty = hang).
run_hostile() {
	local label=$1 plant=$2
	local wcm=3 cum=2 mock sync mark
	mock="$(mktemp -d)" sync="$(mktemp -d)"
	mark="$sync/reached_root"
	# id: an interruptible post-arm hang at the root check. It drops a MARKER when reached, proving
	# the PINNED entry ignored the hostile PROC, isolated, and ran main -- not derailed earlier.
	printf '#!/bin/bash\n: >"%s"\nsleep 300\n' "$mark" >"$mock/id"
	chmod +x "$mock/id"
	# the wrapper plants the fake proc entry for ITS OWN pid, then EXECs the entry (pid preserved).
	cat >"$sync/wrapper.sh" <<EOF
#!/bin/bash
hp="$sync/proc"
mkdir -p "\$hp/\$\$"
if [ "$plant" = fifo ]; then
	mkfifo "\$hp/\$\$/stat"            # a blocking read for an unpinned entry
else
	printf '%s (verify) S 1 %s %s 0 0 0\n' "\$\$" "\$\$" "\$\$" > "\$hp/\$\$/stat" # fake: sid==pgid==pid
fi
exec env PROC="\$hp" PATH="$mock:\$PATH" WALL_CLOCK_MAX=$wcm CLEANUP_MAX=$cum CURL_MAX=5 bash "$SCRIPT"
EOF
	chmod +x "$sync/wrapper.sh"
	# a caller session so the wrapper is a NON-leader child: the fake "isolated" stat is a genuine
	# spoof (its real pgid is the caller's group, not its pid), and a canary must survive.
	cat >"$sync/caller.sh" <<EOF
#!/bin/bash
sleep 300 & echo \$! > "$sync/canary"
echo \$(date +%s) > "$sync/start"
bash "$sync/wrapper.sh" >/dev/null 2>&1 &
w=\$!
echo \$w > "$sync/waiter"
wait "\$w"
echo \$? > "$sync/wstat"
echo \$(date +%s) > "$sync/end"
EOF
	chmod +x "$sync/caller.sh"
	setsid bash "$sync/caller.sh" &
	local outer=$! deadline=$((wcm + cum + 6)) waiter canary st en kids p line i g
	for _ in $(seq 1 50); do [ -s "$sync/waiter" ] && break; sleep 0.1; done
	waiter="$(cat "$sync/waiter" 2>/dev/null)"
	canary="$(cat "$sync/canary" 2>/dev/null)"
	# capture the waiter's children (the isolated verifier + the pacer) WHILE alive, for the
	# descendants-zero oracle asserted BEFORE any force-clean.
	sleep 0.5
	kids="$(pgrep -P "$waiter" 2>/dev/null | tr '\n' ' ')"
	local wstat=""
	for _ in $(seq 1 $((deadline * 20))); do
		wstat="$(cat "$sync/wstat" 2>/dev/null)"
		[ -n "$wstat" ] && break
		sleep 0.05
	done
	st="$(cat "$sync/start" 2>/dev/null)" en="$(cat "$sync/end" 2>/dev/null)"
	local elapsed=$((${en:-0} - ${st:-0})) reached=no
	[ -e "$mark" ] && reached=yes
	# EXACT status: the pinned entry isolates, the watchdog TERMs the interruptible root-check hang,
	# and the child dies via its handled path -> documented status 2.
	[ "$wstat" = 2 ] && ok "$label: the entry self-terminated with the EXACT status 2 (hostile PROC ignored)" || bad "$label: status '${wstat:-none}' (expected 2)"
	# bounded, with a LOWER time bound proving the watchdog had to fire (not a fluke-fast exit).
	if [ -n "$wstat" ] && [ "$elapsed" -ge "$((wcm - 1))" ] && [ "$elapsed" -le "$deadline" ]; then
		ok "$label: self-terminated in ~${elapsed}s within [$((wcm - 1)),${deadline}]s (watchdog fired; not an instant fluke)"
	else
		bad "$label: elapsed ${elapsed}s outside [$((wcm - 1)),${deadline}]s"
	fi
	# reached-path marker: the pinned entry actually ran main (post-arm), not derailed by the PROC.
	[ "$reached" = yes ] && ok "$label: the post-arm root check was REACHED (entry isolated and ran main)" || bad "$label: the post-arm path was NOT reached -- the hostile PROC derailed the entry"
	kill -0 "$canary" 2>/dev/null && ok "$label: the caller canary survived" || bad "$label: the caller canary was killed"
	# descendants-zero, asserted BEFORE the force-clean below.
	g=""
	for i in $(seq 1 200); do
		g=""
		for p in $kids; do
			kill -0 "$p" 2>/dev/null || continue
			read -r line <"/proc/$p/stat" 2>/dev/null || continue
			line="${line##*) }"
			[ "${line%% *}" = Z ] || g="$g${g:+ }$p"
		done
		[ -z "$g" ] && break
		sleep 0.05
	done
	[ -z "$g" ] && ok "$label: zero verifier/pacer descendants remain before cleanup (${kids:-none} reaped)" || bad "$label: survivors before cleanup: $g"
	# reap
	[ -n "$waiter" ] && kill -KILL "-$waiter" "$waiter" 2>/dev/null
	kill -KILL "$canary" "$outer" 2>/dev/null
	pkill -KILL -f "$sync/wrapper.sh" 2>/dev/null || true
	rm -rf "$mock" "$sync"
}

# --- (1) PROC/<pid>/stat is a FIFO (pre-bound blocking read) -----------------------------------
run_hostile "hostile-proc-fifo" fifo
# --- (2) PROC/<pid>/stat is a fake isolated stat (spoofed SID/PGID proof) ----------------------
run_hostile "hostile-proc-fakestat" fakestat

# --- (3) DUAL mode: SOURCED injection below the entry boundary STILL works ---------------------
# The pin is executable-mode only; the parser/unit tests must be able to inject stat content.
# shellcheck source=/dev/null
(
	PROC=/proc # reset any inherited pin for the sourced context
	source "$SCRIPT" # guarded main does not run
	set +e
	fp="$_TESTTMP/fakeproc"
	mkdir -p "$fp/4242" "$fp/4243"
	printf '4242 (verify) S 1 4242 4242 0 0 0\n' >"$fp/4242/stat" # isolated: pgid==sid==pid
	printf '4243 (verify) S 1 9 9 0 0 0\n' >"$fp/4243/stat"       # NOT isolated: pgid==sid==9
	PROC="$fp" is_isolated_session_leader 4242 && echo INJECT_ISO_OK || echo INJECT_ISO_BAD
	PROC="$fp" is_isolated_session_leader 4243 && echo INJECT_NONISO_BAD || echo INJECT_NONISO_OK
) >"$_TESTTMP/inject.out" 2>/dev/null
if grep -q INJECT_ISO_OK "$_TESTTMP/inject.out" && grep -q INJECT_NONISO_OK "$_TESTTMP/inject.out"; then
	ok "sourced-mode: PROC injection below the entry boundary still resolves (isolated honored, non-isolated rejected)"
else
	bad "sourced-mode: PROC injection did not work (out: $(tr '\n' ' ' <"$_TESTTMP/inject.out"))"
fi

if [ "$fails" -eq 0 ]; then
	echo "HOSTILE-PROC REGRESSION: PASS"
	exit 0
fi
echo "HOSTILE-PROC REGRESSION: $fails FAILED"
exit 1
