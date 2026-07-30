#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the outer waiter must learn the
# detached run's process group from a race-free CHILD->PARENT handshake, never by reading
# the child's pgid itself right after the fork -- before the child's setsid() runs it is
# still in the CALLER's group, so a racy capture would forward signals into the caller's
# group. Two parts:
#  (1) unit-tests of handshake_ok: it accepts only an OWN isolated child (pid==sid==pgid==
#      the child we spawned, and NOT our own group), refusing our/caller group, non-leaders,
#      a wrong pid, and a missing handshake.
#  (2) end-to-end: a mock `setsid` DELAYS isolation so the child lingers in a real caller
#      group that also holds a CANARY; we signal the waiter EARLY (before the handshake) and
#      prove the caller group is NEVER targeted (canary survives) while the eventual verifier
#      terminates. Reverting to a racy immediate pgid capture makes the early signal reach the
#      caller group -> the canary dies.
#  (3) bridge window: a forward that lands after the handshake but before arm_watchdog installs
#      its TERM handler must exit the documented status 2, not die 143.
# the regression case additions:
#  (4) the builtins-only REGULAR-FILE transport RETAINS a record the child wrote and
#      closed BEFORE the reader polls (the old FIFO discarded exactly that).
#  (5) unit test: sweep_failed_child reaps an isolated child's WHOLE group (leader +
#      descendant) from its REAL /proc, while an UNRELATED isolated canary survives untouched.
#  (6) end-to-end: the REAL entry, driven to report an adversarial tuple naming an
#      unrelated isolated group, refuses it and cleans up from the child's own /proc -- the
#      reported group and the caller group both survive; only the child's group is swept.
#  (7) end-to-end: an isolated child that hands off NO usable handshake but holds
#      a named, live descendant -- the whole group (descendant included) is reaped on timeout.
#  (8) transition boundary (still-in-our-group side): sweep_failed_child FREEZES the pid
#      with SIGSTOP before classifying, so a pid caught pre-setsid is reaped pid-ONLY and the
#      caller group (its canary) is never taken by a group kill.
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
REAL_SETSID="$(command -v setsid)"

# Run-scoped scratch: the verifier now creates its handshake
# transport as a `bloar-hs.XXXXXX` DIRECTORY under TMPDIR. Point TMPDIR at our OWN per-run tree
# and remove exactly that tree at the end -- never a global glob that could unlink a concurrent
# run's live transport. Every verifier we launch inherits this TMPDIR.
_TESTTMP="$(mktemp -d)"
export TMPDIR="$_TESTTMP"
trap 'rm -rf "$_TESTTMP" 2>/dev/null' EXIT

fails=0
ok()  { printf 'ok   %s\n' "$*"; }
bad() { printf 'BAD  %s\n' "$*"; fails=$((fails + 1)); }

# poll_group_empty PGID -> echo the group's remaining LIVE members after polling up to ~20s
# for it to drain. SIGKILL delivery/reaping is asynchronous, so the contract is "no
# descendant REMAINS", not "reaped within 0ms": under whole-suite load a swept process can
# sit as a ZOMBIE (state Z: dead, awaiting reap by init) for a while. A zombie is not a
# survivor, so it does not count; a real orphan is a LIVE process (running sleep) that never
# dies without the sweep, so it persists and the check stays load-bearing.
poll_group_empty() {
	local pg=$1 pid st live i line
	for i in $(seq 1 400); do
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

# --- (1) handshake_ok verification logic ---------------------------------------
# shellcheck source=/dev/null
source "$SCRIPT" # guarded main does not run
set +e
CH=5000
SELF=4000
handshake_ok $CH $SELF 5000 5000 5000 && ok "accepts an OWN isolated child (pid==sid==pgid==child, != self)" || bad "rejected a valid isolated child"
handshake_ok $CH $SELF 5000 5000 4000 && bad "FALSE-PASS: pgid == our own/caller group" || ok "rejects our own/caller group as a forwarding target"
handshake_ok $CH $SELF 5000 4999 5000 && bad "FALSE-PASS: sid != pid (not a session leader)" || ok "rejects a non-session-leader (sid!=pid)"
handshake_ok $CH $SELF 5000 5000 4999 && bad "FALSE-PASS: pgid != pid (not a group leader)" || ok "rejects a non-group-leader (pgid!=pid)"
handshake_ok $CH $SELF 5001 5001 5001 && bad "FALSE-PASS: pid is not the child we spawned" || ok "rejects a pid that isn't our spawned child"
handshake_ok $CH $SELF "" "" ""       && bad "FALSE-PASS: empty/missing handshake" || ok "rejects a missing handshake"

# --- (2) end-to-end: delayed isolation + early signal, caller group must survive ------
MOCK="$(mktemp -d)"
SYNC="$(mktemp -d)"
# A mock setsid DELAYS isolation, so the child lingers in the CALLER group (any racy
# immediate pgid capture would grab that group) and the handshake cannot complete yet.
cat >"$MOCK/setsid" <<EOF
#!/bin/bash
sleep 3
exec "$REAL_SETSID" "\$@"
EOF
# A mock id blocks the child INTERRUPTIBLY at the root check, so the ONLY thing that can
# terminate it within the window is the waiter forwarding the (latched) early signal -- its
# own watchdog is set far out (WALL=30). If the latch is dropped, the early signal is lost
# and the child stays blocked here.
printf '#!/bin/bash\nsleep 300\n' >"$MOCK/id"
chmod +x "$MOCK/setsid" "$MOCK/id"

# The caller SESSION: a setsid leader holding a CANARY and the waiter, both in the leader's
# (caller) group. The waiter is a CHILD (not the leader) so it takes the waiter path. The
# leader records the waiter's exit status so we can assert a defined (non-signal) return.
cat >"$SYNC/caller.sh" <<EOF
#!/bin/bash
echo \$\$ > "$SYNC/leader"
sleep 60 & echo \$! > "$SYNC/canary"
PATH="$MOCK:\$PATH" WALL_CLOCK_MAX=30 CLEANUP_MAX=2 CURL_MAX=5 bash "$SCRIPT" >/dev/null 2>&1 &
w=\$!
echo \$w > "$SYNC/waiter"
wait "\$w"
echo \$? > "$SYNC/waiter_status"
EOF
chmod +x "$SYNC/caller.sh"

setsid bash "$SYNC/caller.sh" &
outer=$!
for _ in $(seq 1 50); do [ -s "$SYNC/waiter" ] && break; sleep 0.1; done
waiter="$(cat "$SYNC/waiter" 2>/dev/null)"
canary="$(cat "$SYNC/canary" 2>/dev/null)"
leader="$(cat "$SYNC/leader" 2>/dev/null)"
# Capture the ISOLATED verifier pid ASYNCHRONOUSLY the instant it appears (session_child returns
# only a session leader, never the pacer -- review). A background poll, so the early signal
# below is NOT gated on isolation (the mock setsid delays it by 3s); we read the captured pid after.
child=""
(for _ in $(seq 1 200); do c="$(session_child "$waiter")"; [ -n "$c" ] && {
	printf '%s' "$c" >"$SYNC/vchild"
	break
}; sleep 0.05; done) &
vcap=$!

cleanup_harness() {
	[ -n "$leader" ] && kill -KILL "-$leader" 2>/dev/null
	[ -n "$canary" ] && kill -KILL "$canary" 2>/dev/null
	[ -n "$child" ] && kill -KILL "-$child" 2>/dev/null
	kill -KILL "$outer" "$vcap" 2>/dev/null
	rm -rf "$_TESTTMP" 2>/dev/null # our OWN per-run tree (holds every SYNC/MOCK and bloar-hs.* dir)
}
trap cleanup_harness EXIT

if [ -z "$waiter" ] || [ -z "$canary" ]; then
	bad "end-to-end setup failed (waiter='$waiter' canary='$canary')"
else
	# Deliver the signal to the waiter EARLY -- during the mock-setsid delay (the child has
	# NOT isolated yet), BEFORE the handshake can complete. It must be LATCHED, not lost.
	kill -TERM "$waiter" 2>/dev/null

	# The waiter must STAY ALIVE through the handshake window (latched, not default-killed).
	# Check mid-delay, while the mock setsid is still sleeping and the child is unisolated.
	sleep 1
	waiter_survived_handshake=no; kill -0 "$waiter" 2>/dev/null && waiter_survived_handshake=yes

	# Load-INDEPENDENT proof (no budget racing the watchdog): with the latch, the waiter applies
	# the held signal after verification, the child dies via the forwarded TERM, and the waiter
	# sweeps and propagates the CHILD's exit status (2). Wait for that recorded status -- however
	# long under load. If the latch were DROPPED, the waiter would instead be default-killed by
	# the early signal and record 143 almost immediately (and the child would survive). The wait
	# is bounded well under the child's own WALL=600 watchdog backstop, so a genuinely-lost
	# signal (child dies only at its watchdog, long after the already-dead waiter) still surfaces
	# as 143/none rather than a false 2.
	wstat=""
	for _ in $(seq 1 400); do wstat="$(cat "$SYNC/waiter_status" 2>/dev/null)"; [ -n "$wstat" ] && break; sleep 0.05; done # up to ~20s

	child="$(cat "$SYNC/vchild" 2>/dev/null)" # the ISOLATED verifier pid captured above (not the pacer)
	canary_alive=no; kill -0 "$canary" 2>/dev/null && canary_alive=yes
	waiter_alive=no; kill -0 "$waiter" 2>/dev/null && waiter_alive=yes
	verifier_alive=no; [ -n "$child" ] && kill -0 "$child" 2>/dev/null && verifier_alive=yes
	desc=""; [ -n "$child" ] && desc="$(poll_group_empty "$child")" # poll: reaping is async, "no descendant REMAINS"

	[ "$canary_alive" = yes ] && ok "caller group NOT targeted: the canary survived the early signal" || bad "the canary died: the early signal reached the caller group (racy pgid capture?)"
	[ "$waiter_survived_handshake" = yes ] && ok "the waiter STAYED ALIVE through the handshake (early signal latched, not default-killed)" || bad "the waiter died in the handshake window (early signal not latched?)"
	[ "$wstat" = 2 ] && ok "the LATCHED early signal was applied: the waiter propagated the child's DOCUMENTED status 2 (a dropped latch dies early with 143)" || bad "the waiter status was '${wstat:-none}', not the documented 2 (latch dropped -> 143, or the forward never terminated the child)"
	[ "$verifier_alive" = no ] && ok "the verifier terminated (the waiter propagated its exit)" || bad "the verifier is still alive: the early signal was lost (latch dropped?)"
	[ "$waiter_alive" = no ] && ok "the waiter terminated after applying the latched signal" || bad "the waiter is still alive"
	[ -z "$desc" ] && ok "no descendant remains in the verifier group" || bad "descendants remain in the verifier group: $desc"
fi

# (Former section 3, the deterministic bridge-window test, is retired: it relied on the
# _BLOAR_HS_HOLD env hook, which is now SCRUBBED at the fresh entry boundary as caller-supplied
# control state. The bridge trap `trap 'exit 2' TERM INT` stays in production and
# is exercised by test-signal-forwarding.sh, whose forward path lands in the handshake-to-arm
# window and requires the exact status 2.)

SYNC4="$(mktemp -d)"

# --- (4) drive the EXACT descriptor protocol the waiter uses, and
# prove it retains a child-written-FIRST record. The parent EXCLUSIVELY creates the 0600 file
# (umask 077 + noclobber == O_EXCL: it must also REFUSE a pre-planted symlink), opens BOTH a
# read fd and a child-write fd BEFORE spawn, and passes only the fd NUMBER. The child inherits
# the open write fd across its exec and writes via `>&$fd`, NEVER the path -- and it does so
# STRICTLY BEFORE the parent's poll-read. The record must be read. A FIFO (or a reader that had
# not pre-opened) would lose it. ------------------------------------------------------------
# 4a: exclusive create refuses a pre-planted symlink (the root-safety property).
g4sym="$SYNC4/sym"
: >"$SYNC4/victim"
ln -s "$SYNC4/victim" "$g4sym"
if (umask 077; set -C; : >"$g4sym") 2>/dev/null; then
	bad "group2: exclusive create FOLLOWED a pre-planted symlink (root-unsafe)"
else
	ok "group2: exclusive-create (O_EXCL) REFUSED a pre-planted symlink at the handshake path"
fi
# 4b: the fd-inheritance protocol retains a child-FIRST record.
g4dir="$(mktemp -d "$SYNC4/hs.XXXXXX")"
g4f="$g4dir/hs"
(umask 077; set -C; : >"$g4f") || bad "group2: could not exclusively create the handshake file"
g4mode="$(stat -c '%a' "$g4f" 2>/dev/null)"
[ "$g4mode" = 600 ] && ok "group2: the handshake file is created 0600" || bad "group2: handshake file mode is $g4mode, not 600"
exec {g4r}<"$g4f"
exec {g4w}>>"$g4f"
# child inherits g4w across exec; writes via the fd number, then exits (closing its copy) BEFORE
# the parent reads. The path is even unlinked first, to prove the child never touches it.
g4a="" g4b="" g4c=""
_BLOAR_G4_FD=$g4w setsid bash -c 'printf "%s %s %s\n" 4242 4243 4244 >&"$_BLOAR_G4_FD"' &
g4child=$!
exec {g4w}>&-
wait "$g4child" # the child has now written AND exited -- strictly before our read
read -r g4a g4b g4c <&"$g4r" 2>/dev/null
exec {g4r}<&-
if [ "$g4a $g4b $g4c" = "4242 4243 4244" ]; then
	ok "group2: the inherited-fd transport RETAINS a record the child wrote+exited BEFORE the poll (FIFO lost it)"
else
	bad "group2: a child-written-first record was lost over the fd protocol (got '$g4a $g4b $g4c')"
fi

# --- (5) the regression case: sweep_failed_child establishes ownership from the pid's REAL /proc,
# NEVER a reported tuple. Two controls in one shot: an isolated child with a LIVE DESCENDANT is
# reaped whole (missing-handshake-descendant); an UNRELATED isolated canary -- exactly what a
# hostile self-consistent tuple would name -- is left ALIVE (wrong-PID-canary). The function
# takes only the pid WE spawned, so it can never act on a tuple's group. --------------------
setsid bash -c 'sleep 300 & echo $! >"'"$SYNC4"'/desc"; sleep 300' &
g5child=$!
setsid sleep 300 & # unrelated isolated group; nothing to do with g5child
g5canary=$!
g5desc=""
for _ in $(seq 1 100); do
	[ -s "$SYNC4/desc" ] && is_isolated_session_leader "$g5child" && { g5desc="$(cat "$SYNC4/desc" 2>/dev/null)"; break; }
	sleep 0.05
done
if [ -z "$g5desc" ] || ! is_isolated_session_leader "$g5child"; then
	bad "group3: harness could not stage an isolated child+descendant (desc='$g5desc')"
else
	sweep_failed_child "$g5child" # ownership from g5child's /proc; sweeps ITS group only
	g5left="$(poll_group_empty "$g5child")"
	if [ -z "$g5left" ]; then
		ok "group3: the isolated child's WHOLE group was reaped (leader + descendant $g5desc), from its real /proc"
	else
		bad "group3: descendant/leader survived the sweep (remaining: $g5left)"
	fi
	if kill -0 "$g5canary" 2>/dev/null; then
		ok "group3: the UNRELATED isolated canary $g5canary SURVIVED (a hostile tuple naming it authorizes nothing)"
	else
		bad "group3: the unrelated canary $g5canary was killed -- ownership leaked to a non-child group"
	fi
fi
kill -KILL "-$g5canary" "$g5canary" 2>/dev/null
[ -n "$g5child" ] && kill -KILL "-$g5child" "$g5child" 2>/dev/null
rm -rf "$SYNC4"

# (Former sections 6 and 7, the _BLOAR_HS_FAKE-driven wrong-PID-canary and
# missing-handshake-descendant e2e cases, are retired: they relied on the _BLOAR_HS_FAKE env
# hook, now SCRUBBED at the fresh entry boundary. Their coverage lives in
# the sourced sweep_failed_child unit test (section 5): it proves the sweep reaps an isolated
# child's WHOLE group incl. descendants from its real /proc, and never acts on a reported tuple.)

# --- (8) transition boundary, the STILL-IN-OUR-GROUP side: a spawned pid that has NOT
# isolated (as it would be if caught pre-setsid) must be reaped pid-ONLY -- never by a group kill
# that would take our own caller group with it. sweep_failed_child FREEZES it first, so the
# classification cannot be raced by a late setsid. Here the child stays in our group and a caller
# canary sits in that same group: the canary MUST survive. -----------------------------------
sleep 300 & # a CALLER-group canary (our own group)
g8canary=$!
bash -c 'sleep 300' & # a spawned pid that never isolates: it stays in OUR group
g8child=$!
sleep 0.2
if is_isolated_session_leader "$g8child"; then
	bad "group3-transition: harness staged an isolated child (wanted an in-our-group one)"
else
	sweep_failed_child "$g8child" # STOP -> read frozen /proc (in our group) -> pid-only kill
	g8gone=no
	for _ in $(seq 1 100); do kill -0 "$g8child" 2>/dev/null || { g8gone=yes; break; }; sleep 0.05; done
	wait "$g8child" 2>/dev/null
	[ "$g8gone" = yes ] && ok "group3-transition: the in-our-group child was reaped (pid kill)" || bad "group3-transition: the in-our-group child survived"
	if kill -0 "$g8canary" 2>/dev/null; then
		ok "group3-transition: the caller-group canary SURVIVED (no group kill of our own group)"
	else
		bad "group3-transition: the caller-group canary was killed -- a group sweep hit our own group"
	fi
fi
kill -KILL "$g8canary" "$g8child" 2>/dev/null

# --- (9) STABLE-STATE gate: group authority requires a CONFIRMED stopped/dead snapshot. We
# stage a REAL isolated child + descendant, then feed sweep_failed_child a /proc where the child
# reads as RUNNING ('R') FOREVER (PROC override, below the sourced entry boundary), so the
# observation loop NEVER confirms a stable state within its budget and MUST refuse group authority.
# The isolated child's descendant therefore SURVIVES (only the leader pid is signalled).
SYNC9="$(mktemp -d)"
setsid bash -c 'sleep 300 & echo $! >"'"$SYNC9"'/desc"; sleep 300' &
g9child=$!
g9desc=""
for _ in $(seq 1 100); do
	[ -s "$SYNC9/desc" ] && is_isolated_session_leader "$g9child" && {
		g9desc="$(cat "$SYNC9/desc")"
		break
	}
	sleep 0.05
done
if [ -z "$g9desc" ]; then
	bad "stable-state-gate: harness could not stage an isolated child+descendant"
else
	fp="$SYNC9/proc"
	mkdir -p "$fp/$g9child"
	printf '%s (verify) R 1 %s %s 0 0 0\n' "$g9child" "$g9child" "$g9child" >"$fp/$g9child/stat" # always RUNNING
	(
		PROC="$fp" _deadline=$((EPOCHSECONDS + 1)) # tiny budget; the fake running state never confirms
		sweep_failed_child "$g9child"
	)
	# the descendant must be a LIVE process (state != Z), not merely a not-yet-reaped zombie of a
	# group kill -- poll briefly so an async group-KILL (the bug) surfaces as a dead/zombie descendant.
	g9live=no
	for _ in $(seq 1 40); do
		read -r g9l <"/proc/$g9desc/stat" 2>/dev/null || { g9live=no; break; }
		g9l="${g9l##*)}"
		set -- $g9l
		[ "${1:-}" = Z ] && { g9live=no; break; } || g9live=yes
		sleep 0.05
	done
	if [ "$g9live" = yes ]; then
		ok "stable-state-gate: an unconfirmed (RUNNING) child conferred NO group authority -- descendant $g9desc still LIVE"
	else
		bad "stable-state-gate: the descendant was swept (dead/zombie) without a confirmed stopped/dead snapshot"
	fi
fi
kill -KILL "-$g9child" "$g9child" 2>/dev/null
[ -n "$g9desc" ] && kill -KILL "$g9desc" 2>/dev/null
rm -rf "$SYNC9"

# --- (10) handshake reader fd MUST NOT leak into the child: the waiter opens its reader
# AFTER the spawn, so after the handshake the child holds NO descriptor naming the transport file.
MOCK10="$(mktemp -d)"
printf '#!/bin/bash\nsleep 300\n' >"$MOCK10/id" # interruptible post-arm hang so the child stays alive
chmod +x "$MOCK10/id"
PATH="$MOCK10:$PATH" WALL_CLOCK_MAX=20 CLEANUP_MAX=2 CURL_MAX=5 bash "$SCRIPT" >/dev/null 2>&1 &
g10ow=$!
g10ch=""
for _ in $(seq 1 100); do
	g10ch="$(session_child "$g10ow")"
	[ -n "$g10ch" ] && break
	sleep 0.05
done
sleep 0.5 # let the handshake write + fd close complete
if [ -z "$g10ch" ]; then
	bad "reader-fd-leak: could not stage a genuine isolated child"
else
	g10leak=$(ls -l "/proc/$g10ch/fd/" 2>/dev/null | grep -c 'bloar-hs')
	[ "${g10leak:-1}" -eq 0 ] && ok "reader-fd-leak: no child fd names the handshake file after the handshake" || bad "reader-fd-leak: $g10leak child fd(s) still name the handshake file"
fi
[ -n "$g10ch" ] && kill -KILL "-$g10ch" 2>/dev/null
kill -KILL "$g10ow" 2>/dev/null
pkill -KILL -f "$MOCK10/id" 2>/dev/null || true
rm -rf "$MOCK10"

# --- (11) TWO-READ TRANSITION oracle: the group authority MUST be decided from the SAME
# atomic snapshot that observed the stable state -- never a LATER independent read. Section 9 only
# proves an unconfirmed RUNNING state grants no authority; it stays GREEN even if a mutation re-reads
# the identity after the stable observation. Here the fake stat is a FIFO delivering TWO stopped
# snapshots in sequence: FIRST "T pgid=1 sid=1" (stable, but NON-authorizing: the group is not the
# child), THEN "T pgid=CHILD sid=CHILD" (authorizing). Correct code reads ONCE atomically, sees the
# non-authorizing identity, and pid-ONLY kills -> the isolated child's descendant SURVIVES. A
# mutation that re-reads the identity independently after the stable observation consumes the SECOND
# (authorizing) line and group-KILLs -> the descendant DIES. Descendant-alive proves the one-snapshot path.
SYNC11="$(mktemp -d)"
setsid bash -c 'sleep 300 & echo $! >"'"$SYNC11"'/desc"; sleep 300' &
g11child=$!
g11desc=""
for _ in $(seq 1 100); do
	[ -s "$SYNC11/desc" ] && is_isolated_session_leader "$g11child" && {
		g11desc="$(cat "$SYNC11/desc")"
		break
	}
	sleep 0.05
done
if [ -z "$g11desc" ]; then
	bad "two-read-transition: harness could not stage an isolated child+descendant"
else
	fp11="$SYNC11/proc"
	mkdir -p "$fp11/$g11child"
	mkfifo "$fp11/$g11child/stat"
	# writer: a STABLE-but-NON-authorizing snapshot, then a STABLE authorizing one; keep the fifo
	# open so the second line persists in the pipe buffer for a (buggy) later second read.
	(
		exec 3>"$fp11/$g11child/stat"
		printf '%s (verify) T 1 1 1\n' "$g11child" >&3                         # stable T, group=1 (NOT child)
		printf '%s (verify) T 1 %s %s\n' "$g11child" "$g11child" "$g11child" >&3 # stable T, group=child
		sleep 30
	) &
	g11w=$!
	sleep 0.3
	(
		PROC="$fp11" _deadline=$((EPOCHSECONDS + 2))
		sweep_failed_child "$g11child"
	)
	# the descendant must remain a LIVE process (state != Z), not swept by a group kill.
	g11live=no
	for _ in $(seq 1 40); do
		read -r g11l <"/proc/$g11desc/stat" 2>/dev/null || { g11live=no; break; }
		g11l="${g11l##*)}"
		# shellcheck disable=SC2086
		set -- $g11l
		[ "${1:-}" = Z ] && { g11live=no; break; } || g11live=yes
		sleep 0.05
	done
	if [ "$g11live" = yes ]; then
		ok "two-read-transition: group authority used the SAME atomic snapshot (non-authorizing) -- descendant $g11desc still LIVE"
	else
		bad "two-read-transition: the descendant was group-swept -- a later independent read saw the authorizing snapshot"
	fi
	kill "$g11w" 2>/dev/null
fi
kill -KILL "-$g11child" "$g11child" 2>/dev/null
[ -n "$g11desc" ] && kill -KILL "$g11desc" 2>/dev/null
rm -rf "$SYNC11"

if [ "$fails" -eq 0 ]; then
	echo "HANDSHAKE-RACE REGRESSION: PASS"
	exit 0
fi
echo "HANDSHAKE-RACE REGRESSION: $fails FAILED"
exit 1
