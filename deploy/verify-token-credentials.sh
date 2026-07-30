#!/usr/bin/env bash
#
# verify-token-credentials.sh -- deployment verification for the systemd auth
# token credential handoff.
#
# WHAT IT PROVES, tied to the SHIPPED unit files (not synthesised properties):
#
#   1. Source permissions. The token source the units name (LoadCredential=) is
#      uid 0, gid 0, mode 0600, free of extended ACLs, and unreadable by both the
#      daemon's own user and an unrelated user. A daemon reading the resolved
#      credential copy cannot check this itself (docs 3.1), so it lives here.
#
#   2. A real archive + a no-op POST oracle. bloard is started as the User= the
#      shipped bloard.service names, with the token delivered exactly the way that
#      unit's LoadCredential= line delivers it. A no-op blobs POST with the correct
#      token must give curl-success + HTTP 200 + the exact {"blobs":[]} body; a
#      missing or wrong token must give exactly 401. This proves the archive
#      enforces auth -- it is NOT the authenticated-operation proof (that is 2b).
#
#   2b. The authenticated-operation proof: a REAL bloar-index process, under the
#      DynamicUser the shipped indexer unit sets and with its systemd-DELIVERED
#      credential (LoadCredential, not -token-file), runs publish-manifest against
#      the archive's fresh arbitrum-one head and reaches the authenticated POST
#      /manifest. Correct credential -> the mutation is accepted ("manifest
#      published"); a wrong one -> 401 at POST .../manifest; a missing one -> it
#      fails before the mutation. A curl cannot substitute for this. Evidence is
#      bound to THIS run: the two rejections capture the process output directly
#      (--pipe --wait, no journal), and the acceptance reads only the current
#      InvocationID's journal -- so a stale same-name entry cannot satisfy any
#      classifier.
#
#   3. Concurrent distinct UIDs. The accepted mutation unit holds alive under its
#      DynamicUser after "manifest published" (its command execs a bounded sleep);
#      while both it and the named-user archive are alive, each process's live uid
#      is read from /proc and asserted non-root and distinct. So the captured uid is
#      that of the process that authenticated the POST, alongside the archive's.
#
#   Every identity/credential property of the transient runs is DERIVED from the
#   real unit files (User=, LoadCredential= id, DynamicUser=), never a second
#   hardcoded copy -- so a unit with any of those removed or miswired fails here.
#
# The runs receive their token from throwaway root:root sources created here (so we
# know the values, can present them, and never read or disturb the real token); the
# real source is touched only by the read-only permission assertions in (1).
#
# WHY IT IS A SCRIPT AND NOT A go test
#
# The mechanism under test is systemd's: LoadCredential=, DynamicUser=, the named
# system user, and the per-unit $CREDENTIALS_DIRECTORY systemd populates as root.
# Reproducing it needs a real systemd system manager and root. This script
# therefore CANNOT run in CI or an unprivileged sandbox: it refuses without root
# and a reachable manager rather than reporting a hollow pass. Run it on a deploy
# host, as root, after installing the units and after any change to them or to the
# token's on-disk permissions.
#
# USAGE
#
#   sudo ./verify-token-credentials.sh
#
# Overridable via the environment:
#   BLOARD_UNIT_FILE  shipped bloard unit         (default <script dir>/systemd/bloard.service)
#   INDEX_UNIT_FILE   shipped indexer unit        (default <script dir>/systemd/bloar-index@.service)
#   BLOARD_BIN        bloard binary               (default /usr/local/bin/bloard)
#   INDEX_BIN         bloar-index binary          (default /usr/local/bin/bloar-index)
#   UNPRIV_USER       an unrelated non-root user  (default nobody; must differ from bloard's User=)
#   ARCHIVE_MPB       scratch archive's max_put_blobs (default 64; must be 1..1024)
#   CURL_MAX          per-request curl ceiling, s (default 5)
#   WALL_CLOCK_MAX    overall verifier ceiling, s (default 180)
#   CLEANUP_MAX       hard cleanup deadline, s    (default 30)

set -euo pipefail

# SCRIPT_DIR without an external `dirname` (the regression case: nothing external may run
# before the watchdog is armed, or a wedged/mocked binary could hang the bootstrap):
# strip the trailing /name with parameter expansion, then cd/pwd (both builtins). The
# command substitution forks a subshell but execs nothing.
_sd="${BASH_SOURCE[0]%/*}"
[ "$_sd" = "${BASH_SOURCE[0]}" ] && _sd=. # bare filename (no slash) -> current directory
SCRIPT_DIR="$(cd -- "$_sd" && pwd)"
unset _sd
PROC="${PROC:-/proc}" # process-info source; overridable so tests can supply fake stat files
BLOARD_UNIT_FILE="${BLOARD_UNIT_FILE:-$SCRIPT_DIR/systemd/bloard.service}"
INDEX_UNIT_FILE="${INDEX_UNIT_FILE:-$SCRIPT_DIR/systemd/bloar-index@.service}"
BLOARD_BIN="${BLOARD_BIN:-/usr/local/bin/bloard}"
INDEX_BIN="${INDEX_BIN:-/usr/local/bin/bloar-index}"
UNPRIV_USER="${UNPRIV_USER:-nobody}"
ARCHIVE_MPB="${ARCHIVE_MPB:-64}"
CURL_MAX="${CURL_MAX:-5}"            # per-request curl ceiling, seconds
WALL_CLOCK_MAX="${WALL_CLOCK_MAX:-180}" # overall verifier ceiling, seconds
CLEANUP_MAX="${CLEANUP_MAX:-30}"     # grace after the watchdog's TERM before it SIGKILLs the group
HOLD_SECS=15                          # how long the accepted mutation unit holds alive for the uid capture
OP_MAX=10                             # per manager/journal call ceiling, seconds
OP_KILL_GRACE=5                       # timeout's SIGTERM->SIGKILL grace for a wedged call
SDRUN_MAX=60                          # systemd-run ceiling (its --wait units self-bound via RuntimeMaxSec)
# The whole verifier lifecycle shares ONE runtime budget measured from OUTER-entry start
#: the operational ceiling is EXACTLY WALL_CLOCK_MAX + CLEANUP_MAX.
# The outer entry stamps _BLOAR_WALL_DEADLINE = EPOCHSECONDS + WALL_CLOCK_MAX once and exports
# it; the re-exec'd child inherits it and arms its watchdog to the REMAINING budget, and the
# waiter bounds every wait by _BLOAR_WALL_DEADLINE + CLEANUP_MAX. There is NO runtime slack --
# any small margin lives only in the TESTS as scheduler tolerance, never in the advertised bound.

# A high-entropy per-run nonce, so transient unit names never collide with a previous
# run's -- the root of binding evidence to THIS invocation. Computed builtin-only and
# only AFTER the watchdog is armed; declared empty here so a sourced
# test harness that never runs main() still has it defined under `set -u`.
RUN_NONCE=""
MAIN_PID=$$

# rand_hex sets REPLY to a random hex string using ONLY shell builtins -- no external
# command, so it can never wedge the run. It reads the kernel UUID
# source with the `read` builtin (a redirect open, not an exec) and strips the dashes,
# falling back to pid/RANDOM/SECONDS if that source is unavailable.
rand_hex() {
	if read -r REPLY <"$PROC/sys/kernel/random/uuid" 2>/dev/null && [ -n "$REPLY" ]; then
		REPLY="${REPLY//-/}"
	else
		REPLY="$$$RANDOM$RANDOM$SECONDS"
	fi
}

# All HTTP goes through this one bounded helper. A raw `curl` anywhere
# else is a missing timeout; the repo test TestVerifierRoutesCurlThroughHelper
# greps for one.
curlx() { curl -s --max-time "$CURL_MAX" "$@"; }

# Every systemd-manager / journal call is bounded with `timeout --kill-after`
#, so no wedged call can hang the run past OP_MAX+OP_KILL_GRACE.
# The overall backstop is the watchdog (arm_watchdog), which kills the process
# group even when a foreground child has deferred the main shell's TERM trap.
sctl()  { timeout -k "$OP_KILL_GRACE" "$OP_MAX" systemctl "$@"; }
jctl()  { timeout -k "$OP_KILL_GRACE" "$OP_MAX" journalctl "$@"; }
sdrun() { timeout -k "$OP_KILL_GRACE" "$SDRUN_MAX" systemd-run "$@"; }

# Output. FAILURES accumulates so every check runs before we exit non-zero.
FAILURES=0
info() { printf '     %s\n' "$*"; }
pass() { printf 'PASS %s\n' "$*"; }
warn() { printf 'WARN %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; FAILURES=$((FAILURES + 1)); }
die()  { printf 'ABORT %s\n' "$*" >&2; exit 2; }

# Transient unit bookkeeping, all in the parent shell (never a subshell), so the
# EXIT trap can reap every unit even on an interrupt.
UNITS=()
UNIT_COUNTER=0
REPLY_UNIT=""
alloc_unit() {
	UNIT_COUNTER=$((UNIT_COUNTER + 1))
	REPLY_UNIT="bloar-cred-smoke-$RUN_NONCE-$UNIT_COUNTER.service"
	UNITS+=("$REPLY_UNIT")
}

SCRATCH=""
WATCHDOG_PID=""
MAIN_PGID=""

cleanup() {
	# Run once (reset EXIT), and IGNORE TERM for the duration: a group-TERM can still be
	# in flight when we enter cleanup -- the watchdog's own polite TERM, or the very signal
	# a caller forwarded -- and if cleanup reset TERM to default it would itself be killed
	# mid-teardown, leaving systemd units un-stopped. Ignoring it lets teardown finish.
	trap - EXIT
	trap '' TERM
	# The terminal deadline must SURVIVE teardown. The old code SIGKILLed
	# the watchdog on entry, which disarmed the pending group-KILL before the bounded
	# `sctl stop`s ran -- so a wedged manager call could stretch teardown to
	# N*(OP_MAX+OP_KILL_GRACE) (four units ~= 120s), far past the advertised wall+cleanup
	# bound. Instead: arm a fresh ABSOLUTE cleanup killer that group-KILLs after CLEANUP_MAX
	# unless teardown finishes and cancels it, THEN retire the main watchdog (the killer now
	# owns the bound). Only when the watchdog was armed -- i.e. isolation was confirmed
	# (arm_watchdog) -- may we group-kill; else a stray group signal could reach a caller's
	# group, so we skip it. Any `sleep` orphaned by these SIGKILLs is reaped by the outer
	# waiter's group sweep after the run exits, so none survives.
	local cleanup_killer=""
	if [ -n "$WATCHDOG_PID" ]; then
		(
			trap '' TERM
			trap - EXIT
			sleep "$CLEANUP_MAX"
			kill -KILL "-$MAIN_PGID" 2>/dev/null # absolute bound on the cleanup phase itself
		) &
		cleanup_killer=$!
		# The watchdog ignores TERM (see arm_watchdog); SIGKILL it now that the cleanup
		# killer holds the terminal bound. It is no longer needed either way: on a normal
		# exit it never fired; on a timeout it already sent its group-TERM.
		kill -KILL "$WATCHDOG_PID" >/dev/null 2>&1 || true
	fi
	local u
	for u in "${UNITS[@]:-}"; do
		[ -n "$u" ] || continue
		sctl stop "$u" >/dev/null 2>&1 || true
		sctl reset-failed "$u" >/dev/null 2>&1 || true
	done
	[ -n "$SCRATCH" ] && rm -rf "$SCRATCH"
	# Teardown completed within CLEANUP_MAX -> cancel the absolute killer. If teardown had
	# instead wedged, we would never reach here and the killer would fire, so the whole run
	# still dies by wall+cleanup.
	[ -n "$cleanup_killer" ] && kill -KILL "$cleanup_killer" >/dev/null 2>&1 || true
}

# validate_durations checks EVERY duration/count knob with a STRICT positive-integer
# grammar, for two distinct reasons. First, a bare `[ "$val" -gt 0 ]`
# ACCEPTS some values that make the watchdog's `sleep "$WALL_CLOCK_MAX"` FAIL -- a
# trailing space ('1 '), a sign ('-5'), an arithmetic expression ('1+1') all give
# "invalid time interval"/"invalid option" -- which silently disarms the terminal bound.
# Second, other loose values sleep WOULD accept ('0', a leading space, a leading zero
# like '007', a decimal like '1.5', an oversized number) are still not meaningful
# deadlines, so we refuse them for strictness: a zero, ambiguous, or absurd bound is no
# deadline at all. We require ^[1-9][0-9]{0,5}$: 1..6 digits, no sign/space/leading-zero/
# decimal/expression, with a sane <=999999s ceiling. It runs BEFORE any watchdog or
# external work, so a bad value fails loudly up front.
validate_durations() {
	local v val
	for v in WALL_CLOCK_MAX CLEANUP_MAX CURL_MAX OP_MAX OP_KILL_GRACE SDRUN_MAX HOLD_SECS; do
		val="${!v}" # indirect expansion, not eval: no re-evaluation of the value's content
		[[ "$val" =~ ^[1-9][0-9]{0,5}$ ]] ||
			die "$v='$val' must be a strict positive integer of 1..6 digits (seconds). Some malformed values ('1 ', '-5', '1+1') make the watchdog's sleep fail and disarm it; others sleep would accept ('0', a leading space, a leading zero, a decimal, an oversized number) are refused for strictness -- a zero, ambiguous, or absurd bound is not a meaningful deadline."
	done
	# ARCHIVE_MPB is a COUNT knob (scratch archive's max_put_blobs, 1..1024). Canonicalize it with
	# the SAME strict grammar HERE, before any arithmetic context: the later
	# `[ "$ARCHIVE_MPB" -ge 1 ]` range check evaluates its operand as arithmetic, so an injected
	# array subscript like `BASH_SOURCE[$(cmd)]` would EXECUTE there. The `[[ =~ ]]` match (1..4
	# digits, no sign/space/leading-zero) runs no arithmetic on the raw value; the -le is safe after.
	[[ "$ARCHIVE_MPB" =~ ^[1-9][0-9]{0,3}$ ]] && [ "$ARCHIVE_MPB" -le 1024 ] ||
		die "ARCHIVE_MPB='$ARCHIVE_MPB' must be a strict integer 1..1024 (scratch archive max_put_blobs)."
}

# read_pgid_sid PID -> set REPLY_PGID and REPLY_SID from $PROC/PID/stat using ONLY shell
# builtins (the regression case: no ps/tr, so the pre-arm isolation checks exec no external
# command a wedged/mocked binary could hang). /proc/<pid>/stat is a single line
# "pid (comm) state ppid pgrp session ..."; the comm field is parenthesised and may
# itself contain spaces or ')', so we parse from AFTER the LAST ')': the remaining
# whitespace-split fields are state(1) ppid(2) pgrp(3) session(4) ... Returns nonzero if
# the stat line cannot be read or a field is missing.
read_pgid_sid() {
	local line rest
	REPLY_PGID="" REPLY_SID=""
	read -r line <"$PROC/$1/stat" 2>/dev/null || return 1
	rest="${line##*)}" # drop "pid (comm)"; rest = " state ppid pgrp session ..."
	# shellcheck disable=SC2086 # deliberate word-split of the remaining fields
	set -- $rest
	REPLY_PGID="${3:-}"
	REPLY_SID="${4:-}"
	[ -n "$REPLY_PGID" ] && [ -n "$REPLY_SID" ]
}

# read_state PID -> set REPLY_STATE to the scheduler state field (field 1 after "pid (comm)":
# R/S/D/T/Z/...). Builtins only, same /proc parse as read_pgid_sid. Nonzero if unreadable.
read_state() {
	local line
	REPLY_STATE=""
	read -r line <"$PROC/$1/stat" 2>/dev/null || return 1
	line="${line##*)}"
	# shellcheck disable=SC2086 # deliberate word-split of the remaining fields
	set -- $line
	REPLY_STATE="${1:-}"
	[ -n "$REPLY_STATE" ]
}

# read_stat PID -> set REPLY_STATE, REPLY_PGID, REPLY_SID from ONE /proc/PID/stat read, so a
# stopped/dead observation AND the group it authorizes come from the SAME atomic snapshot --
# never a later independent read that could see a changed state. Fields after
# "pid (comm)": state(1) ppid(2) pgrp(3) session(4). Builtins only. Nonzero if unreadable.
read_stat() {
	local line
	REPLY_STATE="" REPLY_PGID="" REPLY_SID=""
	read -r line <"$PROC/$1/stat" 2>/dev/null || return 1
	line="${line##*)}"
	# shellcheck disable=SC2086 # deliberate word-split of the remaining fields
	set -- $line
	REPLY_STATE="${1:-}" REPLY_PGID="${3:-}" REPLY_SID="${4:-}"
	[ -n "$REPLY_STATE" ]
}

# _pause SECONDS -- a builtins-only sub-second sleep for the bounded polls. When the outer waiter
# has armed its coproc pacer we block on that never-written pipe (a real delay, no external
# `sleep`); when called from a context without the pacer (the sourced unit tests) it returns at
# once and the caller's iteration cap bounds the loop. Late-binds _PACER, set only in the waiter.
_pause() {
	[ -n "${_PACER_PID:-}" ] && read -r -t "$1" -u "${_PACER[0]}" _ 2>/dev/null
	return 0
}

# is_own_transport_fd FD -> success iff FD names a decimal fd that is OPEN, WRITABLE, and a REGULAR
# inode. This is only a TYPE / WRITABILITY SANITY check so the nonce read + record write below
# cannot BLOCK on a pipe/fifo -- it is NOT authenticity: a caller can synthesize a
# writable regular fd (`9>file _BLOAR_HS_FD=9`), so fd type proves nothing about origin, and a
# spoofable env marker must never be trusted either. AUTHENTICITY is the MANDATORY nonce read that
# follows: the child writes its identity only after reading back the per-run nonce the waiter wrote
# into the transport inode before the spawn, so a forged write-only fd (fails the read) or a fresh
# readable fd (no nonce line) never elicits the write. Builtin `[` tests on /proc/self/fd/FD (our
# OWN descriptor table -- not the pid-keyed $PROC), so it execs nothing.
is_own_transport_fd() {
	local fd=$1
	case "$fd" in '' | *[!0-9]*) return 1 ;; esac
	[ -w "/proc/self/fd/$fd" ] && [ -f "/proc/self/fd/$fd" ]
}

# is_isolated_session_leader PID -> success iff PID is BOTH its own process-group leader
# AND its own session leader (pgid==pid AND sid==pid). The watchdog kills a whole process
# GROUP, so it may only be armed when that group is provably the verifier's OWN isolated
# session -- never a caller's inherited group. the regression case
# requires asserting SID==PID as well as PGID==PID, from the real process state rather
# than the _BLOAR_VERIFY_ISOLATED marker (which only guards the re-exec loop, and could be
# spoofed). Reads via /proc (read_pgid_sid), so it too is external-command-free.
is_isolated_session_leader() {
	local pid=$1
	read_pgid_sid "$pid" || return 1
	[ "$REPLY_PGID" = "$pid" ] && [ "$REPLY_SID" = "$pid" ]
}

# handshake_ok CHILD SELF_PGID GOT_PID GOT_SID GOT_PGID -> success iff the identity the
# detached child reported over the handshake is a trustworthy forwarding target: it is the
# child we spawned (GOT_PID == CHILD), that child is its OWN isolated session leader
# (GOT_SID == GOT_PGID == GOT_PID), and its group is NOT our own (GOT_PGID != SELF_PGID).
# Anything else -- a missing handshake, a non-leader, or (critically) the caller's/waiter's
# own group -- is REFUSED, so the waiter can never forward a signal into an unverified group
#.
handshake_ok() {
	local child=$1 self=$2 pid=$3 sid=$4 pgid=$5
	[ -n "$pid" ] && [ -n "$sid" ] && [ -n "$pgid" ] &&
		[ "$pid" = "$child" ] && [ "$sid" = "$pid" ] && [ "$pgid" = "$pid" ] &&
		[ "$pgid" != "$self" ]
}

# sweep_failed_child CHILD -- reap a detached child the waiter can NO LONGER trust to have
# handed off a verified group (a failed or absent handshake). A rejected handshake authorizes
# NOTHING about any REPORTED tuple: an attacker who wrote a self-consistent
# tuple for an UNRELATED isolated group must never get that group group-killed. So we establish
# ownership OURSELVES, from the REAL /proc of the pid WE spawned (CHILD is ours by construction).
#
# The classification is race-free: reading /proc of a LIVE child could see it
# in our group an instant before its setsid() flips it into its own -- a pid-only kill would then
# leak the descendants it went on to spawn. So we FIRST freeze CHILD with SIGSTOP (a stopped
# process cannot transition), THEN read its frozen /proc state, THEN act:
#   - isolated into its own session/group (sid == pgid == CHILD): SIGKILL that owned group, which
#     also ends the stopped leader, BEFORE the caller's wait/reap releases the pid;
#   - still in OUR group: SIGKILL only the pid (SIGCONT is moot -- the pid is already dying).
# A dead CHILD reads no /proc, so we fall through to the (harmless) pid kill.
#
# SIGSTOP is ASYNCHRONOUS: the signal is merely queued, so an instant
# after kill -STOP the child may still be RUNNING and could complete its setsid() before we read
# /proc. We therefore POLL the /proc state, bounded by the REMAINING budget, until it is a STABLE
# non-running snapshot -- group-stopped (T), traced-stop (t), a zombie (Z), or gone (unreadable) --
# BEFORE classifying. Only such a snapshot authorizes acting on the child's GROUP: a still-running
# or otherwise unverified state confers NO group authority (we then signal only the pid), because
# a running child could setsid out from under the /proc read and we would kill the wrong group.
sweep_failed_child() {
	local child=$1 authorized=no bound
	kill -STOP "$child" 2>/dev/null # request the freeze
	bound=$((EPOCHSECONDS + 5))     # the sourced unit tests have no _deadline; a small standalone cap
	[ -n "${_deadline:-}" ] && bound=$_deadline
	while [ "$EPOCHSECONDS" -lt "$bound" ]; do
		if ! read_stat "$child"; then
			authorized=yes # gone/unreadable: it can no longer transition (no group -- empty pgid/sid)
			break
		fi
		case "$REPLY_STATE" in
		T | t | Z)
			authorized=yes # stably stopped or a zombie: it can no longer transition
			break
			;;
		esac
		kill -STOP "$child" 2>/dev/null # (re)assert in case it was resumed or not yet delivered
		_pause 0.01
	done
	# Group authority is decided from the SAME atomic snapshot that proved the child stable
	# (REPLY_PGID/REPLY_SID come from read_stat above, NOT a later independent read): only if
	# it is provably its OWN isolated session/group leader do we sweep the group; a running/unverified
	# child (authorized=no, or a gone child with no pgid) is never signalled as a group.
	if [ "$authorized" = yes ] &&
		[ "${REPLY_PGID:-}" = "$child" ] && [ "${REPLY_SID:-}" = "$child" ]; then
		kill -KILL "-$child" 2>/dev/null || true # sweep the owned group (ends the stopped leader too)
	fi
	kill -KILL "$child" 2>/dev/null || true # reap the leader pid; an unauthorized group is never swept
}

# arm_watchdog launches the terminal watchdog. The watchdog is SELF-SUFFICIENT: it
# escalates SIGTERM->SIGKILL on the whole process GROUP on its own timers, so a
# foreground child that has deferred the main shell's TERM trap (bash does that)
# cannot keep the run alive. It ignores TERM so its own group-TERM does not kill it
# before it can KILL.
#
# The group it kills MUST be the verifier's OWN, never a caller's inherited group
#: the entry guard re-execs under setsid so this process is a
# session AND group leader. is_isolated_session_leader asserts BOTH here -- if it does
# not hold, the isolation failed and group-killing would be unsafe, so we refuse.
arm_watchdog() {
	if ! is_isolated_session_leader "$MAIN_PID"; then
		read_pgid_sid "$MAIN_PID" || true
		die "not an isolated session leader (pid=$MAIN_PID pgid=${REPLY_PGID:-?} sid=${REPLY_SID:-?}): the run is not isolated, so refusing to arm a group killer."
	fi
	MAIN_PGID="$MAIN_PID"
	# Arm to the REMAINING budget, not a fresh full wall: the outer
	# entry stamped the absolute wall deadline, and isolation/handshake already consumed part of
	# it, so we sleep only what is LEFT before the terminal escalation. Clamp at 0 (fire at once
	# if the budget is already spent). This is what keeps the whole lifecycle inside a single
	# WALL_CLOCK_MAX+CLEANUP_MAX from outer entry, rather than the inner run restarting the clock.
	local remaining=$((_BLOAR_WALL_DEADLINE - EPOCHSECONDS))
	[ "$remaining" -lt 0 ] && remaining=0
	trap 'die "wall-clock deadline (${WALL_CLOCK_MAX}s from outer entry) exceeded"' TERM
	# It ignores TERM so its OWN group-TERM does not kill it before it can KILL. Its `sleep`
	# children may be orphaned when cleanup SIGKILLs it, but the outer waiter's group sweep
	# reaps them after the run exits, so none survives.
	(
		trap '' TERM
		trap - EXIT
		sleep "$remaining"
		kill -TERM "-$MAIN_PGID" 2>/dev/null # polite: unblock a wedged child, let cleanup run
		sleep "$CLEANUP_MAX"
		kill -KILL "-$MAIN_PGID" 2>/dev/null # hard terminal bound, independent of the trap
	) &
	WATCHDOG_PID=$!
}

# --- unit-file parsing --------

# unit_value FILE KEY -> the last `KEY=...` value (systemd's last-wins), or empty.
unit_value() {
	grep -E "^$2=" "$1" | tail -n1 | sed "s/^$2=//"
}

BLOARD_USER="" BLOARD_CRED_ID="" BLOARD_CRED_SRC=""
INDEX_DYNUSER="" INDEX_CRED_ID="" INDEX_CRED_SRC=""
parse_units() {
	# Every identity/credential property the transient runs use is DERIVED here
	# from the shipped unit files -- never a second hardcoded copy that could pass
	# while the real unit drifted. Removing or changing User=, LoadCredential=, or
	# DynamicUser= in the real unit therefore fails this verification.
	[ -f "$BLOARD_UNIT_FILE" ] || die "bloard unit $BLOARD_UNIT_FILE not found (set BLOARD_UNIT_FILE)."
	[ -f "$INDEX_UNIT_FILE" ]  || die "indexer unit $INDEX_UNIT_FILE not found (set INDEX_UNIT_FILE)."

	BLOARD_USER="$(unit_value "$BLOARD_UNIT_FILE" User)"
	[ -n "$BLOARD_USER" ] || die "bloard unit has no User= line; the credential model runs bloard as the named non-root user 'bloar'."
	# Enforced exactly, per the approved contract: the shipped unit must run bloard
	# as 'bloar' (docs/operations.md 3.2). A unit changed to User=nobody, User=root,
	# or anything else fails here rather than being silently accepted.
	[ "$BLOARD_USER" = "bloar" ] || die "bloard unit runs as User=$BLOARD_USER, not the approved 'bloar'; refusing."

	local bloard_cred
	bloard_cred="$(unit_value "$BLOARD_UNIT_FILE" LoadCredential)"
	[ -n "$bloard_cred" ] || die "bloard unit has no LoadCredential= line: the token would never reach the daemon."
	BLOARD_CRED_ID="${bloard_cred%%:*}"
	BLOARD_CRED_SRC="${bloard_cred#*:}"

	INDEX_DYNUSER="$(unit_value "$INDEX_UNIT_FILE" DynamicUser)"
	[ "$INDEX_DYNUSER" = "yes" ] ||
		die "indexer unit is not DynamicUser=yes (got '${INDEX_DYNUSER:-unset}'); the approved topology runs the indexer under a throwaway uid."
	local index_cred
	index_cred="$(unit_value "$INDEX_UNIT_FILE" LoadCredential)"
	[ -n "$index_cred" ] || die "indexer unit has no LoadCredential= line."
	INDEX_CRED_ID="${index_cred%%:*}"
	INDEX_CRED_SRC="${index_cred#*:}"

	# The config references ${CREDENTIALS_DIRECTORY}/token, so the credential id
	# MUST be "token" or the delivered file has the wrong name and the daemon
	# cannot resolve it.
	[ "$BLOARD_CRED_ID" = "token" ] || fail "bloard LoadCredential id is '$BLOARD_CRED_ID', not 'token'; the config's \${CREDENTIALS_DIRECTORY}/token would not resolve."
	[ "$INDEX_CRED_ID" = "token" ]  || fail "indexer LoadCredential id is '$INDEX_CRED_ID', not 'token'; the config's \${CREDENTIALS_DIRECTORY}/token would not resolve."
	info "bloard: User=$BLOARD_USER, LoadCredential=$BLOARD_CRED_ID:$BLOARD_CRED_SRC"
	info "indexer: DynamicUser=$INDEX_DYNUSER, LoadCredential=$INDEX_CRED_ID:$INDEX_CRED_SRC"
}

# --- preflight -----------------------------------------------------------------

preflight() {
	local c
	# getfacl and timeout are REQUIRED: without getfacl the ACL gate
	# cannot run (absent is a hard failure, not a warning); without timeout the
	# cleanup calls are unbounded.
	for c in systemd-run systemctl runuser stat curl ss journalctl getfacl timeout; do
		command -v "$c" >/dev/null 2>&1 || die "$c not found; this deployment verification needs it."
	done
	sctl show --property=Version >/dev/null 2>&1 || die "cannot reach the systemd system manager (no PID 1 systemd?): run this on a real deploy host."
	{ [ "$ARCHIVE_MPB" -ge 1 ] && [ "$ARCHIVE_MPB" -le 1024 ]; } 2>/dev/null ||
		die "ARCHIVE_MPB='$ARCHIVE_MPB' is not a supported max_put_blobs (1..1024); the generated configs would be rejected."
	[ -x "$BLOARD_BIN" ] || die "bloard binary $BLOARD_BIN is missing or not executable (set BLOARD_BIN)."
	[ -x "$INDEX_BIN" ]  || die "bloar-index binary $INDEX_BIN is missing or not executable (set INDEX_BIN)."
	id "$BLOARD_USER" >/dev/null 2>&1 || die "the User= the bloard unit names, '$BLOARD_USER', does not exist; create it before deploying (docs/operations.md 3.2)."
	id "$UNPRIV_USER" >/dev/null 2>&1 || die "unrelated user '$UNPRIV_USER' does not exist (set UNPRIV_USER to any non-root account)."
	[ "$(id -u "$UNPRIV_USER")" -ne 0 ] || die "UNPRIV_USER '$UNPRIV_USER' is root; it must be an unrelated NON-root account."
	# The unrelated-denial user must NOT be the daemon user -- by name AND by
	# resolved uid -- or "an unrelated uid cannot read the source" and "the daemon
	# user cannot read the source" would collapse into one weaker check.
	[ "$UNPRIV_USER" != "$BLOARD_USER" ] || die "UNPRIV_USER '$UNPRIV_USER' is the daemon user; set it to a different non-root account so the two read-denial checks are distinct."
	[ "$(id -u "$UNPRIV_USER")" != "$(id -u "$BLOARD_USER")" ] || die "UNPRIV_USER '$UNPRIV_USER' resolves to the same uid as the daemon user '$BLOARD_USER'; the two read-denial checks would collapse onto one uid."
}

# A free localhost TCP port.
free_port() {
	local p i
	for i in $(seq 1 30); do
		p=$(((RANDOM % 20000) + 20000))
		ss -ltnH "sport = :$p" 2>/dev/null | grep -q . || { echo "$p"; return 0; }
	done
	return 1
}

# The main PID of a running unit, and the numeric uid of a live PID. Capturing the
# uid from the LIVE process (while both units run) is what proves two concurrent
# identities are distinct -- a dynamic uid read after the fact could have been
# recycled.
main_pid() { sctl show -p MainPID --value "$1" 2>/dev/null || true; }
pid_uid()  { [ -n "$1" ] && [ "$1" != "0" ] && stat -c '%u' "/proc/$1" 2>/dev/null || true; }

# unit_journal UNIT -> the journal of the unit's CURRENT invocation ONLY, keyed by
# its systemd InvocationID. Reading `journalctl -u <name>` instead would
# also return a prior run's entries under the same recycled unit name -- exactly
# the stale-evidence false positive review demonstrated. Empty when the unit
# has no invocation yet.
unit_invocation() { sctl show -p InvocationID --value "$1" 2>/dev/null || true; }
unit_journal() {
	local inv
	inv="$(unit_invocation "$1")"
	[ -n "$inv" ] || return 0
	jctl "_SYSTEMD_INVOCATION_ID=$inv" --no-pager 2>/dev/null || true
}

journal_tail() {
	info "---- journal (this invocation): $1 ----"
	unit_journal "$1" | tail -n 12 | sed 's/^/     | /'
}

# Token-error signatures shared by both binaries ("token_file" is a substring of
# bloard's "server.auth_token_file" and the indexer's "archive.token_file").
has_token_read_error() { printf '%s' "$1" | grep -qi 'token_file' && printf '%s' "$1" | grep -qi 'permission denied'; }
has_token_empty_error() { printf '%s' "$1" | grep -qi 'token_file' && printf '%s' "$1" | grep -qi 'is empty'; }
has_credential_dir_error() { printf '%s' "$1" | grep -qi 'CREDENTIALS_DIRECTORY'; }

# --- mutation classifiers (pure predicates) -----------------------------------
# Factored out so the COMMITTED regression deploy/test-stale-evidence.sh can feed
# them stale-vs-current evidence and prove the old false passes cannot return. A
# one-shot's evidence is its OWN captured output (systemd-run --pipe), never a
# journal read; the accepted mutation's is the CURRENT invocation's journal only.
# ARGS: rc, captured-output. Each matches an EXACT structured signal, so a loose
# substring cannot satisfy it:
#   - the 401 status has a boundary (`: 401` then EOL or a non-digit), so a longer
#     status like 4010 does not match;
#   - the missing case wants archclient/resolveTokenFile's structured "begins with
#     ${CREDENTIALS_DIRECTORY}, but that variable is unset", not a bare mention;
#   - success wants slog's `msg="manifest published"` event, not the phrase.
oneshot_is_wrong_401()    { local rc=$1 out=$2; [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -qE 'POST /bloar/v1/heads/arbitrum-one/manifest: 401($|[^0-9])'; }
oneshot_is_missing_cred() { local rc=$1 out=$2; [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -qF 'begins with ${CREDENTIALS_DIRECTORY}, but that variable is unset'; }
journal_shows_published() { printf '%s' "$1" | grep -qF 'msg="manifest published"'; }

# --- (1) source permissions ----------------------------------------------------

deny_read() {
	local user=$1 file=$2
	if runuser -u "$user" -- sh -c 'test -r "$1"' _ "$file"; then
		fail "user '$user' can read the token source $file; only root (systemd) should."
	else
		pass "user '$user' cannot read the token source $file."
	fi
}

check_source_permissions() {
	local src=$BLOARD_CRED_SRC
	[ "$INDEX_CRED_SRC" = "$src" ] ||
		fail "the two units load the token from different sources ($src vs $INDEX_CRED_SRC); they must share one."
	[ -e "$src" ] || { fail "the unit's credential source $src does not exist."; return; }

	local uid gid mode
	uid="$(stat -c '%u' "$src")"
	gid="$(stat -c '%g' "$src")"
	mode="$(stat -c '%a' "$src")"
	info "source $src: uid=$uid gid=$gid mode=$mode"
	[ "$uid" = "0" ]    || fail "source $src is owned by uid $uid, not 0 (root); the model is 0600 root:root."
	[ "$gid" = "0" ]    || fail "source $src has gid $gid, not 0 (root); a non-root group could be granted read."
	[ "$mode" = "600" ] || fail "source $src is mode $mode, not 600; only root (systemd) may read it."
	if [ "$uid" = "0" ] && [ "$gid" = "0" ] && [ "$mode" = "600" ]; then
		pass "source $src is 0600 root:root."
	fi

	# ACL hard gate. getfacl is required (preflight). Capture its output
	# and exit status SEPARATELY so an inspection failure is never collapsed into
	# "empty output = no ACLs": a getfacl that could not read the file is a FAILURE,
	# not a pass. Any named user:/group: entry is an ACL grant beyond owner/other.
	local facl frc
	set +e
	facl="$(getfacl -pc "$src" 2>/dev/null)"
	frc=$?
	set -e
	if [ "$frc" -ne 0 ]; then
		fail "could not inspect ACLs of $src (getfacl exit $frc); cannot verify the source carries no ACL grants."
	elif printf '%s\n' "$facl" | grep -qE '^(user|group):[^:]+:'; then
		fail "source $src has extended ACL entries granting access: $(printf '%s' "$facl" | grep -E '^(user|group):[^:]+:' | tr '\n' ' ')"
	else
		pass "source $src has no extended ACLs (getfacl inspected cleanly)."
	fi

	deny_read "$BLOARD_USER" "$src"
	deny_read "$UNPRIV_USER" "$src"
}

# --- (2) real archive, started and authenticated -------------------------------

BLOARD_UNIT="" BLOARD_PORT=""
start_archive() {
	# Unique per run so two verifiers on one host cannot collide on the store dir.
	local token_src=$1 store_dir="bloar-cred-smoke-store-$$-$RANDOM"
	if [ -e "/run/$store_dir" ]; then
		fail "RuntimeDirectory /run/$store_dir already exists; the per-run store dir is not collision-free."
		return 1
	fi
	pass "unique RuntimeDirectory: /run/$store_dir did not pre-exist (collision-free across concurrent verifiers)."
	BLOARD_PORT="$(free_port)" || { fail "could not find a free port for the archive."; return 1; }
	local cfg="$SCRATCH/bloard.yaml"
	cat >"$cfg" <<-YAML
		net: mainnet
		beacon:
		  genesis_time: 1606824023
		  seconds_per_slot: 12
		store:
		  path: /run/$store_dir
		server:
		  listen: "127.0.0.1:$BLOARD_PORT"
		  auth_token_file: "\${CREDENTIALS_DIRECTORY}/token"
		  max_put_blobs: $ARCHIVE_MPB
		heads:
		  all:
		    origin_slot: 8626176
		    seg_bits: 9
		    fanout_bits: 8
		    pin: { mode: full }
		  arbitrum-one:
		    origin_slot: 8626176
		    seg_bits: 13
		    fanout_bits: 8
		    pin: { mode: window, duration: 720h }
	YAML
	chmod 0644 "$cfg"

	alloc_unit
	BLOARD_UNIT=$REPLY_UNIT
	# RuntimeDirectory gives the named user a writable, auto-removed store under
	# /run; the credential comes from the SHIPPED unit's id, delivered to that
	# unit's User. RuntimeMaxSec bounds the run even if this script is killed. No
	# --wait: this is the daemon we then talk to.
	if ! sdrun --quiet --unit="$BLOARD_UNIT" --property=Type=exec \
		--property="User=$BLOARD_USER" \
		--property="RuntimeDirectory=$store_dir" \
		--property="LoadCredential=$BLOARD_CRED_ID:$token_src" \
		--property=RuntimeMaxSec=120 \
		-- "$BLOARD_BIN" run -config "$cfg"; then
		fail "systemd-run could not start the archive unit $BLOARD_UNIT."
		journal_tail "$BLOARD_UNIT"
		return 1
	fi

	# Wait for it to actually serve. If it never does, the token read (its first
	# startup step) is the prime suspect; classify from the journal.
	local i status=000
	for i in $(seq 1 30); do
		status="$(curlx -o /dev/null -w '%{http_code}' "http://127.0.0.1:$BLOARD_PORT/bloar/v1/heads" 2>/dev/null || true)"
		[ "$status" = "200" ] && break
		sleep 0.5
	done
	if [ "$status" != "200" ]; then
		local jtxt
		jtxt="$(unit_journal "$BLOARD_UNIT")"
		if has_token_read_error "$jtxt"; then
			fail "the archive could not read its credential token (permission denied): the LoadCredential handoff is broken."
		elif has_token_empty_error "$jtxt"; then
			fail "the archive read an empty credential token."
		elif has_credential_dir_error "$jtxt"; then
			fail "the archive could not resolve \${CREDENTIALS_DIRECTORY}: the unit is missing LoadCredential= or the config form is wrong."
		else
			fail "the archive did not come up on 127.0.0.1:$BLOARD_PORT within the timeout."
		fi
		journal_tail "$BLOARD_UNIT"
		return 1
	fi
	pass "archive started as User=$BLOARD_USER and read its credential token (serving on 127.0.0.1:$BLOARD_PORT)."
	return 0
}

# The archive no-op POST oracle (this is NOT the authenticated-operation proof --
# that is the real indexer mutation in check_indexer_mutation; this only proves
# the archive enforces auth with an exact status and response shape). The correct
# token must give: curl exit success AND HTTP 200 AND the exact no-op body
# {"blobs":[]}; a missing or wrong token must give exactly 401.
check_archive_auth() {
	local token=$1 url="http://127.0.0.1:$BLOARD_PORT/bloar/v1/blobs" body="$SCRATCH/blobs-resp.json"
	local without wrong with rc
	without="$(curlx -o /dev/null -w '%{http_code}' -X POST "$url" 2>/dev/null || true)"
	wrong="$(curlx -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer not-$token" "$url" 2>/dev/null || true)"
	set +e
	with="$(curlx -o "$body" -w '%{http_code}' -X POST -H "Authorization: Bearer $token" --data-binary @/dev/null "$url" 2>/dev/null)"
	rc=$?
	set -e

	if [ "$without" = "401" ]; then pass "a missing-token write is refused exactly 401."
	else fail "an unauthenticated POST returned $without, not 401; auth is not enforced."; fi
	if [ "$wrong" = "401" ]; then pass "a wrong-token write is refused exactly 401."
	else fail "a wrong-token POST returned $wrong, not 401; the token is not being compared."; fi

	local shape
	shape="$(tr -d ' \t\n' <"$body" 2>/dev/null || true)"
	if [ "$rc" -eq 0 ] && [ "$with" = "200" ] && [ "$shape" = '{"blobs":[]}' ]; then
		pass "the correct token no-op POST: request OK, HTTP 200, body {\"blobs\":[]}."
	else
		fail "the archive no-op POST oracle failed: request exit=$rc, http=$with, body=$(cat "$body" 2>/dev/null)."
	fi
}

# --- (2b) real bloar-index authenticated MUTATION -----------------------------
#
# The load-bearing auth proof: the REAL bloar-index process, under DynamicUser
# with its systemd-DELIVERED credential (LoadCredential, not -token-file), runs
# publish-manifest against the scratch archive's fresh arbitrum-one head. A fresh
# head has no manifest, so the preflight skips its L1 position check and reaches
# the authenticated POST /bloar/v1/heads/{head}/manifest -- so no L1 RPC is
# contacted (parent_chain_rpc points at a closed port and is never dialled for
# real). Correct credential -> the POST is accepted and the indexer logs "manifest
# published"; a wrong delivered credential -> the POST is 401; a missing credential
# -> the config's ${CREDENTIALS_DIRECTORY} cannot resolve and the mutation is never
# attempted.

# run_oneshot_publish_manifest LABEL CONFIG SRC : one publish-manifest attempt
# under DynamicUser that is EXPECTED TO FAIL. Its output is captured DIRECTLY from
# the process (systemd-run --pipe --wait) -- this run's own stdout+stderr, never a
# journal read -- so no stale entry under a recycled unit name can satisfy the
# classifier. SRC empty means NO LoadCredential (the "missing" case). The
# classifier requires the CURRENT run's specific error.
run_oneshot_publish_manifest() {
	local label=$1 cfg=$2 src=$3
	alloc_unit
	local unit=$REPLY_UNIT
	local props=(--property=Type=exec --property="DynamicUser=$INDEX_DYNUSER" --property=RuntimeMaxSec=30)
	if [ -n "$src" ]; then props+=(--property="LoadCredential=$INDEX_CRED_ID:$src"); fi
	local out rc=0
	set +e
	out="$(sdrun --pipe --wait --collect --quiet --unit="$unit" "${props[@]}" \
		-- "$INDEX_BIN" publish-manifest -config "$cfg" 2>&1)"
	rc=$?
	set -e
	sctl reset-failed "$unit" >/dev/null 2>&1 || true

	case "$label" in
	wrong)
		# Must fail AT the authenticated mutation: the archclient error names the
		# method, the exact path, and the 401 status of THIS run's POST.
		if oneshot_is_wrong_401 "$rc" "$out"; then
			pass "a wrong delivered credential is refused 401 at POST .../manifest (exit $rc, this run's captured output)."
		else
			fail "the wrong-credential publish-manifest did not fail at the manifest POST with 401 (rc=$rc)."
			printf '%s\n' "$out" | tail -n 4 | sed 's/^/     | /'
		fi ;;
	missing)
		if oneshot_is_missing_cred "$rc" "$out"; then
			pass "a missing credential (no LoadCredential) fails before the mutation, naming CREDENTIALS_DIRECTORY (exit $rc)."
		else
			fail "the missing-credential publish-manifest did not fail closed naming CREDENTIALS_DIRECTORY (rc=$rc)."
			printf '%s\n' "$out" | tail -n 4 | sed 's/^/     | /'
		fi ;;
	esac
}

# poll_held_success UNIT -> echoes "ok" if the unit's CURRENT invocation logged the
# structured success event, "exited" if the unit died first, "" on timeout. It reads
# ONLY unit_journal (the current InvocationID), so a stale by-name "manifest published"
# from a recycled unit name cannot satisfy it. Factored
# out of start_held_mutation so the committed regression can drive exactly this polling
# with stale-by-name-success + current-non-success evidence and prove that reverting
# unit_journal to a `journalctl -u <name>` read reintroduces the false pass.
poll_held_success() {
	local unit=$1 i state out
	for i in $(seq 1 "${HELD_POLL_TRIES:-40}"); do
		state="$(sctl show -p ActiveState --value "$unit" 2>/dev/null || true)"
		out="$(unit_journal "$unit")"
		if journal_shows_published "$out"; then echo ok; return 0; fi
		if [ "$state" = "failed" ] || [ "$state" = "inactive" ]; then echo exited; return 0; fi
		sleep 0.5
	done
	return 0
}

# start_held_mutation CONFIG SRC : the CORRECT run. It must SUCCEED at the
# authenticated mutation AND stay alive under its DynamicUser for the concurrent
# uid capture. The unit runs publish-manifest and, ONLY on success, holds
# with `exec sleep`; the DynamicUser uid is shared by the mutation and the hold, so
# the captured uid is that of the process that authenticated the POST. Success is
# read from the CURRENT invocation's journal via
# poll_held_success -- not a fixed sleep, and not a by-name read that could see a
# prior run's "manifest published".
HELD_UNIT=""
start_held_mutation() {
	local cfg=$1 src=$2
	alloc_unit
	HELD_UNIT=$REPLY_UNIT
	if ! sdrun --quiet --unit="$HELD_UNIT" --property=Type=exec \
		--property="DynamicUser=$INDEX_DYNUSER" \
		--property="LoadCredential=$INDEX_CRED_ID:$src" \
		--property="RuntimeMaxSec=$((HOLD_SECS + 30))" \
		-- /bin/sh -c '"$0" publish-manifest -config "$1" && exec sleep '"$HOLD_SECS" "$INDEX_BIN" "$cfg"; then
		fail "systemd-run could not start the correct-credential mutation unit $HELD_UNIT."
		return 1
	fi

	local outcome
	outcome="$(poll_held_success "$HELD_UNIT")"

	if [ "$outcome" = "ok" ]; then
		pass "the delivered credential authenticated a real mutation: publish-manifest logged 'manifest published' (this invocation), and the unit holds alive for the uid capture."
		return 0
	fi
	# Not observed on THIS invocation -> classify the failure from the current journal.
	local out state
	out="$(unit_journal "$HELD_UNIT")"
	state="$(sctl show -p ActiveState --value "$HELD_UNIT" 2>/dev/null || true)"
	if has_token_read_error "$out"; then
		fail "the correct-credential mutation failed on a token read (permission denied)."
	elif has_credential_dir_error "$out"; then
		fail "the correct-credential mutation could not resolve \${CREDENTIALS_DIRECTORY}."
	else
		fail "the correct-credential publish-manifest did not reach 'manifest published' (state=${state:-unknown}); the delivered credential did not authenticate the mutation."
	fi
	journal_tail "$HELD_UNIT"
	return 1
}

check_indexer_mutation() {
	local correct_src=$1 wrong_src=$2
	local cfg="$SCRATCH/publish-manifest.yaml"
	cat >"$cfg" <<-YAML
		beacon:
		  genesis_time: 1606824023
		archive:
		  url: http://127.0.0.1:$BLOARD_PORT
		  token_file: "\${CREDENTIALS_DIRECTORY}/token"
		  head: arbitrum-one
		chain:
		  parent_chain_rpc: http://127.0.0.1:1
		  sources:
		    - type: inbox-events
		      address: "0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"
		      from_block: 0
		index:
		  max_put_blobs: $ARCHIVE_MPB
	YAML
	chmod 0644 "$cfg"

	# The two rejections first (one-shots that exit), then the accepted one (which
	# publishes the genesis manifest and then holds alive for the uid capture).
	run_oneshot_publish_manifest missing "$cfg" ""
	run_oneshot_publish_manifest wrong "$cfg" "$wrong_src"
	start_held_mutation "$cfg" "$correct_src"
}

# --- (3) concurrent distinct UIDs ---------------------------------------------

# With BOTH the named-user archive and the accepted-mutation unit RUNNING, capture
# each process's live uid (from /proc, not the journal) and assert they are
# non-root and distinct -- before either is stopped. The mutation unit is the one
# that authenticated the POST and is still holding. A uid read after a
# sequential run could have been recycled, which is why the two must be alive at
# once.
capture_uids_concurrent() {
	local bpid ipid buid iuid want
	bpid="$(main_pid "$BLOARD_UNIT")"
	ipid="$(main_pid "$HELD_UNIT")"
	buid="$(pid_uid "$bpid")"
	iuid="$(pid_uid "$ipid")"
	want="$(id -u "$BLOARD_USER" 2>/dev/null || true)"
	info "concurrent: archive pid=$bpid uid=${buid:-?}; mutation unit pid=$ipid uid=${iuid:-?}"

	if [ -z "$buid" ]; then
		fail "could not read the live archive uid (pid ${bpid:-none}); is it still running?"
	elif [ "$buid" = "0" ]; then
		fail "the archive ran as uid 0; it must run as the non-root User=$BLOARD_USER."
	elif [ -n "$want" ] && [ "$buid" != "$want" ]; then
		fail "the archive ran as uid $buid, not $BLOARD_USER's uid ($want)."
	else
		pass "the archive ran as $BLOARD_USER (uid $buid, non-root)."
	fi

	if [ -z "$iuid" ]; then
		fail "could not read the live mutation-unit uid (pid ${ipid:-none}); did it stop holding?"
	elif [ "$iuid" = "0" ]; then
		fail "the mutation unit ran as uid 0; DynamicUser must allocate a non-root uid."
	else
		pass "the authenticated mutation ran under DynamicUser (uid $iuid, non-root)."
	fi

	if [ -n "$buid" ] && [ -n "$iuid" ]; then
		if [ "$buid" = "$iuid" ]; then
			fail "the archive and the mutation unit were running under the SAME uid $buid; the two identities must be distinct."
		else
			pass "the archive ($buid) and the authenticated mutation ($iuid) ran concurrently under distinct uids."
		fi
	fi
}

# Each verifier-hygiene property, asserted directly rather than left
# implied. The pre-store/pre-listener serve fail-closed is asserted separately by
# the Go regression TestServeFailsClosedWithoutCredentialDir (cmd/bloard).
check_hygiene() {
	local token_src=$1
	local m u g
	m="$(stat -c '%a' "$token_src" 2>/dev/null || true)"
	u="$(stat -c '%u' "$token_src" 2>/dev/null || true)"
	g="$(stat -c '%g' "$token_src" 2>/dev/null || true)"
	if [ "$m" = "600" ] && [ "$u" = "0" ] && [ "$g" = "0" ]; then
		pass "secure scratch: the throwaway token is 0600 root:root (install set that before any bytes)."
	else
		fail "the scratch token is mode $m owner $u:$g, not 0600 root:root."
	fi
	if [ "$CURL_MAX" -gt 0 ] 2>/dev/null; then
		pass "finite network bound: every request is capped at ${CURL_MAX}s."
	else
		fail "CURL_MAX is not a positive number: '$CURL_MAX'."
	fi
	if [ "$WALL_CLOCK_MAX" -gt 0 ] 2>/dev/null && [ -n "$WATCHDOG_PID" ] && kill -0 "$WATCHDOG_PID" 2>/dev/null; then
		pass "finite overall bound: the ${WALL_CLOCK_MAX}s watchdog (pid $WATCHDOG_PID) is running."
	else
		fail "the wall-clock watchdog is not active (WALL_CLOCK_MAX='$WALL_CLOCK_MAX', pid='${WATCHDOG_PID:-none}')."
	fi
	if trap -p EXIT | grep -q cleanup; then
		pass "cleanup on timeout: the EXIT trap is installed, so a watchdog SIGTERM still tears everything down."
	else
		fail "no EXIT trap installed; cleanup would not run on timeout."
	fi
}

main() {
	# Validate every duration knob BEFORE any watchdog or external work (the regression case
	# A bad bound must fail here, not after the run touches the system.
	validate_durations

	# Install the cleanup trap and ARM the watchdog BEFORE any external or
	# potentially-blocking work. NOTHING external runs
	# before this point: SCRIPT_DIR uses only builtins (parameter-expansion dirname, then
	# cd/pwd), the entry guard's isolation check reads /proc via the `read` builtin,
	# RUN_NONCE is deferred to just below, and validate_durations is pure. The root check
	# (`id -u`) and preflight (`id <user>`) then do NSS lookups that can hang forever
	# (LDAP/SSSD); with the watchdog already armed, even such a hang is bounded. cleanup
	# tolerates the empty SCRATCH/UNITS state it sees if an early check dies, and only
	# group-kills once arm_watchdog has confirmed isolation.
	trap cleanup EXIT
	arm_watchdog

	# Now that the terminal bound is live, compute the per-run nonce that names every
	# transient unit -- builtin-only, so it too cannot wedge.
	rand_hex
	RUN_NONCE="$REPLY"

	[ "$(id -u)" -eq 0 ] || die "must run as root: systemd sets up the credential handoff as root, and the source-permission check drops to other users. Re-run with sudo."
	parse_units
	preflight

	SCRATCH="$(mktemp -d /run/bloar-cred-smoke.XXXXXX)"
	chmod 0755 "$SCRATCH"

	# Throwaway credential sources whose values we know, so we can both deliver them
	# (LoadCredential) and present them (curl). Created 0600 root:root with install
	# BEFORE any bytes -- no 0644 instant even here. The real /etc/bloar/token is
	# never read. token-wrong is a DIFFERENT value, for the wrong-credential case.
	local token_src="$SCRATCH/token" wrong_src="$SCRATCH/token-wrong" token
	rand_hex
	token="smoke-$REPLY" # builtin-only random value; no external `head`
	install -m 0600 -o root -g root /dev/null "$token_src"
	printf '%s' "$token" >"$token_src"
	install -m 0600 -o root -g root /dev/null "$wrong_src"
	printf '%s' "not-$token" >"$wrong_src"

	printf '== bloar token credential verification ==\n'
	info "bloard unit: $BLOARD_UNIT_FILE"
	info "indexer unit: $INDEX_UNIT_FILE"

	printf '\n-- (0) verifier hygiene --\n'
	check_hygiene "$token_src"

	printf '\n-- (1) source permissions --\n'
	check_source_permissions

	printf '\n-- (2) real archive + no-op POST oracle --\n'
	if start_archive "$token_src"; then
		check_archive_auth "$token"
		printf '\n-- (2b) real bloar-index authenticated mutation (delivered credential) --\n'
		# missing + wrong (one-shots), then correct (holds alive as HELD_UNIT).
		if check_indexer_mutation "$token_src" "$wrong_src"; then
			printf '\n-- (3) concurrent distinct UIDs (archive + the mutation unit) --\n'
			capture_uids_concurrent
			[ -n "$HELD_UNIT" ] && sctl stop "$HELD_UNIT" >/dev/null 2>&1 || true
		fi
	else
		fail "skipping the authenticated, mutation, and uid checks: the archive did not start."
	fi

	printf '\n'
	if [ "$FAILURES" -ne 0 ]; then
		die "$FAILURES check(s) failed."
	fi
	printf 'OK: credential handoff verified against the shipped units for bloard and bloar-index.\n'
}

# Sourcing (for the classifier mock harness / tests) defines the functions without
# running the verification; executing runs it.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
	# Executable entry: pin PROC to the real /proc UNCONDITIONALLY. The
	# top-level PROC="${PROC:-/proc}" default is overridable only when SOURCED -- below this
	# entry boundary, for tests that inject a fake /proc into is_isolated_session_leader. The
	# executable path must never read a caller-controlled PROC: it was spoofable and hangable
	# (pointing PROC at a fifo hung the pre-bound $PROC/$$/stat read).
	PROC=/proc
	# Resolve `rm` to an ABSOLUTE path from the standard locations so a caller's PATH override
	# cannot SHADOW the waiter's cleanup remove: the teardown must be both bounded
	# AND effective -- a hostile TERM/INT-ignoring `rm` on PATH otherwise leaves the transport dir
	# behind (a bare `rm` there is killed by the timeout before it unlinks anything). An absolute
	# path bypasses PATH entirely; only root could plant a hostile /usr/bin/rm, which is outside the
	# trusted-operator model. Fall back to a bare `rm` only if no standard path exists.
	RM=rm
	for _c in /usr/bin/rm /bin/rm; do [ -x "$_c" ] && { RM=$_c; break; }; done
	# Validate every duration/count knob with the STRICT regex BEFORE any arithmetic context
	#. WALL_CLOCK_MAX/CLEANUP_MAX feed the `$(( ))` deadline below, and
	# bash EVALUATES array subscripts inside arithmetic -- so an injected value like
	# `x[$(cmd)]` would EXECUTE `cmd` pre-bound. validate_durations is a `[[ =~ ]]` match (no
	# arithmetic on the raw value) and execs nothing, so it is safe to run first.
	validate_durations
	# Trust of caller-supplied control state is NEVER gated on the _BLOAR_VERIFY_ISOLATED marker
	#: the marker is env, hence spoofable, so it may ONLY break the re-exec loop
	# below -- it must not decide whether a caller's fd/deadline is honoured. The two things a caller
	# could inherit are handled UNCONDITIONALLY where they are used, with no marker:
	#   - the wall deadline is CLAMPED to at most a fresh now+WALL every place it is consumed (the
	#     outer stamps it fresh; the child clamps min(inherited, fresh)), so a forged value can never
	#     extend the ceiling;
	#   - the transport fd is trusted only by the MANDATORY nonce read in the child path, so a forged
	#     fd never elicits the identity write.
	# The only purely-cosmetic caller hooks that no production path reads (_BLOAR_HS_HOLD/_FAKE, kept
	# solely so the tests can prove they are inert) are unset here unconditionally as hygiene.
	#
	# SECURITY FRAMING (the threat model). The one and only THIRD-PARTY-affecting operation
	# in this script -- the watchdog's group-KILL -- is authorized SOLELY from the REAL /proc state
	# (sweep_failed_child / arm_watchdog require sid==pgid==pid), NEVER from the marker or an fd. A
	# spoofed marker at most lets a caller's OWN already-isolated run consume its OWN (clamped) bound
	# or, if it fully populates its OWN readable fd, write its OWN identity to its OWN file --
	# self-directed and inert, reaching no other process, acceptable within the operator model.
	unset _BLOAR_HS_HOLD _BLOAR_HS_FAKE
	# ONE runtime budget measured from OUTER-entry start: the deadline stamping is
	# builtins-only (EPOCHSECONDS), so it precedes every external. The OUTER entry ALWAYS stamps it
	# FRESH; the genuine marked child consumes the outer's absolute deadline AS-IS; any other entry
	# already scrubbed the inherited value above and stamps its own fresh bound.
	#
	# Isolate into our OWN session/process group before doing anything, so the watchdog's group-kill
	# can only ever reach the verifier's children -- never the caller's inherited group. We decide
	# from the REAL process state read via /proc with builtins, not the
	# _BLOAR_VERIFY_ISOLATED marker: if we are not already an
	# isolated session leader (sid==pgid==pid), re-exec under setsid. The marker only breaks an
	# infinite re-exec loop -- if set yet isolation still did not take, that is unrecoverable.
	if ! is_isolated_session_leader "$$"; then
		if [ -n "${_BLOAR_VERIFY_ISOLATED:-}" ]; then
			read_pgid_sid "$$" || true
			printf 'ABORT re-exec under setsid did not yield an isolated session (sid=%s pgid=%s pid=%s); refusing.\n' "${REPLY_SID:-?}" "${REPLY_PGID:-?}" "$$" >&2
			exit 2
		fi
		command -v setsid >/dev/null 2>&1 ||
			{ printf 'ABORT setsid not found; it is required to isolate the verifier'\''s process group.\n' >&2; exit 2; }
		# LATCH pre-verification signals FROM THE VERY START --
		# before the coproc pacer or the child spawn. Until the handshake verifies we have no
		# legitimate forwarding target, but an INT/TERM must NOT be lost to the waiter's default
		# termination (which would orphan a still-isolating child). So INT/TERM only RECORD a
		# pending TERM here; we apply it to the VERIFIED group once the handshake completes. INT
		# is recorded as TERM: the detached async child inherited SIGINT ignored and
		# cannot un-ignore it, so TERM is its handled path).
		_pending=""
		trap '_pending=TERM' TERM INT
		# errexit off for the rest of the waiter: `read`/`wait` return 128+signum when a trap
		# fires, which under `set -e` would exit us prematurely; we handle those returns by hand.
		set +e
		export _BLOAR_VERIFY_ISOLATED=1
		# OUTER entry: ALWAYS stamp the wall deadline FRESH -- ignore any inherited
		# value, so a caller can never extend the bound. Builtins-only (EPOCHSECONDS), so it precedes
		# every external below; the whole lifecycle then fits EXACTLY WALL_CLOCK_MAX+CLEANUP_MAX here.
		_BLOAR_WALL_DEADLINE=$((EPOCHSECONDS + WALL_CLOCK_MAX))
		export _BLOAR_WALL_DEADLINE
		_deadline=$((_BLOAR_WALL_DEADLINE + CLEANUP_MAX))
		# Reserve a slice of the cleanup budget for the waiter's OWN teardown (the transport rm), so
		# it runs INSIDE the ceiling and never past it: the operational waits below
		# end by _op_deadline, leaving [_op_deadline, _deadline] for the rm. Kept >= the wall deadline.
		_op_deadline=$((_deadline - 1))
		[ "$_op_deadline" -lt "$_BLOAR_WALL_DEADLINE" ] && _op_deadline=$_BLOAR_WALL_DEADLINE
		# Builtins-only pacer: a coproc is a forked bash subshell that blocks on
		# `read` and EXECs nothing, so `read -t` on its pipe is a pure-bash sleep. It is itself an
		# UNISOLATED child: tracked and reaped on EVERY exit path by the cleanup trap below.
		coproc _PACER { read -r _; }
		bsleep() { _pause "$1"; }
		# On EVERY waiter exit: reap the pacer AND remove our OWN transport dir. The rm is a BOUNDED
		# external inside the cleanup budget, `timeout`-wrapped so a wedged
		# rm cannot outlive wall+cleanup, so nothing is left behind and no residue accumulates.
		_hs_dir=""
		# On EVERY waiter exit: reap the pacer, CLOSE every descriptor we still hold
		# (the read/write handshake fds), and remove EXACTLY our own transport dir -- all inside the
		# one ceiling.
		_cleanup_waiter() {
			[ -n "${_PACER_PID:-}" ] && {
				kill "$_PACER_PID" 2>/dev/null
				wait "$_PACER_PID" 2>/dev/null
			}
			[ -n "${_hrfd:-}" ] && { exec {_hrfd}<&- || :; } 2>/dev/null
			[ -n "${_hwfd:-}" ] && { exec {_hwfd}>&- || :; } 2>/dev/null
			# Remove our OWN transport dir within the reserved teardown budget. We
			# bound it by `timeout -s KILL` to the time LEFT before the ABSOLUTE _deadline -- SIGKILL
			# at expiry, no `-k` TERM grace that would overshoot -- so a wedged rm dies BY _deadline,
			# never past it. If the ceiling is already spent (no reserve left), skip rather than
			# overshoot. A real rm of the tiny dir is sub-millisecond, well inside the reserve.
			if [ -n "$_hs_dir" ]; then
				local _rem2=$((_deadline - EPOCHSECONDS))
				[ "$_rem2" -ge 1 ] && timeout -s KILL "$_rem2" "$RM" -rf "$_hs_dir" 2>/dev/null
			fi
		}
		trap _cleanup_waiter EXIT
		# Root-safe handshake transport. Create the secure 0700 dir UNDER
		# the active bound: `timeout` wraps mktemp (budget-derived ceiling) so a wedged/hostile mktemp
		# still dies by wall+cleanup -- the deadline already exists. Then do the O_EXCL open in THIS
		# process: `set -C` makes `exec {fd}>` use O_CREAT|O_EXCL (refusing any pre-planted
		# symlink/FIFO) and we KEEP the fd -- a subshell open would close it, forcing a pathname
		# REOPEN (a TOCTOU window). The read endpoint is derived from the SAME inode via
		# /proc/self/fd/<wfd>, so the pathname is NEVER trusted again after the exclusive create.
		# Bound mktemp so BOTH its TERM and the KILL escalation fit the remaining budget: give the
		# TERM (_rem-1)s and a tight 1s grace, so a wedged mktemp is dead by the deadline -- not the
		# 5s operational OP_KILL_GRACE, which would overshoot wall+cleanup.
		_rem=$((_deadline - EPOCHSECONDS - 1))
		[ "$_rem" -lt 1 ] && _rem=1
		_hs_dir="$(timeout -k 1 "$_rem" mktemp -d "${TMPDIR:-/tmp}/bloar-hs.XXXXXX")"
		[ -n "$_hs_dir" ] && [ -d "$_hs_dir" ] ||
			{ printf 'ABORT could not create a secure handshake directory within the bound.\n' >&2; exit 2; }
		_hs="$_hs_dir/hs"
		umask 077
		set -C
		if ! exec {_hwcreate}>"$_hs"; then
			set +C
			printf 'ABORT could not exclusively create the handshake file %s.\n' "$_hs" >&2
			exit 2
		fi
		set +C
		# Per-run NONCE that the child must read back before it writes its identity.
		# Builtins-only ($RANDOM), so no external runs on the handshake path. Its job is to make the
		# GENUINE readable transport distinguishable from a forged write-only fd (which cannot be
		# read) or a fresh empty caller fd (no nonce line), and to let the waiter confirm the
		# responder actually read OUR inode.
		_hs_nonce="${RANDOM}${RANDOM}${RANDOM}${RANDOM}"
		printf 'NONCE %s\n' "$_hs_nonce" >&"$_hwcreate"
		# Reopen the SAME inode O_RDWR at offset 0 so the fd the child INHERITS can READ the nonce
		# back (the O_EXCL create fd is O_WRONLY and now positioned past the nonce). `<>` does not
		# truncate, so the nonce header survives; the -ef check confirms it is still our own inode.
		exec {_hwfd}<>"/proc/self/fd/$_hwcreate" ||
			{ printf 'ABORT could not reopen the handshake file for the inherited transport.\n' >&2; exit 2; }
		[ "/proc/self/fd/$_hwcreate" -ef "/proc/self/fd/$_hwfd" ] ||
			{ printf 'ABORT the reopened handshake transport is not the exclusively-created inode.\n' >&2; exit 2; }
		exec {_hwcreate}>&- # done with the O_WRONLY create fd; the child inherits the O_RDWR one
		# The child does NOT authenticate this fd by its type (a caller can forge a writable regular
		# fd -- review); it trusts _BLOAR_HS_FD only by reading back the nonce we just wrote
		# into its inode. A caller who did not go through this waiter has no fd carrying our nonce.
		export _BLOAR_HS_FD="$_hwfd"
		# setsid detaches the controlling terminal; we keep a THIN outer waiter here that forwards
		# INT/TERM into the VERIFIED session group. We spawn BEFORE opening our reader:
		# the child then inherits ONLY the write fd, never the reader -- a reader opened first
		# would leak into the child as a live handle on the handshake inode.
		setsid bash "$SCRIPT_DIR/${BASH_SOURCE[0]##*/}" "$@" &
		_child=$!
		# Read side, opened AFTER the spawn so it is NOT inherited: reopen through /proc/self/fd/<wfd>
		# -- a NEW open-file description on the SAME inode (independent offset from the child's write
		# fd -- review), never the mutable pathname -- then fstat-verify SameFile.
		exec {_hrfd}<"/proc/self/fd/$_hwfd" ||
			{ printf 'ABORT could not open the handshake read endpoint from the held inode.\n' >&2; exit 2; }
		[ "/proc/self/fd/$_hwfd" -ef "/proc/self/fd/$_hrfd" ] ||
			{ printf 'ABORT the handshake read endpoint is not the same inode as the exclusively-created write fd.\n' >&2; exit 2; }
		exec {_hwfd}>&- # the child holds the write end now; the waiter keeps only its read fd
		read_pgid_sid "$$"
		_self_pgid="${REPLY_PGID:-$$}" # our OWN group -- never a valid forwarding target
		# Poll our READ fd for the child's reply, pacing with the builtin bsleep, until it arrives,
		# the child dies, or the ONE deadline elapses. The inode holds our own "NONCE <nonce>" header
		# line first, then the child's "<nonce> pid sid pgid" reply: we SKIP the header and accept
		# only a line whose first field is OUR nonce (so the responder provably read our inode -- a
		# forged tuple on a caller fd carries a different or absent nonce). A read at end-of-file
		# consumes nothing (position stays) and the inode RETAINS the record, so a child that wrote-
		# and-exited before our first poll is still read on a later pass.
		_c_pid="" _c_sid="" _c_pgid="" _f1="" _f2="" _f3="" _f4=""
		while :; do
			if read -r _f1 _f2 _f3 _f4 <&"$_hrfd" 2>/dev/null; then
				if [ "$_f1" = "$_hs_nonce" ] && [ -n "$_f4" ]; then
					_c_pid="$_f2" _c_sid="$_f3" _c_pgid="$_f4"
					break
				fi
				continue # our NONCE header (or a partial line) -- read the next
			fi
			kill -0 "$_child" 2>/dev/null || break
			[ "$EPOCHSECONDS" -ge "$_op_deadline" ] && break
			bsleep 0.05
		done
		exec {_hrfd}<&-
		if ! handshake_ok "$_child" "$_self_pgid" "$_c_pid" "$_c_sid" "$_c_pgid"; then
			printf 'ABORT isolation handshake did not verify an OWN child group (child=%s self_pgid=%s got pid=%s sid=%s pgid=%s); refusing to forward.\n' \
				"$_child" "$_self_pgid" "${_c_pid:-?}" "${_c_sid:-?}" "${_c_pgid:-?}" >&2
			# A failed/absent handshake authorizes NOTHING about the reported tuple:
			# sweep_failed_child FREEZES the pid we spawned with SIGSTOP, then classifies from its
			# REAL /proc so it cannot setsid out from under us, sweeping only its own group (or, if
			# still in ours, only the pid). The reported tuple is never a kill target.
			sweep_failed_child "$_child"
			wait "$_child" 2>/dev/null
			exit 2
		fi
		_child_pgid="$_c_pgid" # verified: == $_child, an isolated session leader, not our group
		# Verified: install the real forwarding traps, then APPLY any signal latched during the
		# handshake window so an early Ctrl-C is never lost.
		_forward() { kill -"$1" "-$_child_pgid" 2>/dev/null || true; }
		trap '_forward TERM' TERM
		trap '_forward TERM' INT
		[ -n "$_pending" ] && _forward TERM
		# Wait for the detached run, bounded by the SAME deadline (the child self-bounds via its
		# own watchdog armed to the remaining budget, but the waiter enforces the one ceiling
		# regardless): poll kill-0, paced by the builtin bsleep; on the deadline, group-KILL. Then
		# reap for the exit status.
		while kill -0 "$_child" 2>/dev/null; do
			[ "$EPOCHSECONDS" -ge "$_op_deadline" ] && { kill -KILL "-$_child_pgid" 2>/dev/null; break; }
			bsleep 0.1
		done
		wait "$_child"
		_rc=$?
		# The detached run's main shell has exited; sweep its VERIFIED group to reap any orphan
		# (a timer sleep, or an interrupted operation's child) so nothing lingers. We sit
		# OUTSIDE that group, so this cannot touch the waiter/caller.
		kill -KILL "-$_child_pgid" 2>/dev/null || true
		exit "$_rc"
	fi
	# We are the isolated session leader (the check above passed: sid==pgid==pid).
	# Clamp the inherited wall deadline UNCONDITIONALLY to at most a fresh now+WALL_CLOCK_MAX
	# -- NO spoofable marker gates this. The genuine child's inherited
	# outer deadline is (outer_start + WALL_CLOCK_MAX), and isolation took > 0 time, so it is
	# < now+WALL and is PRESERVED by the min() -- the child still honours the outer's absolute
	# ceiling. Any forged or huge value is bounded DOWN to the fresh ceiling, so a caller can never
	# EXTEND the bound (the "a clamp could extend the ceiling" note conflated a FRESH
	# now+WALL, which would extend, with this min(inherited, now+WALL), which never can). A malformed
	# value -- which a genuine waiter never sends -- falls back to the fresh bound rather than
	# reaching arithmetic on caller input.
	_fresh_deadline=$((EPOCHSECONDS + WALL_CLOCK_MAX))
	case "${_BLOAR_WALL_DEADLINE:-}" in
	'' | *[!0-9]* | ????????????*) _BLOAR_WALL_DEADLINE=$_fresh_deadline ;;
	*) [ "$_BLOAR_WALL_DEADLINE" -gt "$_fresh_deadline" ] && _BLOAR_WALL_DEADLINE=$_fresh_deadline ;;
	esac
	export _BLOAR_WALL_DEADLINE
	# Hand our VERIFIED identity to the waiter so it can forward signals to THIS group and no other
	#: write our OWN REAL "pid sid pgid" through the inherited transport fd (`>&$fd`,
	# never a path). But FIRST read back the per-run NONCE the waiter wrote into that inode: the fd's
	# type is NOT authenticity (a caller forges `9>file` -- review), so the MANDATORY nonce
	# read is what keeps this identity record OFF any fd the waiter did not prepare. A forged write-
	# only fd fails the read; a fresh readable fd yields no nonce line -> we do NOT write. We echo the
	# nonce as field 1 so the waiter confirms the responder read its inode, then CLOSE the fd.
	_hs_nonce="" _hs_tag=""
	if is_own_transport_fd "${_BLOAR_HS_FD:-}"; then
		read -r _hs_tag _hs_nonce <&"$_BLOAR_HS_FD" 2>/dev/null || _hs_nonce=""
	fi
	if [ "$_hs_tag" = NONCE ] && [ -n "$_hs_nonce" ]; then
		# BRIDGE trap. The instant we announce our group
		# the waiter may forward a signal, but main()->arm_watchdog does not install its own
		# TERM handler until a little later, so a forward landing in this handshake-to-arm
		# window would otherwise hit TERM's DEFAULT disposition and kill our shell (seen as a
		# 143 the waiter faithfully propagates). Bridge that window with a builtins-only trap
		# that exits the DOCUMENTED status: nothing external has run and no EXIT/cleanup trap
		# is installed yet, so there is nothing to tear down. arm_watchdog then replaces the
		# TERM handler with its own. (INT stays ignored on this async child; harmless.)
		trap 'exit 2' TERM INT
		read_pgid_sid "$$"
		printf '%s %s %s %s\n' "$_hs_nonce" "$$" "${REPLY_SID:-$$}" "${REPLY_PGID:-$$}" >&"$_BLOAR_HS_FD" 2>/dev/null || true
		exec {_BLOAR_HS_FD}>&- || : # close the transport fd right after the one-line reply (no stderr clobber)
	fi
	unset _BLOAR_HS_FD
	main "$@"
fi
