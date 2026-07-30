#!/usr/bin/env bash
#
# Committed regression for the deploy-verifier hardening: the verifier's mutation
# classifiers must (a) require the EXACT structured signal of THIS run and (b) read
# the current run's evidence -- the process's own --pipe output for the one-shots,
# the current InvocationID's journal for the accepted mutation -- so a stale
# same-unit-name entry or a loose substring cannot resurrect the old
# correct/wrong/missing false passes. It sources the verifier's classifiers AND
# drives the real collection function run_oneshot_publish_manifest against mocked
# systemd-run/journal, so reverting collection back to a by-name journal read fails
# it. It needs neither root nor systemd (TestStaleEvidenceRegression runs it).
# Exit 0 = the false passes stay dead.

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

fails=0
ok()  { printf 'ok   %s\n' "$*"; }
bad() { printf 'BAD  %s\n' "$*"; fails=$((fails + 1)); }

# Exact structured signals of a CURRENT run, and stale/loose look-alikes.
CUR_401='bloar-index: archclient: POST /bloar/v1/heads/arbitrum-one/manifest: 401: authorization required'
CUR_CRED='bloar-index: archive.token_file "${CREDENTIALS_DIRECTORY}/token" begins with ${CREDENTIALS_DIRECTORY}, but that variable is unset: configure a plain path'
CUR_OK='time=2026-07-18T00:00:00Z level=INFO msg="manifest published" head=arbitrum-one tip=bafyCURRENT'
STALE_OK='time=2026-07-17T00:00:00Z level=INFO msg="manifest published" head=arbitrum-one tip=bafySTALE'

# Mocks: reproduce the stale journal AND provide the one-shot --pipe path.
MOCK="$(mktemp -d)"
cat >"$MOCK/systemd-run" <<'EOF'
#!/bin/bash
# systemd-run --pipe --wait: emit the process's OWN output, exit its status.
printf '%s\n' "${MOCK_PIPE_OUT:-}"
exit "${MOCK_PIPE_RC:-0}"
EOF
cat >"$MOCK/systemctl" <<'EOF'
#!/bin/bash
# systemctl show -p <PROP> --value <unit>: answer per property; exit MOCK_SCTL_RC so
# the wrapper-fidelity test can prove a nonzero manager status flows through.
prop=""
while [ $# -gt 0 ]; do case "$1" in -p) prop="${2:-}"; shift 2 ;; *) shift ;; esac; done
case "$prop" in
  ActiveState) printf '%s\n' "${MOCK_ACTIVESTATE:-active}" ;;
  *)           printf '%s\n' "${MOCK_INVOCATION:-CURRENTINV}" ;; # InvocationID and any other show
esac
exit "${MOCK_SCTL_RC:-0}"
EOF
cat >"$MOCK/journalctl" <<'EOF'
#!/bin/bash
# _SYSTEMD_INVOCATION_ID=<cur> -> this invocation; -u <name> -> by-name (stale). Exit
# MOCK_JCTL_RC so the fidelity test can prove a nonzero journal status flows through.
rc="${MOCK_JCTL_RC:-0}"
for a in "$@"; do case "$a" in
  _SYSTEMD_INVOCATION_ID=CURRENTINV) printf '%s\n' "${MOCK_CUR_JOURNAL:-}"; exit "$rc" ;;  # this invocation
  -u*)                               printf '%s\n' "${MOCK_STALE_JOURNAL:-}"; exit "$rc" ;; # by-name: stale
esac; done
exit "$rc"
EOF
chmod +x "$MOCK/systemd-run" "$MOCK/systemctl" "$MOCK/journalctl"
export PATH="$MOCK:$PATH"

# shellcheck source=/dev/null
source "$HERE/verify-token-credentials.sh"   # guarded main does not run
set +e

# --- (1) exact predicates + negative controls ---------------------------------
oneshot_is_wrong_401 1 "$CUR_401"                 && ok  "wrong: accepts the current 401"                  || bad "wrong: rejected the current 401"
oneshot_is_wrong_401 1 "$STALE_OK"                && bad "wrong: FALSE-PASS on a stale success"            || ok  "wrong: rejects a stale success"
oneshot_is_wrong_401 0 "$CUR_401"                 && bad "wrong: FALSE-PASS on exit 0"                     || ok  "wrong: rejects exit 0"
oneshot_is_wrong_401 1 'bloar-index: archclient: POST /bloar/v1/heads/arbitrum-one/manifest: 4010 unrelated' && bad "wrong: FALSE-PASS on 4010 (no status boundary)" || ok "wrong: rejects 4010 (status boundary enforced)"

oneshot_is_missing_cred 1 "$CUR_CRED"                                              && ok  "missing: accepts the current unset-variable error" || bad "missing: rejected the current cred error"
oneshot_is_missing_cred 1 "$STALE_OK"                                              && bad "missing: FALSE-PASS on a stale success"           || ok  "missing: rejects a stale success"
oneshot_is_missing_cred 1 'some failure that merely mentions CREDENTIALS_DIRECTORY' && bad "missing: FALSE-PASS on a loose mention"           || ok  "missing: rejects a loose CREDENTIALS_DIRECTORY mention"

journal_shows_published "$CUR_OK"                                             && ok  "correct: accepts the structured success event" || bad "correct: rejected the structured success event"
journal_shows_published 'ERROR: manifest published sentinel was not observed' && bad "correct: FALSE-PASS on a prose substring"      || ok  "correct: rejects a prose 'manifest published' substring"

# --- (2) InvocationID-bound read (accepted-mutation collection) ----------------
export MOCK_CUR_JOURNAL="$CUR_401" MOCK_STALE_JOURNAL="$STALE_OK"
cur="$(unit_journal any-recycled-name)"; set +e
journal_shows_published "$cur" && bad "correct: unit_journal leaked the stale success" || ok "correct: unit_journal shows only the current invocation"
journalctl -u any-recycled-name --no-pager | grep -qF 'msg="manifest published"' &&
	ok "correct: confirmed the by-name read surfaces the stale success (the attack the fix defeats)" ||
	bad "self-check: the mock by-name read did not produce the stale success"

# --- (3) drive the REAL collection path: run_oneshot must read --pipe ----------
# systemd-run emits THIS run's error; the by-name journal holds a STALE success.
# run_oneshot must classify from the --pipe output and pass; reverting its
# collection to a by-name journal read would classify the stale success -> FAILURES>0.
INDEX_BIN=/bin/true
export MOCK_STALE_JOURNAL="$STALE_OK"
export MOCK_PIPE_OUT="$CUR_401" MOCK_PIPE_RC=1
FAILURES=0; run_oneshot_publish_manifest wrong /dev/null "" >/dev/null 2>&1; set +e
[ "$FAILURES" -eq 0 ] && ok "wrong: run_oneshot classified from its --pipe output (stale journal ignored)" || bad "wrong: run_oneshot did not pass on the current --pipe failure (collection path reverted?)"
export MOCK_PIPE_OUT="$CUR_CRED" MOCK_PIPE_RC=1
FAILURES=0; run_oneshot_publish_manifest missing /dev/null "" >/dev/null 2>&1; set +e
[ "$FAILURES" -eq 0 ] && ok "missing: run_oneshot classified from its --pipe output (stale journal ignored)" || bad "missing: run_oneshot did not pass on the current --pipe failure (collection path reverted?)"

# --- (4) real verdict wiring is NEGATIVE-driven --------------------------------
# the regression case: push a near-miss through the REAL run_oneshot path and prove the
# verdict is wired to the EXACT classifier -- the 4010 look-alike through the wrong
# path, a loose CREDENTIALS_DIRECTORY mention through the missing path. Each must be
# REJECTED (FAILURES increments). Replacing the classifier calls in run_oneshot with a
# loose `[ $rc -ne 0 ]` would ACCEPT these (rc=1, FAILURES stays 0) -> this fails.
INDEX_BIN=/bin/true
export MOCK_STALE_JOURNAL="$STALE_OK"
export MOCK_PIPE_OUT='bloar-index: archclient: POST /bloar/v1/heads/arbitrum-one/manifest: 4010 unrelated' MOCK_PIPE_RC=1
FAILURES=0; run_oneshot_publish_manifest wrong /dev/null "" >/dev/null 2>&1; set +e
[ "$FAILURES" -eq 1 ] && ok "wrong: run_oneshot FAILS the 4010 near-miss through the real path (verdict wired to the exact classifier)" || bad "wrong: run_oneshot ACCEPTED the 4010 near-miss (verdict loosened to a bare rc check?)"
export MOCK_PIPE_OUT='some failure that merely mentions CREDENTIALS_DIRECTORY' MOCK_PIPE_RC=1
FAILURES=0; run_oneshot_publish_manifest missing /dev/null "" >/dev/null 2>&1; set +e
[ "$FAILURES" -eq 1 ] && ok "missing: run_oneshot FAILS the loose-mention near-miss through the real path" || bad "missing: run_oneshot ACCEPTED the loose CREDENTIALS_DIRECTORY mention (verdict loosened?)"

# --- (5) held-success collection is InvocationID-bound -------------------------
# the regression case: drive the REAL poll_held_success with a STALE by-name success and a
# current-invocation NON-success. It must NOT report "ok" (the success is not on THIS
# invocation). Reverting unit_journal to a `journalctl -u <name>` read would surface the
# stale success -> "ok" -> this fails. HELD_POLL_TRIES=1 keeps it instant; ActiveState
# =failed means the unit has already exited.
export MOCK_CUR_JOURNAL="$CUR_401"    # this invocation did NOT publish
export MOCK_STALE_JOURNAL="$STALE_OK" # a prior run under the recycled name did
export MOCK_ACTIVESTATE=failed
export HELD_POLL_TRIES=1
held="$(poll_held_success some-recycled-unit)"; set +e
[ "$held" != "ok" ] && ok "held: poll_held_success ignores a stale by-name success (got '${held:-<timeout>}')" || bad "held: poll_held_success FALSE-PASSED on a stale by-name success (collection reverted to -u?)"
export MOCK_CUR_JOURNAL="$CUR_OK"     # now THIS invocation published
held="$(poll_held_success some-recycled-unit)"; set +e
[ "$held" = "ok" ] && ok "held: poll_held_success reports ok when the current invocation published" || bad "held: poll_held_success missed a current-invocation success"
unset MOCK_ACTIVESTATE HELD_POLL_TRIES

# --- (6a) wrapper fidelity: preserve captured output AND nonzero status ---------
# the regression case + the regression case: if a wrapper dropped the exit code or the
# captured bytes, every classifier above would read the wrong thing. Each wrapper must
# pass BOTH the child's output and its NONZERO status through. Adding `|| true` to any
# wrapper zeroes the status -> the matching check fails.
export MOCK_PIPE_OUT='wrapper-fidelity' MOCK_PIPE_RC=7
wout="$(sdrun --pipe --wait -- x 2>&1)"; wrc=$?; set +e
{ [ "$wout" = "wrapper-fidelity" ] && [ "$wrc" -eq 7 ]; } && ok "sdrun preserves the child's captured output AND exit status 7" || bad "sdrun ate the child's output/status (out='$wout' rc=$wrc)"
export MOCK_CUR_JOURNAL='journal-fidelity' MOCK_JCTL_RC=6
jout="$(jctl _SYSTEMD_INVOCATION_ID=CURRENTINV --no-pager)"; jrc=$?; set +e
{ [ "$jout" = 'journal-fidelity' ] && [ "$jrc" -eq 6 ]; } && ok "jctl preserves the journal output AND nonzero status 6" || bad "jctl ate the journal output/status (out='$jout' rc=$jrc)"
export MOCK_INVOCATION='sctl-fidelity' MOCK_SCTL_RC=5
sout="$(sctl show -p X --value y)"; src=$?; set +e
{ [ "$sout" = 'sctl-fidelity' ] && [ "$src" -eq 5 ]; } && ok "sctl preserves the manager output AND nonzero status 5" || bad "sctl ate the manager output/status (out='$sout' rc=$src)"
unset MOCK_JCTL_RC MOCK_SCTL_RC MOCK_INVOCATION

# --- (6b) isolation predicate: arm only an OWN isolated session ----------------
# the regression case + the regression case: is_isolated_session_leader is what arm_watchdog
# consults before it will group-kill. It must require BOTH pgid==pid AND sid==pid, read
# from the real process state -- now via /proc/<pid>/stat with builtins (no ps), so we
# drive it with fake stat files under $PROC. Dropping the SID check reintroduces the
# sid!=pid false pass; a comm containing spaces/')' must not fool the after-last-')'
# field parse.
FAKEPROC="$(mktemp -d)"
mkstat() { mkdir -p "$FAKEPROC/$1"; printf '%s (%s) S 1 %s %s 0 -1 0 0 0\n' "$1" "$4" "$2" "$3" >"$FAKEPROC/$1/stat"; } # pid pgid sid comm
export PROC="$FAKEPROC"
mkstat 4242 4242 4242 bash
is_isolated_session_leader 4242 && ok "isolation: accepts an isolated leader (pgid==sid==pid)" || bad "isolation: rejected a valid isolated leader"
mkstat 4242 9999 4242 bash
is_isolated_session_leader 4242 && bad "isolation: FALSE-PASS with pgid!=pid" || ok "isolation: rejects pgid!=pid"
mkstat 4242 4242 9999 bash
is_isolated_session_leader 4242 && bad "isolation: FALSE-PASS with sid!=pid (SID assertion missing?)" || ok "isolation: rejects sid!=pid (session-leadership asserted)"
mkstat 4243 4243 4243 'weird ) name' # comm with a space AND a ')'
is_isolated_session_leader 4243 && ok "isolation: parses a comm containing spaces and ')' (fields after the last ')')" || bad "isolation: mis-parsed a comm containing ')'"
unset PROC
rm -rf "$FAKEPROC"

rm -rf "$MOCK"
if [ "$fails" -eq 0 ]; then
	echo "STALE-EVIDENCE REGRESSION: PASS"
	exit 0
fi
echo "STALE-EVIDENCE REGRESSION: $fails FAILED"
exit 1
