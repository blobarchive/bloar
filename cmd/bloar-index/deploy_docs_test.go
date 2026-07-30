package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestStaleEvidenceRegression runs the committed deploy/test-stale-evidence.sh,
// which sources the verifier's mutation classifiers and proves that a stale
// same-unit-name "manifest published" from a prior invocation cannot satisfy the
// correct/wrong/missing classifiers. Keeping it in the Go
// suite means the old false passes cannot return unnoticed.
func TestStaleEvidenceRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-stale-evidence.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("stale-evidence regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "STALE-EVIDENCE REGRESSION: PASS") {
		t.Fatalf("stale-evidence regression did not report PASS:\n%s", out)
	}
}

// TestHardBoundRegression runs the committed deploy/test-hard-bound.sh, which
// proves the verifier's watchdog SIGKILLs the whole process group even when a
// foreground child has deferred the main shell's TERM trap -- a run cannot hang
// past WALL_CLOCK_MAX + CLEANUP_MAX. It runs its victim
// under setsid, isolated from this process.
func TestHardBoundRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-hard-bound.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("hard-bound regression failed (%v):\n%s", err, out)
	}
	// PASS = the watchdog hard-terminated in time; SKIP = setsid unavailable here.
	if !strings.Contains(string(out), "HARD-BOUND REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("hard-bound regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestDurationGrammarRegression runs the committed deploy/test-duration-grammar.sh,
// which drives the REAL validate_durations (and the real executable entry) to prove the
// verifier's duration knobs use a strict positive-integer grammar: a loose
// `[ -gt 0 ]` accepts "1 "/" 1"/"007", which sleep rejects and which
// would silently disarm the watchdog.
func TestDurationGrammarRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-duration-grammar.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("duration-grammar regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "DURATION-GRAMMAR REGRESSION: PASS") {
		t.Fatalf("duration-grammar regression did not report PASS:\n%s", out)
	}
}

// TestPreflightHangRegression runs the committed deploy/test-preflight-hang.sh, which
// drives the NORMAL executable entry (guard -> setsid re-exec -> outer waiter -> main)
// with `id` mocked to hang at the root check, and proves the watchdog -- armed BEFORE any
// NSS lookup -- still bounds the whole lifecycle to WALL_CLOCK_MAX + CLEANUP_MAX.
// SKIP = setsid unavailable here.
func TestPreflightHangRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-preflight-hang.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("preflight-hang regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "PREFLIGHT-HANG REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("preflight-hang regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestCleanupBoundRegression runs the committed deploy/test-cleanup-bound.sh, which
// drives the NORMAL executable entry to a cleanup whose `systemctl stop` HANGS while
// ignoring TERM, and proves the run still dies within CLEANUP_MAX + margin because the
// absolute cleanup killer SIGKILLs the whole group -- the terminal deadline is not
// cancelled by cleanup. SKIP = setsid or an on-disk `true` unavailable here.
func TestCleanupBoundRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-cleanup-bound.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("cleanup-bound regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "CLEANUP-BOUND REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("cleanup-bound regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestBootstrapBoundRegression runs the committed deploy/test-bootstrap-bound.sh, which
// drives the REAL executable entry with `head` mocked to hang (ignoring TERM/INT) and
// proves nothing external runs before the watchdog is armed -- so the wedging mock cannot
// hang the bootstrap and the whole lifecycle still dies within WALL_CLOCK_MAX +
// CLEANUP_MAX. SKIP = setsid unavailable here.
func TestBootstrapBoundRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-bootstrap-bound.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("bootstrap-bound regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "BOOTSTRAP-BOUND REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("bootstrap-bound regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestSignalForwardingRegression runs the committed deploy/test-signal-forwarding.sh,
// which proves that BOTH a TERM and an INT delivered to the outer waiter terminate the
// detached run, leave no child in the verifier's process group, and return a defined
// status -- the waiter translates INT to TERM because the detached async child inherits
// SIGINT ignored. SKIP = setsid unavailable here.
func TestSignalForwardingRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-signal-forwarding.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("signal-forwarding regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "SIGNAL-FORWARDING REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("signal-forwarding regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestHandshakeRaceRegression runs the committed deploy/test-handshake-race.sh, which
// proves the outer waiter learns the detached run's process group from a race-free
// child->parent handshake (verified by handshake_ok), never by reading the child's pgid
// itself right after the fork -- so a delayed setsid + an early signal can never forward
// into the caller's group. SKIP = setsid unavailable here.
func TestHandshakeRaceRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-handshake-race.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("handshake-race regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "HANDSHAKE-RACE REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("handshake-race regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestOuterBoundaryRegression runs the committed deploy/test-outer-boundary.sh, which drives
// the real entry through a mocked setsid to prove the two things the hostile mkfifo/rm mocks
// cannot: a HUNG, TERM-ignoring setsid (the child-before-handshake path) self-terminates at the
// exact wall+cleanup deadline with status 2, caller canary alive and zero verifier/pacer
// descendants; and a SLOW setsid proves the inner child arms its watchdog to the REMAINING
// budget (graceful status 2), not a fresh full wall. SKIP = setsid unavailable
// here.
func TestOuterBoundaryRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-outer-boundary.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("outer-boundary regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "OUTER-BOUNDARY REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("outer-boundary regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestHostileProcRegression runs the committed deploy/test-hostile-proc.sh, which drives the real
// EXECUTABLE entry with a hostile PROC env -- both a FIFO stat (a pre-bound blocking read) and a
// fake regular stat spoofing sid==pgid==pid -- and proves the entry ignores it (self-terminates
// within wall+cleanup; never spoofed into skipping isolation), while the SOURCED parser path can
// still inject stat content below the entry boundary. SKIP = setsid unavailable
// here.
func TestHostileProcRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-hostile-proc.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("hostile-proc regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "HOSTILE-PROC REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("hostile-proc regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestCallerTrustRegression runs the committed deploy/test-caller-trust.sh, which proves the
// executable entry does not trust caller-controlled inputs: an arithmetic-injecting duration knob
// (BASH_SOURCE-subscript) is rejected before any $(( )); a huge inherited _BLOAR_WALL_DEADLINE is
// clamped to the entry's own wall+cleanup; and a hostile inherited _BLOAR_HS_HOLD without the
// waiter nonce is scrubbed rather than blocked on. SKIP = setsid unavailable
// here.
func TestCallerTrustRegression(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	script := filepath.Join("..", "..", "deploy", "test-caller-trust.sh")
	out, err := exec.Command("bash", script).CombinedOutput()
	if err != nil {
		t.Fatalf("caller-trust regression failed (%v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "CALLER-TRUST REGRESSION: PASS") && !strings.Contains(string(out), "SKIP") {
		t.Fatalf("caller-trust regression did not report PASS or SKIP:\n%s", out)
	}
}

// TestRunbookCommandsPinned structurally pins the publish-manifest runbook
// (docs/operations.md §7.5 and §7.6) to commands that actually run. It checks
// EACH command OCCURRENCE in the
// runnable code blocks, not the section as a whole: every publish-manifest command
// must itself carry the exact instance config path
// (/etc/bloar/index/chain-arbitrum-one.yaml) and a credential-delivery form
// (-token-file or a systemd LoadCredential wrapper), and every systemctl command
// touching the indexer must use the real template instance
// (bloar-index@chain-arbitrum-one). One correct occurrence elsewhere does not
// excuse a wrong one -- reverting any single §7.5/§7.6 command fails this test.
func TestRunbookCommandsPinned(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations.md"))
	if err != nil {
		t.Fatalf("reading operations.md: %v", err)
	}
	doc := string(data)

	for _, sec := range []string{"7.5", "7.6"} {
		body := docsSection(t, doc, sec)

		sawPublish := false
		for _, blk := range codeBlocks(body) {
			// Join shell line-continuations so each command is one logical line, then
			// check every command occurrence on its own.
			for _, cmd := range strings.Split(joinContinuations(blk), "\n") {
				cmd = strings.TrimSpace(cmd)
				if strings.HasPrefix(cmd, "#") {
					continue // a comment inside the code block, not a command
				}
				if strings.Contains(cmd, "publish-manifest") {
					sawPublish = true
					if !strings.Contains(cmd, "/etc/bloar/index/chain-arbitrum-one.yaml") {
						t.Errorf("§%s: publish-manifest command lacks the real instance config path: %q", sec, cmd)
					}
					if !strings.Contains(cmd, "-token-file") && !strings.Contains(cmd, "LoadCredential") {
						t.Errorf("§%s: publish-manifest command delivers no credential (-token-file or LoadCredential): %q", sec, cmd)
					}
				}
				if strings.Contains(cmd, "systemctl") && strings.Contains(cmd, "bloar-index") &&
					!strings.Contains(cmd, "bloar-index@chain-arbitrum-one") {
					t.Errorf("§%s: systemctl command uses a non-template indexer instance: %q", sec, cmd)
				}
			}
		}
		if !sawPublish {
			t.Errorf("§%s has no publish-manifest command block; the runbook regression cannot pin it", sec)
		}

		// The old wrong paths must not appear anywhere in the section (prose too).
		for _, bad := range []string{"/etc/bloar/arbitrum-one.yaml", "/etc/bloar/index/arbitrum-one.yaml"} {
			if strings.Contains(body, bad) {
				t.Errorf("§%s references the wrong config path %q (the instance file is chain-arbitrum-one.yaml)", sec, bad)
			}
		}
	}
}

// joinContinuations collapses shell `\`-newline line continuations so a
// multi-line command becomes one logical line.
func joinContinuations(s string) string {
	return regexp.MustCompile(`\\\n[ \t]*`).ReplaceAllString(s, " ")
}

// docsSection returns the body of "### <num> ..." up to the next "### " or "## ".
func docsSection(t *testing.T, doc, num string) string {
	t.Helper()
	head := "### " + num + " "
	i := strings.Index(doc, head)
	if i < 0 {
		t.Fatalf("operations.md has no section %q", head)
	}
	rest := doc[i+len(head):]
	end := len(rest)
	for _, marker := range []string{"\n### ", "\n## "} {
		if j := strings.Index(rest, marker); j >= 0 && j < end {
			end = j
		}
	}
	return rest[:end]
}

// codeBlocks returns the contents of every ``` fenced block in body.
func codeBlocks(body string) []string {
	parts := strings.Split(body, "```")
	var out []string
	for i := 1; i < len(parts); i += 2 { // odd segments are inside fences
		out = append(out, parts[i])
	}
	return out
}

// TestVerifierRoutesCurlThroughHelper pins the curl contract: the one curlx
// helper must enforce --max-time "$CURL_MAX", and
// no other line may invoke curl regardless of argument shape. It matches the
// command WORD (\bcurl\b), not a flag prefix, so a URL-first `curl "$url"` cannot
// slip past; the only exemptions are comments and the preflight tool-list `for`
// loop. Removing the timeout from curlx, or adding any raw curl, fails this test.
func TestVerifierRoutesCurlThroughHelper(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "verify-token-credentials.sh"))
	if err != nil {
		t.Fatalf("reading verify-token-credentials.sh: %v", err)
	}
	curlWord := regexp.MustCompile(`\bcurl\b`)
	toolLoop := regexp.MustCompile(`^\s*for\s+\w+\s+in\b`)
	sawHelper := false
	for i, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue // comment
		}
		code := line
		if idx := strings.Index(code, " #"); idx >= 0 {
			code = code[:idx] // drop trailing comment (the script's URLs carry no '#')
		}
		if toolLoop.MatchString(code) {
			continue // the `for c in … curl …` preflight tool list, not an invocation
		}
		if !curlWord.MatchString(code) {
			continue
		}
		if strings.Contains(code, "curlx()") {
			sawHelper = true
			if !strings.Contains(line, `--max-time "$CURL_MAX"`) {
				t.Errorf(`curlx() must enforce --max-time "$CURL_MAX"; got: %q`, strings.TrimSpace(line))
			}
			continue
		}
		t.Errorf("verify-token-credentials.sh:%d invokes curl outside the bounded curlx helper: %q", i+1, strings.TrimSpace(line))
	}
	if !sawHelper {
		t.Error("no curlx() helper definition found in verify-token-credentials.sh")
	}
}
