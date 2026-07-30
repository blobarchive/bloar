package main

// follow-up pin for the safety boundary: the identity code's read-only-parent
// handling exists because the shipped deployment can put a key on a read-only
// filesystem. This pins that deployment shape, so a change to the unit or the docs
// that would invalidate the assumption is caught here rather than in the field.

import (
	"os"
	"strings"
	"testing"
)

func TestShippedUnitPinsReadOnlyKeyAssumptions(t *testing.T) {
	unit, err := os.ReadFile("../../deploy/systemd/bloard.service")
	if err != nil {
		t.Fatalf("reading the shipped unit: %v", err)
	}
	u := string(unit)

	// ProtectSystem=strict makes everything read-only but the explicit write paths;
	// the identity code's existing-target inode lock is what lets a key survive on
	// the read-only remainder.
	if !containsLine(u, "ProtectSystem=strict") {
		t.Error("bloard.service no longer sets ProtectSystem=strict; the identity read-only-parent handling assumes it")
	}
	// The write surface is narrow: /var/lib/bloar and nothing else. Any other
	// ReadWritePaths line means a key directory an operator thought was read-only is
	// writable, silently changing the path the identity code takes.
	var rwLines []string
	for _, line := range strings.Split(u, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "ReadWritePaths=") {
			rwLines = append(rwLines, line)
		}
	}
	if len(rwLines) != 1 || rwLines[0] != "ReadWritePaths=/var/lib/bloar" {
		t.Errorf("bloard.service ReadWritePaths is %v, want exactly [ReadWritePaths=/var/lib/bloar]", rwLines)
	}

	// The docs put a key under read-only /etc/bloar, document the read-only carve-out,
	// and allow one key for both the signing and identity jobs (the same-path case
	// that lands the identity key on a read-only filesystem).
	ops, err := os.ReadFile("../../docs/operations.md")
	if err != nil {
		t.Fatalf("reading operations.md: %v", err)
	}
	o := string(ops)
	for _, want := range []string{"/etc/bloar/ed25519.key", "Read-only key directories", "the same key"} {
		if !strings.Contains(o, want) {
			t.Errorf("operations.md no longer documents %q; the read-only key scenario the identity code handles is unpinned", want)
		}
	}
}

func containsLine(s, want string) bool {
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}
