package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/blobarchive/bloar/server"
)

const configTestArchiveID = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

func logicalArchiveConfig(archiveID, signingKey string, heads bool) string {
	headBlock := ""
	if heads {
		headBlock = "heads:\n  all: {}\n"
	}
	return fmt.Sprintf(`
net: mainnet
beacon:
  genesis_time: 1606824023
store:
  path: /var/lib/bloar
server:
  auth_token_file: /etc/bloar/token
publish:
  archive_id: %q
  signing_key_file: %q
%s`, archiveID, signingKey, headBlock)
}

func TestLogicalArchiveConfig(t *testing.T) {
	cfg := loadString(t, logicalArchiveConfig(configTestArchiveID, "/etc/bloar/ed25519.key", true))
	if cfg.Publish.ArchiveID != configTestArchiveID {
		t.Fatalf("publish.archive_id = %q", cfg.Publish.ArchiveID)
	}
	id, err := cfg.ArchiveID()
	if err != nil {
		t.Fatalf("ArchiveID: %v", err)
	}
	want, err := server.ParseArchiveID(configTestArchiveID)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil || *id != want {
		t.Fatalf("ArchiveID = %v, want %s", id, want)
	}

	empty, err := (&Config{}).ArchiveID()
	if err != nil || empty != nil {
		t.Fatalf("unset ArchiveID = (%v, %v), want (nil, nil)", empty, err)
	}

	for _, tc := range []struct {
		name       string
		archiveID  string
		signingKey string
		heads      bool
		want       string
	}{
		{name: "malformed", archiveID: "not-an-id", signingKey: "/key", heads: true, want: "publish.archive_id"},
		{name: "uppercase", archiveID: strings.ToUpper(configTestArchiveID), signingKey: "/key", heads: true, want: "lowercase"},
		{name: "unsigned", archiveID: configTestArchiveID, heads: true, want: "requires publish.signing_key_file"},
		{name: "follower only", archiveID: configTestArchiveID, signingKey: "/key", heads: false, want: "writes no heads"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, "config.yaml", logicalArchiveConfig(tc.archiveID, tc.signingKey, tc.heads)))
			if err == nil {
				t.Fatal("invalid logical archive configuration was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}
