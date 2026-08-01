package p2p_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/blobarchive/bloar/p2p"
)

func TestLoadIdentityNeverCreatesMissingAuthority(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "retained-ipns.key")
	if _, err := p2p.LoadIdentity(path); err == nil {
		t.Fatal("LoadIdentity minted a missing retained authority")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing key exists after LoadIdentity: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("LoadIdentity left creation sidecars behind: %v", entries)
	}
}

func TestLoadIdentityReturnsProvisionedAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained-ipns.key")
	created, wasCreated, err := p2p.LoadOrCreateIdentity(path)
	if err != nil || !wasCreated {
		t.Fatalf("provisioning identity = created %v, %v", wasCreated, err)
	}
	loaded, err := p2p.LoadIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	createdRaw, _ := crypto.MarshalPrivateKey(created)
	loadedRaw, _ := crypto.MarshalPrivateKey(loaded)
	if string(createdRaw) != string(loadedRaw) {
		t.Fatal("LoadIdentity returned a different authority")
	}
}
