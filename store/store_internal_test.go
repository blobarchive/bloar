package store

import (
	"reflect"
	"testing"
)

// TestSyncWritesEnabled pins the durability requirement in spec 6. flatfs
// exports no accessor for its sync flag, and a store that silently stopped
// syncing would pass every other test here, so this reads the flag directly.
// If flatfs renames the field, this fails loudly, which is the point.
func TestSyncWritesEnabled(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	f := reflect.ValueOf(s.ds).Elem().FieldByName("sync")
	if !f.IsValid() {
		t.Fatal("flatfs.Datastore has no 'sync' field: flatfs changed, re-verify that Open still enables sync writes")
	}
	if f.Kind() != reflect.Bool {
		t.Fatalf("flatfs.Datastore 'sync' is a %s, want bool: flatfs changed, re-verify that Open still enables sync writes", f.Kind())
	}
	if !f.Bool() {
		t.Error("flatfs sync writes are disabled, spec 6 requires them enabled")
	}
}

func TestShardMismatchErrorMessage(t *testing.T) {
	err := &ShardMismatchError{
		Path: "/var/lib/bloar/blocks",
		Want: ShardFunc,
		Got:  "/repo/flatfs/shard/v1/next-to-last/2",
	}
	want := `store: /var/lib/bloar/blocks was created with shard function "/repo/flatfs/shard/v1/next-to-last/2", ` +
		`this build requires "/repo/flatfs/shard/v1/next-to-last/3"; refusing to open`
	if got := err.Error(); got != want {
		t.Errorf("Error():\n got %s\nwant %s", got, want)
	}
}
