//go:build unix

package p2p

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// lockExclusive takes an exclusive advisory lock on f and blocks until it is held
// . The lock is released when f is closed and, crucially, when the
// process dies -- flock is owned by the open file description, and the kernel drops
// it on exit -- so there is no stale-lock file to detect or reclaim after a crash.
func lockExclusive(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX)
}

// importedParentSyncTolerable reports whether a directory-fsync error on the
// imported-target read path is the one tolerated carve-out: EROFS, a genuinely
// read-only mount. The target was installed and made
// durable by whatever provisioned it, and there is no pending entry of ours to sync,
// so a read-only mount is the documented case where the read may still return. The
// carve-out is deliberately narrow: EACCES, EPERM, EIO, and any unsupported result
// are NOT tolerated -- they mean a real fsync failure or a directory this build
// cannot reason about, and are fatal. The creation and race-winner paths have their
// OWN pending entry to make durable and tolerate nothing.
func importedParentSyncTolerable(err error) bool {
	return errors.Is(err, unix.EROFS)
}
