//go:build linux

package p2p

import (
	"errors"

	"golang.org/x/sys/unix"
)

// renameat2 is unix.Renameat2, indirected through a var so the regression test can
// drive the unsupported-syscall fallback below without a real old kernel.
var renameat2 = unix.Renameat2

// installNoReplace atomically installs temp as target, failing if target already
// exists. On Linux renameat2 with RENAME_NOREPLACE does this in
// one syscall: on success target names temp's contents and temp no longer exists;
// a pre-existing target is EEXIST, which the caller matches against fs.ErrExist and
// reads back the winner.
//
// Where renameat2 or the flag is unsupported it falls back to the portable
// link+unlink the plan promised: ENOSYS is a kernel
// without renameat2 (pre-3.15), and EINVAL is a filesystem that does not implement
// RENAME_NOREPLACE (the flag is the only one passed, so EINVAL here means the flag,
// not a bad argument). Every other errno -- EEXIST, EACCES, ENOSPC, EXDEV, ... --
// fails closed, because it is a real failure link+unlink would not paper over.
func installNoReplace(temp, target string) error {
	err := renameat2(unix.AT_FDCWD, temp, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
		return linkNoReplace(temp, target)
	}
	return err
}
