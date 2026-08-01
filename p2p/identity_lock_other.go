//go:build !unix

package p2p

import (
	"errors"
	"os"
)

// lockExclusive is unimplemented off unix: the crash-releasing directory lock that
// serializes identity creation is an flock, which has no portable
// equivalent here. bloar is a unix daemon (flatfs, systemd), so this path is never
// taken in a supported deployment; it returns an error rather than silently
// creating an identity without the lock's race and durability guarantees.
func lockExclusive(*os.File) error {
	return errors.New("p2p: identity key locking is not supported on this platform; bloar is a unix daemon")
}

// importedParentSyncTolerable is false off unix: there is no read-only-mount
// carve-out to recognise here, and lockExclusive already refuses this platform, so a
// directory fsync error is propagated unchanged.
func importedParentSyncTolerable(error) bool { return false }
