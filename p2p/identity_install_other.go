//go:build !linux

package p2p

// installNoReplace atomically installs temp as target, failing if target already
// exists. Where renameat2(RENAME_NOREPLACE) is unavailable, the
// portable link+unlink gives the same no-replace semantics.
func installNoReplace(temp, target string) error {
	return linkNoReplace(temp, target)
}
