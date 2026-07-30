package p2p

import "os"

// linkNoReplace installs temp as target with a hard link, failing (fs.ErrExist) if
// target already exists, then unlinks temp. It is the portable no-replace install
// : link refuses to clobber an existing target -- so a race winner
// is never overwritten -- and target and temp name one already-fsynced inode until
// the unlink. A failed unlink is non-fatal: target already names the durable bytes,
// and a later sweep reclaims the temp. It is the fallback the Linux path takes when
// renameat2(RENAME_NOREPLACE) is unsupported, and the whole install elsewhere.
func linkNoReplace(temp, target string) error {
	if err := os.Link(temp, target); err != nil {
		return err
	}
	_ = os.Remove(temp)
	return nil
}
