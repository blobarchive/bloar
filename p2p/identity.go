package p2p

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// identityFileMode is what a private key file may be. Nothing reads this back
// to enforce it -- an operator who chmods the file has said what they meant --
// but nothing this package creates is ever wider.
const identityFileMode = 0o600

// sidecarReservedPrefix is the hidden prefix every identity sidecar (the lock and
// the temps) begins with. It is reserved: a configured target base name may not
// start with it (see LoadOrCreateIdentity), so a real key file can never be, or lie
// inside, another target's derived lock or temp namespace.
const sidecarReservedPrefix = ".bloar-id-"

// LoadIdentity reads an existing identity without ever creating one. It is the
// fail-closed path for a retained authority: a missing mount or misspelled path
// must stop startup, not mint a new PeerID/IPNS name that a subsequent restart
// would mistake for the intended key.
//
// The same locked-inode, replacement detection, parsing, and read-only-parent
// handling as LoadOrCreateIdentity apply. The only semantic difference is that
// os.ErrNotExist is fatal and no sidecar, parent directory, or key is created.
func LoadIdentity(path string) (crypto.PrivKey, error) {
	if path == "" {
		return nil, errors.New("p2p: identity key file must not be empty")
	}
	base := filepath.Base(path)
	if strings.HasPrefix(base, sidecarReservedPrefix) {
		return nil, fmt.Errorf("p2p: identity key file %s uses the reserved %q sidecar prefix; rename it",
			path, sidecarReservedPrefix)
	}
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	for attempt := 0; ; attempt++ {
		key, stable, err := readLockedTarget(dir, path, false /* imported: tolerate a read-only mount */)
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("p2p: required identity key %s does not exist; refusing to mint a new authority", path)
		}
		if err != nil {
			return nil, err
		}
		if stable {
			return key, nil
		}
		if attempt >= maxTargetOpenAttempts {
			return nil, fmt.Errorf("p2p: identity key %s kept being replaced while locking it; giving up "+
				"after %d attempts", path, attempt+1)
		}
	}
}

// LoadOrCreateIdentity reads the ed25519 identity key at path, creating it on
// first use. The second result reports whether this call created it.
//
// # The format, and why it is this one
//
// Hex on one line: either a 32-byte seed or a full 64-byte ed25519 private key.
// That is publish.signing_key_file's format, deliberately. Spec 8.1 says the
// IPNS record MAY be signed by the same key as the publication document, and
// the cheapest way to make that offer real is for the two files to be the same
// format, so an operator who wants one key points both keys at one path and is
// done. A libp2p-marshalled protobuf key would have been the conventional
// choice and would have made that impossible.
//
// # Durable, race-safe creation
//
// The identity is what every multiaddr this node publishes names, and is the IPNS
// name itself, so an identity that changed across a restart would invalidate both.
// Reading and creation therefore have to survive a crash and a concurrent creator,
// and BOTH run under a per-target directory lock -- there is no lockless fast path:
//
//   - A crash-releasing flock serializes access (the OS drops it when the process
//     dies, so there is no stale-lock protocol to get wrong). Reading an EXISTING
//     target locks the target's OWN inode, creating no sidecar, so a key under a
//     read-only parent still works; it reads THROUGH the locked fd and confirms with
//     os.SameFile that the fd still names the file at the path, retrying if a
//     concurrent replace moved it. CREATING a missing target takes two locks: a
//     reserved sidecar lock in the parent serializes creators, and the creator holds
//     an inode lock on the temp -- which becomes the target at the rename -- through
//     publication and the parent fsync, so a reader that opens the just-installed
//     target blocks until its directory entry is durable rather than returning inside
//     that window.
//   - A missing target is created by writing a fresh seed to a same-directory temp,
//     fsyncing it, and installing it with a no-replace rename. Every path attempts a
//     parent-directory fsync before returning success -- the target's directory entry
//     is a durability fact separate from its bytes. The creation and race-winner
//     paths treat any fsync failure as fatal; the imported-target read tolerates ONLY
//     a read-only-mount error (EROFS), where the entry was made durable by whoever
//     provisioned the key and there is no pending entry of ours.
//   - A failed write leaves only this caller's own temp, removed by name; sibling
//     temps a crashed run abandoned are swept only under the sidecar lock once a
//     valid target is installed. An empty or truncated existing target is a
//     corrupt-state error, never silently used and never silently deleted.
//
// # Reserved sidecar files
//
// The creation lock and the temps live beside the target as hidden files in a
// per-target namespace: ".bloar-id-<h>.lock" and ".bloar-id-<h>.tmp-<unique>",
// where h is the full SHA-256 hex of the target's base name. The hash keeps the
// namespace safe: distinct fixed-length namespaces cannot be
// a prefix of one another, so the sweep -- which matches this target's ".tmp-" prefix
// literally -- takes a differently-named target's temp only if the two base names hash
// to the SAME namespace (that target's lock ends in ".lock", not the swept ".tmp-", so
// the sweep never matches a lock either way), and SHA-256's collision resistance (this
// is a hash, not a bijection) makes that shared namespace astronomically unlikely
// rather than impossible; and short derived names sidestep the ENAMETOOLONG a
// ".<base>.lock" hits on a filesystem-maximum base name. ".bloar-id-*" is reserved
// in code: a configured key file whose base name starts with it is refused, so a real
// key can never be, or begin, another key's lock or temp namespace.
func LoadOrCreateIdentity(path string) (crypto.PrivKey, bool, error) {
	if path == "" {
		return nil, false, errors.New("p2p: identity key file must not be empty")
	}
	base := filepath.Base(path)
	if strings.HasPrefix(base, sidecarReservedPrefix) {
		// Reserved in code, not only by convention: this
		// build stores each key's lock and temps under this prefix, so a real key
		// named that shape could BE another key's derived lock, or begin its temp
		// sweep prefix, and be swept out from under it. Refuse it up front.
		return nil, false, fmt.Errorf("p2p: identity key file %s uses the reserved %q sidecar prefix; this build keeps "+
			"each key's lock and temporaries under that prefix, so a key of that shape could collide with another key's "+
			"lock or temp namespace -- rename it", path, sidecarReservedPrefix)
	}
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}

	// The existing-target path creates NO sidecar: it locks the target's own inode,
	// so it works when the parent directory is read-only -- a key under /etc/bloar
	// with ProtectSystem=strict, where only /var/lib/bloar is writable (finding
	// the safety boundary follow-up). Only the missing-target creation path writes sidecars, and it
	// needs a writable parent anyway.
	//
	// Symlinks are followed and then identity-checked: os.Open
	// and os.Stat both resolve a symlink to its target, so we lock and read the target
	// inode, and readLockedTarget confirms the fd still names the file at path. If the
	// path stops naming the inode we locked -- a concurrent install or an operator
	// swap -- we retry rather than return a key read from the old inode.
	for attempt := 0; ; attempt++ {
		key, stable, err := readLockedTarget(dir, path, false /* imported: tolerate a read-only mount */)
		if errors.Is(err, os.ErrNotExist) {
			break // missing: fall through to creation
		}
		if err != nil {
			return nil, false, err
		}
		if stable {
			return key, false, nil
		}
		if attempt >= maxTargetOpenAttempts {
			return nil, false, fmt.Errorf("p2p: identity key %s kept being replaced while locking it; giving up "+
				"after %d attempts", path, attempt+1)
		}
	}

	return createIdentity(dir, path, base)
}

// maxTargetOpenAttempts bounds the existing-target retry: a target that keeps being
// replaced under the lock is either an operator swapping it in a tight loop or a
// bug, and either is better reported than spun on forever.
const maxTargetOpenAttempts = 8

// readLockedTarget opens path, flocks the target's own inode, reads and validates
// the key THROUGH that same fd -- never reopening by name -- and returns it once it
// has confirmed, both BEFORE and AFTER the read and parent fsync, that the fd still
// names the file at path. stable is false, signalling a
// retry, when path no longer names the locked inode: a concurrent install or swap
// left the fd on the old inode, and returning would hand back a key read from a now-
// unlinked file. A missing path is os.ErrNotExist, left for the caller to test. The
// fd is always closed, releasing the lock.
//
// It is the one locked-fd contract every returned-but-not-created target goes
// through -- the imported-key path AND the creation path's re-check and EEXIST
// race-winner (via adoptRacerInstall) -- so none reads a target locklessly. strictFsync
// selects the parent-fsync policy: the creation path has a pending entry of ours to
// make durable and treats every fsync error as fatal; the imported-key path tolerates
// only a read-only mount (see importedParentSyncTolerable), the target having been
// made durable by whatever provisioned it.
func readLockedTarget(dir, path string, strictFsync bool) (crypto.PrivKey, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, err // the caller decides: create, or a vanished winner
		}
		return nil, false, fmt.Errorf("p2p: opening identity key %s: %w", path, err)
	}
	defer f.Close()

	if beforeLock != nil {
		beforeLock()
	}
	if err := lockExclusive(f); err != nil {
		return nil, false, fmt.Errorf("p2p: locking identity key %s: %w", path, err)
	}
	if afterLockBeforeStat != nil {
		afterLockBeforeStat()
	}

	// Early identity check: a fast bail if the path was replaced before we locked. It
	// is only an optimization -- the authoritative check is the final one below.
	if same, err := sameInode(f, path); err != nil {
		return nil, false, err
	} else if !same {
		return nil, false, nil // retry
	}

	raw, err := io.ReadAll(f)
	if err != nil {
		return nil, false, fmt.Errorf("p2p: reading identity key %s: %w", path, err)
	}
	key, err := validateIdentityBytes(raw, path)
	if err != nil {
		return nil, false, err
	}

	if ferr := fsyncDir(dir); ferr != nil {
		if strictFsync || !importedParentSyncTolerable(ferr) {
			return nil, false, fmt.Errorf("p2p: syncing identity key directory %s: %w", dir, ferr)
		}
	}

	if afterReadBeforeFinalStat != nil {
		afterReadBeforeFinalStat()
	}

	// Final identity check, AFTER the read, parse, and dir fsync (the safety boundary round
	// 4): a rename that landed after the early check would otherwise hand back the key
	// read from the now-unlinked inode. Recheck before returning it.
	if same, err := sameInode(f, path); err != nil {
		return nil, false, err
	} else if !same {
		return nil, false, nil // retry
	}
	return key, true, nil
}

// sameInode reports whether f still names the file currently at path (the safety boundary
// follow-up). A path that no longer exists, or names a different inode, is a concurrent
// replace: the caller retries rather than trust a read of the old inode.
func sameInode(f *os.File, path string) (bool, error) {
	fi, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("p2p: statting locked identity key %s: %w", path, err)
	}
	pi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil // removed under the lock: retry
		}
		return false, fmt.Errorf("p2p: statting identity key %s: %w", path, err)
	}
	return os.SameFile(fi, pi), nil
}

// createIdentity handles the missing-target case: it serializes on the reserved
// hashed sidecar lock in the parent (which it must be able to write to, since it is
// about to install a key there) and creates the identity under it. A parent it
// cannot write the lock into is a location an identity cannot be created in, and the
// error says so.
func createIdentity(dir, path, base string) (crypto.PrivKey, bool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, false, fmt.Errorf("p2p: creating identity key directory %s: %w", dir, err)
	}
	lockPath := filepath.Join(dir, sidecarLockName(base))
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, identityFileMode)
	if err != nil {
		return nil, false, fmt.Errorf("p2p: identity key %s does not exist and cannot be created; its directory %s is "+
			"not writable: %w", path, dir, err)
	}
	// Closing the lock file releases the flock; the OS also releases it on process
	// death, which is what makes the lock crash-safe with no cleanup of its own.
	defer lockFile.Close()
	if err := lockExclusive(lockFile); err != nil {
		return nil, false, fmt.Errorf("p2p: locking identity directory %s: %w", dir, err)
	}
	return createLocked(dir, path, base)
}

// createLocked creates the identity with the sidecar lock held.
func createLocked(dir, path, base string) (crypto.PrivKey, bool, error) {
	if beforeRecheck != nil {
		beforeRecheck()
	}
	// Re-check under the sidecar lock through the locked-fd contract -- NOT a lockless
	// pathname read: a racer may have installed the target
	// between this call's missing-file open and the lock. adoptRacerInstall blocks on
	// the target's flock if a writer holds it mid-write, so a partially written target
	// is never read; adopted=false means the path is missing here, so no racer won and
	// we create it.
	if key, adopted, err := adoptRacerInstall(dir, path, base); err != nil {
		return nil, false, err
	} else if adopted {
		return key, false, nil
	}

	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, false, fmt.Errorf("p2p: generating identity key: %w", err)
	}
	encoded := append([]byte(hex.EncodeToString(seed)), '\n')

	// Our own uniquely-named temp, kept OPEN. It is tracked by exact path and, until
	// the target is installed, removed only by that exact path -- never swept by
	// prefix -- so a cleanup can never take a concurrent creator's temp (moot under
	// the sidecar lock, belt and braces).
	temp, tempFile, err := writeTempIdentity(dir, base, encoded)
	if err != nil {
		return nil, false, err
	}
	installed := false
	// Close the fd -- releasing the creator inode lock -- on EVERY exit path, so a
	// parent-fsync failure after the install does not return with the target still
	// exclusively flocked, blocking an in-process retry until the fd is finalized
	//. The temp is removed only when we did NOT install it.
	defer tempFile.Close()
	defer func() {
		if !installed {
			_ = os.Remove(temp)
		}
	}()

	// The creator's INODE lock: flock the temp fd, which
	// becomes the target inode at the rename below, and hold it through publication
	// AND the parent fsync. The sidecar lock serializes creators; this is what stops
	// an inode-locking reader -- one that opens the just-installed target -- from
	// returning inside the publish-to-fsync window, before its directory entry is
	// durable. Such a reader blocks on this lock until the fsync completes.
	if err := lockExclusive(tempFile); err != nil {
		return nil, false, fmt.Errorf("p2p: locking identity temp %s: %w", temp, err)
	}

	if installBarrier != nil {
		installBarrier(temp)
	}

	// Install our temp, retrying if a no-replace-install winner appears and then
	// vanishes. We hold our own temp's fd and inode lock
	// throughout, so on a losing race we adopt a durable winner, and if that winner
	// disappears before we can adopt it, path is free again and we re-install ours --
	// never a raw ENOENT.
	for attempt := 0; ; attempt++ {
		err := installNoReplace(temp, path)
		if err == nil {
			break // we installed it
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, false, fmt.Errorf("p2p: installing identity key %s: %w", path, err)
		}
		// A race winner installed the target first. The sidecar lock makes this
		// unreachable; the no-replace install refuses to clobber anyway. It goes
		// through the SAME locked-fd durability path as the re-check above -- this
		// branch exists precisely for a creator that did NOT serialize on our lock and
		// so may not have fsynced its own directory entry -- so we adopt it durably.
		if key, adopted, aerr := adoptRacerInstall(dir, path, base); aerr != nil {
			return nil, false, aerr
		} else if adopted {
			return key, false, nil
		}
		// The winner vanished before we could adopt it: path is missing again, so
		// re-install our own temp rather than returning a raw ENOENT.
		if attempt >= maxTargetOpenAttempts {
			return nil, false, fmt.Errorf("p2p: identity key %s: a racing install kept appearing and vanishing; "+
				"giving up after %d attempts", path, attempt+1)
		}
	}
	installed = true

	if afterPublishBeforeFsync != nil {
		afterPublishBeforeFsync()
	}

	// The target's bytes were fsynced before the install; the directory entry that
	// names them is a separate durability fact, and a power loss after a successful
	// return that lost it would mint a new PeerID/IPNS name next start (finding
	// the safety boundary). fsync the parent directory so the entry survives. Fatal on failure:
	// this is the creation path, the parent is writable, and there IS a pending entry
	// of ours to make durable.
	if err := fsyncDir(dir); err != nil {
		return nil, false, fmt.Errorf("p2p: syncing identity key directory %s: %w", dir, err)
	}

	// The entry is durable. The deferred close releases the inode lock and lets waiting
	// readers proceed; it runs after the sweep below, which is a brief extra hold.
	sweepStaleTemps(dir, base)

	key, err := parseIdentity(encoded, path)
	if err != nil {
		return nil, false, err
	}
	return key, true, nil
}

// adoptRacerInstall checks, through the locked-fd contract (readLockedTarget with the
// strict fsync policy), whether a durable target is installed at path -- by a racer
// that won the sidecar lock, or a no-replace-install winner that did not serialize on
// it. It returns adopted=true with the key (having made the entry durable and swept
// orphaned temps); adopted=false with a nil error when the path is currently MISSING,
// which the caller treats as a state transition -- the under-lock re-check proceeds to
// creation, an EEXIST loser re-installs its own temp; or a fatal error. It locks the
// winner's OWN inode and reads THROUGH the locked fd, so a winner is never read
// locklessly and a target another process is writing blocks it (never a partial read),
// and a target replaced under the lock is retried up to a bound (the safety boundary round
// 4, 5).
func adoptRacerInstall(dir, path, base string) (crypto.PrivKey, bool, error) {
	for attempt := 0; ; attempt++ {
		key, stable, err := readLockedTarget(dir, path, true /* strict fsync */)
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil // missing now: the caller creates or re-installs
		}
		if err != nil {
			return nil, false, err
		}
		if stable {
			sweepStaleTemps(dir, base)
			return key, true, nil
		}
		if attempt >= maxTargetOpenAttempts {
			return nil, false, fmt.Errorf("p2p: identity key %s kept being replaced while adopting a racing creator's "+
				"install; giving up after %d attempts", path, attempt+1)
		}
	}
}

// installBarrier, if set, is called between writing a creator's temp and installing
// it, with the temp's path. It exists only for the deterministic cleanup-vs-creator
// regression (identity_barrier_test.go), which holds one creator here while another
// runs a cleanup pass; production leaves it nil.
var installBarrier func(tempPath string)

// afterPublishBeforeFsync, if set, is called after a creator installs (publishes)
// the target but before it fsyncs the parent -- while it still holds the creator
// inode lock. It exists only for the regression that a reader cannot return inside
// that window (identity_barrier_test.go); production leaves it nil.
var afterPublishBeforeFsync func()

// beforeLock, if set, is called on a locked-fd read immediately BEFORE the blocking
// flock attempt. It exists only for the deterministic lock-barrier regressions'
// pre-lock handshake (identity_barrier_test.go): a reader signals it has reached the
// flock so the test can assert it does not return until the lock is released, with no
// timing sleep. Production leaves it nil.
var beforeLock func()

// afterLockBeforeStat, if set, is called on a locked-fd read after the inode lock is
// taken and before the EARLY SameFile check. It exists only for the regression that a
// concurrent replace before the early check triggers a retry
// (identity_existing_target_test.go); production leaves it nil.
var afterLockBeforeStat func()

// afterReadBeforeFinalStat, if set, is called on a locked-fd read after the read,
// parse, and dir fsync and before the FINAL SameFile check. It exists only for the
// regression that a replace landing after the early check is caught by the final
// check (identity_samefile_test.go); production leaves it nil.
var afterReadBeforeFinalStat func()

// beforeRecheck, if set, is called at the top of createLocked -- under the sidecar
// lock, before the under-lock re-check. It exists only for the regression that the
// re-check reads a racer's target through the locked-fd contract and blocks on a
// mid-write flock rather than reading partial bytes (identity_recheck_test.go);
// production leaves it nil.
var beforeRecheck func()

// afterSweep, if set, is called at the end of sweepStaleTemps. Test-only, like
// installBarrier: it lets the regression observe a cleanup pass completing.
var afterSweep func()

// writeTempIdentity writes b to a fresh, uniquely-named temp file in dir, fsyncs
// it, and returns its path AND the still-open fd. The caller takes the creator inode
// lock on that fd and holds it through the install and parent fsync (the safety boundary
// follow-up), so writeTempIdentity does not close it on success -- the caller does,
// after the entry is durable. The temp shares the target's parent so the later
// install is a rename within one directory, and carries the target's hashed sidecar
// prefix so a sweep can recognise it. On any failure it closes and removes its own
// temp and returns the error.
func writeTempIdentity(dir, base string, b []byte) (string, *os.File, error) {
	f, err := os.CreateTemp(dir, sidecarTempPrefix(base)+"*")
	if err != nil {
		return "", nil, fmt.Errorf("p2p: creating identity key temp in %s: %w", dir, err)
	}
	name := f.Name()
	if err := func() error {
		if err := f.Chmod(identityFileMode); err != nil {
			return err
		}
		if _, err := f.Write(b); err != nil {
			return err
		}
		return f.Sync()
	}(); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return "", nil, fmt.Errorf("p2p: writing identity key temp %s: %w", name, err)
	}
	return name, f, nil
}

// sidecarNamespace is the fixed-size, per-target token every sidecar file shares:
// the full SHA-256 hex of the target's base name. Two properties keep one target's
// sidecars out of another's sweep prefix, and both hold for DISTINCT base names under
// SHA-256 collision resistance: distinct base names hash to distinct tokens (barring an
// astronomically unlikely collision -- this is a hash, not a bijection), and because
// every token is the same length, two distinct tokens are prefix-free -- neither can be
// a prefix of the other. The hash also keeps every sidecar name short whatever the base
// name's length. The full digest is used rather than a
// truncation: truncating buys nothing here, since the names are already short.
func sidecarNamespace(base string) string {
	sum := sha256.Sum256([]byte(base))
	return hex.EncodeToString(sum[:])
}

// sidecarLockName is the flock file for the target named base, and sidecarTempPrefix
// the common prefix of its temps. Both are hidden and namespaced by sidecarNamespace,
// and the ".tmp-" / ".lock" suffixes keep a temp sweep from ever matching the lock.
func sidecarLockName(base string) string   { return ".bloar-id-" + sidecarNamespace(base) + ".lock" }
func sidecarTempPrefix(base string) string { return ".bloar-id-" + sidecarNamespace(base) + ".tmp-" }

// sweepStaleTemps removes temp files abandoned by a crashed creation. It is called
// only under the lock and only once a valid target is installed, so every temp it
// finds is orphaned -- no live creator can hold one, because a live creator holds
// the lock.
//
// Entries are matched with a literal string prefix over the target's hashed
// namespace, NOT filepath.Glob and NOT the raw base name. Glob would expand a base
// name's metacharacters (p2p*.key), and even a literal raw prefix is not prefix-free
// -- ".p2p.key.tmp-" is a prefix of a DIFFERENT valid target p2p.key.tmp-other's own
// temps and lock -- so either would take another target's live sidecar. The
// fixed-length hashed prefix has no such relation for
// distinct base names: they share it only on an astronomically unlikely SHA-256
// collision (a hash, not a bijection), never by one name extending another.
// Failures are ignored: a leftover temp is untidy, not unsafe, and the next
// successful creation sweeps it.
func sweepStaleTemps(dir, base string) {
	if afterSweep != nil {
		defer afterSweep()
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	prefix := sidecarTempPrefix(base)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), prefix) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// validateIdentityBytes parses key bytes read from the target. An empty or
// whitespace-only file is a corrupt-state error naming the file and the fix, never
// silently used or deleted; it is not confused with a missing file,
// which readLockedTarget reports as os.ErrNotExist -- its os.Open fails -- before this
// is reached.
func validateIdentityBytes(raw []byte, path string) (crypto.PrivKey, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, fmt.Errorf("p2p: identity key %s exists but is empty; a previous write did not complete. "+
			"Remove the file to mint a fresh identity, or restore it from backup -- this build will not overwrite or "+
			"delete it, because doing so could destroy a real key", path)
	}
	return parseIdentity(raw, path)
}

// parseIdentity decodes the file format LoadOrCreateIdentity documents.
func parseIdentity(raw []byte, path string) (crypto.PrivKey, error) {
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("p2p: identity key %s is not hex: %w", path, err)
	}
	switch len(b) {
	case ed25519.SeedSize:
		b = ed25519.NewKeyFromSeed(b)
	case ed25519.PrivateKeySize:
		// seed || public-key: re-derive the public half from the seed and require a
		// constant-time match, so an inconsistent expanded key is
		// refused rather than producing a PeerID/IPNS identity whose signatures do
		// not verify against the public key it reports. The 32-byte seed form has no
		// second half to disagree and is the preferred input.
		derived := ed25519.NewKeyFromSeed(b[:ed25519.SeedSize])
		if subtle.ConstantTimeCompare(derived[ed25519.SeedSize:], b[ed25519.SeedSize:]) != 1 {
			return nil, fmt.Errorf("p2p: identity key %s is a 64-byte expanded ed25519 key whose public half does not "+
				"derive from its seed; supply the 32-byte seed form, or a consistent expanded key", path)
		}
	default:
		return nil, fmt.Errorf("p2p: identity key %s decodes to %d bytes, want an ed25519 seed (%d) or private key (%d)",
			path, len(b), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
	key, err := crypto.UnmarshalEd25519PrivateKey(b)
	if err != nil {
		return nil, fmt.Errorf("p2p: identity key %s is not a valid ed25519 key: %w", path, err)
	}
	return key, nil
}

// fsyncFault, if set, is consulted before every directory fsync; a non-nil return
// makes fsyncDir fail with it. Test-only (nil in production): it lets the EEXIST
// durability regression prove the winner branch routes through fsyncDir by pinning
// that a fsync failure there propagates out of LoadOrCreateIdentity.
var fsyncFault func() error

// fsyncDir fsyncs a directory so a rename or create into it is durable: the entry
// is a separate durability fact from the file's own bytes.
func fsyncDir(dir string) error {
	if fsyncFault != nil {
		if err := fsyncFault(); err != nil {
			return err
		}
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
