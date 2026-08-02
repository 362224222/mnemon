package agencycli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	journalDirectoryName = "agency-client"
	journalLockName      = "lock"
	journalActiveName    = "session.json"
	journalStageName     = "journal.stage"
	journalTerminalHead  = "terminal-"
	journalTerminalTail  = ".json"
	ownerDirectoryMode   = os.FileMode(0o700)
	ownerFileMode        = os.FileMode(0o600)
)

var errJournalAbsent = errors.New("R7 client journal is absent")

type journalStore struct {
	nodeState string
	random    io.Reader
}

type lockedJournalDirectory struct {
	path     string
	dir      *os.File
	lock     *os.File
	ownerUID uint32
}

func newJournalStore(nodeState string, random io.Reader) *journalStore {
	return &journalStore{nodeState: nodeState, random: random}
}

func (store *journalStore) exists() (bool, error) {
	if store == nil {
		return false, errors.New("R7 client journal store is unavailable")
	}
	var exists bool
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if errors.Is(err, errJournalAbsent) {
			return nil
		}
		if err != nil {
			return err
		}
		defer journal.clear()
		exists = true
		return nil
	})
	if errors.Is(err, errJournalAbsent) {
		return false, nil
	}
	return exists, err
}

// withLock pins the owner-only Node and journal directories, then serializes
// the complete private-client transition. Command callbacks may make one
// bounded local Unix-socket call while holding the lock; that is intentional:
// issuing two attachments or publishing a candidate into a replaced session
// would otherwise create two local sources of replay authority.
func (store *journalStore) withLock(create bool,
	callback func(*lockedJournalDirectory) error,
) error {
	if store == nil || store.random == nil || callback == nil {
		return errors.New("R7 client journal store is unavailable")
	}
	release := acquireProcessJournalLock(store.nodeState)
	defer release()
	directory, err := openJournalDirectory(store.nodeState, create)
	if err != nil {
		return err
	}
	defer directory.close()
	if err := unix.Flock(int(directory.lock.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock R7 client journal: %w", err)
	}
	defer unix.Flock(int(directory.lock.Fd()), unix.LOCK_UN) //nolint:errcheck // close also releases it.
	if err := directory.validate(); err != nil {
		return err
	}
	if err := directory.recoverStage(); err != nil {
		return err
	}
	return callback(directory)
}

func openJournalDirectory(nodeState string, create bool) (*lockedJournalDirectory, error) {
	if nodeState == "" || !filepath.IsAbs(nodeState) || filepath.Clean(nodeState) != nodeState {
		return nil, errors.New("R7 Node state path is not absolute and canonical")
	}
	nodeInfo, err := os.Lstat(nodeState)
	if err != nil {
		return nil, fmt.Errorf("inspect R7 Node state: %w", err)
	}
	ownerUID, err := validateOwnerInfo(nodeInfo, ownerDirectoryMode, true)
	if err != nil {
		return nil, fmt.Errorf("inspect R7 Node state: %w", err)
	}
	nodeFD, err := unix.Open(nodeState,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, fmt.Errorf("open R7 Node state: %w", err)
	}
	node := os.NewFile(uintptr(nodeFD), nodeState)
	if node == nil {
		_ = unix.Close(nodeFD)
		return nil, errors.New("open R7 Node state returned no directory")
	}
	defer node.Close()
	if err := validateOpenIdentity(nodeInfo, node, ownerDirectoryMode, true, ownerUID); err != nil {
		return nil, err
	}
	if err := ensureJournalDirectoryEntry(node, ownerUID, create); err != nil {
		return nil, err
	}
	dirFD, err := unix.Openat(int(node.Fd()), journalDirectoryName,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if errors.Is(err, syscall.ENOENT) && !create {
		return nil, errJournalAbsent
	}
	if err != nil {
		return nil, fmt.Errorf("open R7 client journal directory: %w", err)
	}
	dirPath := filepath.Join(nodeState, journalDirectoryName)
	dir := os.NewFile(uintptr(dirFD), dirPath)
	if dir == nil {
		_ = unix.Close(dirFD)
		return nil, errors.New("open R7 client journal returned no directory")
	}
	fail := func(err error) (*lockedJournalDirectory, error) {
		_ = dir.Close()
		return nil, err
	}
	var entry unix.Stat_t
	if err := unix.Fstatat(int(node.Fd()), journalDirectoryName, &entry,
		unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fail(fmt.Errorf("inspect R7 client journal directory: %w", err))
	}
	if err := validateUnixOwner(&entry, ownerDirectoryMode, true, ownerUID); err != nil {
		return fail(err)
	}
	opened, err := dir.Stat()
	if err != nil || !sameUnixIdentity(opened.Sys(), &entry) {
		if err == nil {
			err = errors.New("R7 client journal directory identity changed")
		}
		return fail(err)
	}
	lock, err := openJournalLock(dir, ownerUID)
	if err != nil {
		return fail(err)
	}
	return &lockedJournalDirectory{path: dirPath, dir: dir, lock: lock, ownerUID: ownerUID}, nil
}

func ensureJournalDirectoryEntry(node *os.File, ownerUID uint32, create bool) error {
	var entry unix.Stat_t
	err := unix.Fstatat(int(node.Fd()), journalDirectoryName, &entry, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, syscall.ENOENT) && !create {
		return errJournalAbsent
	}
	if errors.Is(err, syscall.ENOENT) {
		if err := unix.Mkdirat(int(node.Fd()), journalDirectoryName, uint32(ownerDirectoryMode)); err != nil {
			return fmt.Errorf("create R7 client journal directory: %w", err)
		}
		if err := node.Sync(); err != nil {
			return fmt.Errorf("persist R7 client journal directory: %w", err)
		}
		err = unix.Fstatat(int(node.Fd()), journalDirectoryName, &entry, unix.AT_SYMLINK_NOFOLLOW)
	}
	if err != nil {
		return fmt.Errorf("inspect R7 client journal directory: %w", err)
	}
	return validateUnixOwner(&entry, ownerDirectoryMode, true, ownerUID)
}

func openJournalLock(dir *os.File, ownerUID uint32) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), journalLockName,
		unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_CREAT, uint32(ownerFileMode))
	if err != nil {
		return nil, fmt.Errorf("open R7 client journal lock: %w", err)
	}
	lock := os.NewFile(uintptr(fd), journalLockName)
	if lock == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open R7 client journal lock returned no file")
	}
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	if _, err := validateOwnerInfo(info, ownerFileMode, false); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("inspect R7 client journal lock: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != ownerUID || stat.Nlink != 1 {
		_ = lock.Close()
		return nil, errors.New("R7 client journal lock identity is unsafe")
	}
	return lock, nil
}

func (directory *lockedJournalDirectory) validate() error {
	if directory == nil || directory.dir == nil || directory.lock == nil {
		return errors.New("R7 client journal directory is unavailable")
	}
	for _, file := range []struct {
		value *os.File
		mode  os.FileMode
		dir   bool
	}{{directory.dir, ownerDirectoryMode, true}, {directory.lock, ownerFileMode, false}} {
		info, err := file.value.Stat()
		if err != nil {
			return err
		}
		uid, err := validateOwnerInfo(info, file.mode, file.dir)
		if err != nil || uid != directory.ownerUID {
			if err == nil {
				err = errors.New("R7 client journal ownership changed")
			}
			return err
		}
	}
	return nil
}

func (directory *lockedJournalDirectory) close() {
	if directory == nil {
		return
	}
	if directory.lock != nil {
		_ = directory.lock.Close()
	}
	if directory.dir != nil {
		_ = directory.dir.Close()
	}
}

func (directory *lockedJournalDirectory) recoverStage() error {
	var stage unix.Stat_t
	err := unix.Fstatat(int(directory.dir.Fd()), journalStageName, &stage,
		unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect R7 client journal stage: %w", err)
	}
	if err := validateUnixOwner(&stage, ownerFileMode, false, directory.ownerUID); err != nil || stage.Nlink != 1 {
		return errors.New("R7 client journal stage is unsafe")
	}
	if err := unix.Unlinkat(int(directory.dir.Fd()), journalStageName, 0); err != nil {
		return fmt.Errorf("remove interrupted R7 client journal stage: %w", err)
	}
	return directory.dir.Sync()
}

func validateOpenIdentity(before os.FileInfo, opened *os.File, mode os.FileMode,
	directory bool, ownerUID uint32,
) error {
	after, err := opened.Stat()
	if err != nil || !os.SameFile(before, after) {
		if err == nil {
			err = errors.New("owner path identity changed while opening")
		}
		return err
	}
	uid, err := validateOwnerInfo(after, mode, directory)
	if err != nil || uid != ownerUID {
		if err == nil {
			err = errors.New("owner path ownership changed while opening")
		}
		return err
	}
	return nil
}

func validateOwnerInfo(info os.FileInfo, mode os.FileMode, directory bool) (uint32, error) {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != mode {
		return 0, errors.New("owner path mode or type is unsafe")
	}
	if directory && !info.IsDir() || !directory && !info.Mode().IsRegular() {
		return 0, errors.New("owner path has the wrong type")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return 0, errors.New("owner path has the wrong owner")
	}
	return stat.Uid, nil
}

func validateUnixOwner(stat *unix.Stat_t, mode os.FileMode, directory bool,
	ownerUID uint32,
) error {
	if stat == nil || stat.Uid != ownerUID || os.FileMode(stat.Mode).Perm() != mode {
		return errors.New("R7 client journal entry has unsafe ownership or mode")
	}
	fileType := stat.Mode & unix.S_IFMT
	if directory && fileType != unix.S_IFDIR || !directory && fileType != unix.S_IFREG {
		return errors.New("R7 client journal entry has the wrong type")
	}
	return nil
}

func sameUnixIdentity(info any, stat *unix.Stat_t) bool {
	opened, ok := info.(*syscall.Stat_t)
	return ok && stat != nil && uint64(opened.Dev) == uint64(stat.Dev) &&
		uint64(opened.Ino) == uint64(stat.Ino)
}
