package authority

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type privateFileSnapshot struct {
	info   os.FileInfo
	exists bool
}

type authorityPathPlan struct {
	databasePath string
	directory    string
	directoryID  os.FileInfo
	files        map[string]privateFileSnapshot
}

func prepareAuthorityPath(databasePath string) (*authorityPathPlan, error) {
	return prepareAuthorityPathMode(databasePath, true)
}

// prepareExistingAuthorityPath validates an already provisioned authority
// path without creating its directory, database, or writer guard. Daemon
// startup uses this stricter boundary so a missing R7 authority can never be
// mistaken for first-run setup.
func prepareExistingAuthorityPath(databasePath string) (*authorityPathPlan, error) {
	return prepareAuthorityPathMode(databasePath, false)
}

func prepareAuthorityPathMode(databasePath string, initialize bool) (*authorityPathPlan, error) {
	if strings.TrimSpace(databasePath) == "" || !filepath.IsAbs(databasePath) ||
		filepath.Clean(databasePath) != databasePath {
		return nil, errors.New("open authority store: database path must be absolute and clean")
	}
	directory := filepath.Dir(databasePath)
	if directory == databasePath || filepath.Base(databasePath) == string(filepath.Separator) {
		return nil, errors.New("open authority store: database path must name a file")
	}
	var directoryID os.FileInfo
	var err error
	if initialize {
		directoryID, err = ensurePrivateDirectory(directory)
	} else {
		directoryID, err = inspectExistingPrivateDirectory(directory)
	}
	if err != nil {
		return nil, err
	}
	plan := &authorityPathPlan{databasePath: databasePath, directory: directory,
		directoryID: directoryID, files: make(map[string]privateFileSnapshot, 4)}
	for _, path := range authorityFilePaths(databasePath) {
		snapshot, err := inspectPrivateFile(path)
		if err != nil {
			return nil, err
		}
		plan.files[path] = snapshot
	}
	if !plan.files[databasePath].exists &&
		(plan.files[databasePath+"-wal"].exists || plan.files[databasePath+"-shm"].exists) {
		return nil, errors.New("open authority store: SQLite sidecar exists without database")
	}
	if !initialize && (!plan.files[databasePath].exists ||
		!plan.files[databasePath+".writer.lock"].exists) {
		return nil, errors.New("open existing authority store: database or writer guard is missing")
	}
	return plan, nil
}

func inspectExistingPrivateDirectory(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("open existing authority store: inspect state directory: %w", err)
	}
	if err := validatePrivateDirectoryInfo(info); err != nil {
		return nil, err
	}
	return info, nil
}

func ensurePrivateDirectory(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		parent := filepath.Dir(path)
		parentInfo, parentErr := os.Lstat(parent)
		if parentErr != nil || parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
			return nil, errors.New("open authority store: parent of state directory is not a real directory")
		}
		if err := os.Mkdir(path, privateDirectoryMode); err != nil {
			return nil, fmt.Errorf("open authority store: create dedicated state directory: %w", err)
		}
		if err := os.Chmod(path, privateDirectoryMode); err != nil {
			return nil, fmt.Errorf("open authority store: protect new state directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return nil, fmt.Errorf("open authority store: inspect state directory: %w", err)
	}
	if err := validatePrivateDirectoryInfo(info); err != nil {
		return nil, err
	}
	return info, nil
}

func validatePrivateDirectoryInfo(info os.FileInfo) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("open authority store: state directory is not a real directory")
	}
	if info.Mode().Perm() != privateDirectoryMode {
		return fmt.Errorf("open authority store: state directory mode is %04o, want %04o",
			info.Mode().Perm(), privateDirectoryMode)
	}
	if err := validateCurrentOwner(info); err != nil {
		return fmt.Errorf("open authority store: state directory: %w", err)
	}
	return nil
}

func inspectPrivateFile(path string) (privateFileSnapshot, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return privateFileSnapshot{}, nil
	}
	if err != nil {
		return privateFileSnapshot{}, fmt.Errorf("open authority store: inspect %s: %w",
			filepath.Base(path), err)
	}
	if err := validatePrivateFileInfo(info, filepath.Base(path)); err != nil {
		return privateFileSnapshot{}, err
	}
	return privateFileSnapshot{info: info, exists: true}, nil
}

func validatePrivateFileInfo(info os.FileInfo, name string) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("open authority store: %s is not a regular file", name)
	}
	if info.Mode().Perm() != privateFileMode {
		return fmt.Errorf("open authority store: %s mode is %04o, want %04o",
			name, info.Mode().Perm(), privateFileMode)
	}
	if err := validateCurrentOwner(info); err != nil {
		return fmt.Errorf("open authority store: %s: %w", name, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return fmt.Errorf("open authority store: %s must have exactly one filesystem link", name)
	}
	return nil
}

func validateCurrentOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("filesystem owner is unavailable")
	}
	if uint32(stat.Uid) != uint32(os.Geteuid()) {
		return errors.New("path is not owned by the current effective user")
	}
	return nil
}

func (plan *authorityPathPlan) acquireWriterLock() (*os.File, error) {
	path := plan.databasePath + ".writer.lock"
	expected := plan.files[path]
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if expected.exists {
		fd, err := unix.Open(path, flags, uint32(privateFileMode))
		if err != nil {
			return nil, fmt.Errorf("open authority store: open writer guard: %w", err)
		}
		return plan.finishWriterLock(os.NewFile(uintptr(fd), path), expected, false)
	}
	fd, err := unix.Open(path, flags|unix.O_CREAT|unix.O_EXCL, uint32(privateFileMode))
	if err != nil {
		return nil, fmt.Errorf("open authority store: create writer guard: %w", err)
	}
	return plan.finishWriterLock(os.NewFile(uintptr(fd), path), expected, true)
}

func (plan *authorityPathPlan) finishWriterLock(file *os.File, expected privateFileSnapshot,
	created bool,
) (*os.File, error) {
	if file == nil {
		return nil, errors.New("open authority store: writer guard returned no file")
	}
	if created {
		if err := file.Chmod(privateFileMode); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("open authority store: protect new writer guard: %w", err)
		}
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open authority store: inspect opened writer guard: %w", err)
	}
	if err := validatePrivateFileInfo(opened, filepath.Base(file.Name())); err != nil {
		_ = file.Close()
		return nil, err
	}
	if expected.exists && !os.SameFile(expected.info, opened) {
		_ = file.Close()
		return nil, errors.New("open authority store: writer guard identity changed while opening")
	}
	if err := verifyPrivateFileIdentity(file.Name(), opened); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrWriterActive
		}
		return nil, fmt.Errorf("open authority store: lock writer guard: %w", err)
	}
	if err := verifyPrivateFileIdentity(file.Name(), opened); err != nil {
		_ = releaseWriterLock(file)
		return nil, err
	}
	plan.files[file.Name()] = privateFileSnapshot{info: opened, exists: true}
	return file, nil
}

func (plan *authorityPathPlan) prepareDatabaseFile() error {
	expected := plan.files[plan.databasePath]
	if expected.exists {
		return verifyPrivateFileIdentity(plan.databasePath, expected.info)
	}
	fd, err := unix.Open(plan.databasePath,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(privateFileMode))
	if err != nil {
		return fmt.Errorf("open authority store: create database: %w", err)
	}
	file := os.NewFile(uintptr(fd), plan.databasePath)
	if file == nil {
		_ = unix.Close(fd)
		return errors.New("open authority store: create database returned no file")
	}
	if err := file.Chmod(privateFileMode); err != nil {
		_ = file.Close()
		return fmt.Errorf("open authority store: protect new database: %w", err)
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return fmt.Errorf("open authority store: inspect new database: %w", statErr)
	}
	if closeErr != nil {
		return fmt.Errorf("open authority store: close new database: %w", closeErr)
	}
	if err := validatePrivateFileInfo(info, filepath.Base(plan.databasePath)); err != nil {
		return err
	}
	if err := verifyPrivateFileIdentity(plan.databasePath, info); err != nil {
		return err
	}
	plan.files[plan.databasePath] = privateFileSnapshot{info: info, exists: true}
	return nil
}

func (plan *authorityPathPlan) verifyBeforeSQLite() error {
	if err := verifyDirectoryIdentity(plan.directory, plan.directoryID); err != nil {
		return err
	}
	for _, path := range authorityFilePaths(plan.databasePath) {
		expected := plan.files[path]
		if expected.exists {
			if err := verifyPrivateFileIdentity(path, expected.info); err != nil {
				return err
			}
			continue
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return fmt.Errorf("open authority store: revalidate %s: %w", filepath.Base(path), err)
			}
			return fmt.Errorf("open authority store: %s appeared while opening", filepath.Base(path))
		}
	}
	return nil
}

func (plan *authorityPathPlan) verifyAfterSQLite() error {
	if err := verifyDirectoryIdentity(plan.directory, plan.directoryID); err != nil {
		return err
	}
	for _, path := range authorityFilePaths(plan.databasePath) {
		current, err := inspectPrivateFile(path)
		if err != nil {
			return err
		}
		expected := plan.files[path]
		if (path == plan.databasePath || path == plan.databasePath+".writer.lock" || expected.exists) &&
			(!current.exists || !os.SameFile(expected.info, current.info)) {
			return fmt.Errorf("open authority store: %s identity changed while SQLite opened",
				filepath.Base(path))
		}
	}
	return nil
}

func verifyDirectoryIdentity(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("open authority store: revalidate state directory: %w", err)
	}
	if !os.SameFile(expected, current) {
		return errors.New("open authority store: state directory identity changed")
	}
	return validatePrivateDirectoryInfo(current)
}

func verifyPrivateFileIdentity(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("open authority store: revalidate %s: %w", filepath.Base(path), err)
	}
	if !os.SameFile(expected, current) {
		return fmt.Errorf("open authority store: %s identity changed", filepath.Base(path))
	}
	return validatePrivateFileInfo(current, filepath.Base(path))
}

func authorityFilePaths(databasePath string) []string {
	return []string{databasePath, databasePath + "-wal", databasePath + "-shm",
		databasePath + ".writer.lock"}
}

func releaseWriterLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}
