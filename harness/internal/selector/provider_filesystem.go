package selector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	providerDirectoryMode = 0o700
	providerFileMode      = 0o600
)

func prepareProviderFiles(databasePath string) (*os.File, error) {
	if databasePath == "" || !filepath.IsAbs(databasePath) ||
		filepath.Clean(databasePath) != databasePath || filepath.Base(databasePath) != "selector.db" {
		return nil, errors.New("open selector store: path must be absolute, clean, and name selector.db")
	}
	directory := filepath.Dir(databasePath)
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, fmt.Errorf("open selector store: inspect state directory: %w", err)
	}
	if err := validateProviderDirectory(info); err != nil {
		return nil, err
	}
	if err := ensureProviderFile(databasePath); err != nil {
		return nil, err
	}
	lock, err := openProviderFile(databasePath+".writer.lock", unix.O_RDWR|unix.O_CREAT)
	if err != nil {
		return nil, fmt.Errorf("open selector store: writer guard: %w", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("open selector store: writer already active: %w", err)
	}
	return lock, nil
}

func validateProviderDirectory(info os.FileInfo) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("open selector store: state directory is not a real directory")
	}
	if info.Mode().Perm() != providerDirectoryMode {
		return fmt.Errorf("open selector store: state directory mode is %04o, want %04o",
			info.Mode().Perm(), providerDirectoryMode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) {
		return errors.New("open selector store: state directory is not owned by current user")
	}
	return nil
}

func ensureProviderFile(path string) error {
	file, err := openProviderFile(path, unix.O_RDWR|unix.O_CREAT)
	if err != nil {
		return fmt.Errorf("open selector store: prepare database: %w", err)
	}
	return file.Close()
}

func openProviderFile(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, providerFileMode)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != providerFileMode ||
		uint32(stat.Uid) != uint32(os.Geteuid()) || stat.Nlink != 1 {
		_ = file.Close()
		return nil, errors.New("file must be private, regular, owner-held, and singly linked")
	}
	return file, nil
}

func closeProviderLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}
