package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/term"
)

const maxChannelTokenInputBytes = int64(model.MaxChannelRecordBytes*2 + 1)

func readChannelJoinToken(stdin io.Reader, stderr io.Writer, filePath string) (string, error) {
	if filePath != "" {
		return readChannelTokenFile(filePath)
	}
	if terminal, ok := stdin.(*os.File); ok && term.IsTerminal(int(terminal.Fd())) {
		if _, err := io.WriteString(stderr, "Channel invite token: "); err != nil {
			return "", err
		}
		raw, err := term.ReadPassword(int(terminal.Fd()))
		_, _ = io.WriteString(stderr, "\n")
		if err != nil {
			return "", err
		}
		defer clear(raw)
		return validateChannelTokenInput(raw)
	}
	raw, err := io.ReadAll(io.LimitReader(stdin, maxChannelTokenInputBytes+1))
	if err != nil || int64(len(raw)) > maxChannelTokenInputBytes {
		return "", errors.New("Channel invite token input is unavailable or too large")
	}
	defer clear(raw)
	return validateChannelTokenInput(raw)
}

func readChannelTokenFile(path string) (string, error) {
	if path == "" || path == "-" {
		return "", errors.New("invite file must be an explicit owner-only path")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
		info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > maxChannelTokenInputBytes {
		return "", errors.New("invite file must be an owner-only regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return "", errors.New("invite file must be owned by the current user")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("invite file is unavailable")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", errors.New("invite file identity changed")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxChannelTokenInputBytes+1))
	if err != nil || int64(len(raw)) > maxChannelTokenInputBytes {
		return "", errors.New("invite file is unavailable or too large")
	}
	defer clear(raw)
	return validateChannelTokenInput(raw)
}

func validateChannelTokenInput(raw []byte) (string, error) {
	value := strings.TrimSuffix(string(raw), "\n")
	value = strings.TrimSuffix(value, "\r")
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\t ") {
		return "", errors.New("invite input must contain exactly one token")
	}
	if _, err := model.ParseEnrollmentToken(value); err != nil {
		return "", fmt.Errorf("invite input is not a valid Channel token")
	}
	return value, nil
}
