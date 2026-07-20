package upload

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var ErrInvalidName = errors.New("invalid upload name")

func Save(root, name string, body io.Reader) (string, error) {
	if root == "" || name == "" {
		return "", ErrInvalidName
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	destination := filepath.Join(root, name)
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		return "", err
	}
	return destination, nil
}
