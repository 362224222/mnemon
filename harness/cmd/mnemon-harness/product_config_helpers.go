package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
)

func loadHarnessProductConfig(root, explicit string) (productconfig.Config, string, error) {
	path := productconfig.DefaultPath(filepath.Clean(root), explicit)
	cfg, err := productconfig.Load(path)
	if err == nil {
		return cfg, path, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return productconfig.Config{}, "", err
	}
	if legacy, found, legacyErr := productconfig.FromLegacy(filepath.Clean(root)); legacyErr != nil {
		return productconfig.Config{}, "", legacyErr
	} else if found {
		return legacy, path, nil
	}
	return productconfig.Default(), path, nil
}

func saveHarnessProductConfig(root, explicit string, cfg productconfig.Config) (string, error) {
	path := productconfig.DefaultPath(filepath.Clean(root), explicit)
	return path, productconfig.Save(path, cfg)
}
