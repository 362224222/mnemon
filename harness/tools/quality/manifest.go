package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
)

var pointerReceiverPattern = regexp.MustCompile(`\(\*[A-Za-z_][A-Za-z0-9_]*\)`)

func readExactJSON[T any](path string) (T, error) {
	var zero T
	data, err := os.ReadFile(path)
	if err != nil {
		return zero, fmt.Errorf("read %s: %w", path, err)
	}
	value, err := decodeExactJSON[T](data, path)
	if err != nil {
		return zero, err
	}
	canonical, err := canonicalJSON(value)
	if err != nil {
		return zero, fmt.Errorf("encode canonical %s: %w", path, err)
	}
	if !bytes.Equal(data, canonical) {
		return zero, fmt.Errorf("%s is not canonical indented JSON with a trailing newline", path)
	}
	return value, nil
}

func decodeExactJSON[T any](data []byte, label string) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode %s: %w", label, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return value, fmt.Errorf("decode %s: multiple JSON values", label)
		}
		return value, fmt.Errorf("decode trailing %s: %w", label, err)
	}
	return value, nil
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func validateSchema(version int, label string) error {
	if version != manifestSchemaVersion {
		return fmt.Errorf("%s schema_version = %d, want %d", label, version, manifestSchemaVersion)
	}
	return nil
}

func validateFullCommit(value, field string) error {
	if len(value) != 40 {
		return fmt.Errorf("%s must be a full 40-character commit hash", field)
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return fmt.Errorf("%s must be a lowercase hexadecimal commit hash", field)
		}
	}
	return nil
}

func requireText(value, field string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must be non-empty and have no surrounding whitespace", field)
	}
	return nil
}

func rejectWildcard(value, field string) error {
	if strings.ContainsAny(value, "*?[") {
		return fmt.Errorf("%s contains a wildcard", field)
	}
	return nil
}

func rejectSymbolWildcard(value, field string) error {
	withoutPointerReceivers := pointerReceiverPattern.ReplaceAllString(value, "(receiver)")
	return rejectWildcard(withoutPointerReceivers, field)
}

func validateSortedUnique(values []string, field string, requireNonEmpty bool) error {
	if values == nil {
		return fmt.Errorf("%s must be a JSON array, not null", field)
	}
	if requireNonEmpty && len(values) == 0 {
		return fmt.Errorf("%s must not be empty", field)
	}
	if !sort.StringsAreSorted(values) {
		return fmt.Errorf("%s must be sorted", field)
	}
	for index, value := range values {
		if err := requireText(value, fmt.Sprintf("%s[%d]", field, index)); err != nil {
			return err
		}
		if index > 0 && values[index-1] == value {
			return fmt.Errorf("%s contains duplicate %q", field, value)
		}
	}
	return nil
}

func validateHarnessPath(value, field string) error {
	if err := validateRepoPath(value, field); err != nil {
		return err
	}
	if !strings.HasPrefix(value, "harness/") {
		return fmt.Errorf("%s must be below harness/", field)
	}
	return nil
}

func validateRepoPath(value, field string) error {
	if err := requireText(value, field); err != nil {
		return err
	}
	if strings.ContainsAny(value, "\\ \t\r\n") || strings.HasPrefix(value, "/") || value == "." || path.Clean(value) != value {
		return fmt.Errorf("%s must be a clean repository-relative path", field)
	}
	return rejectWildcard(value, field)
}

func parsePathSymbol(value string) (string, string, error) {
	separator := strings.LastIndex(value, "::")
	if separator <= 0 || separator+2 == len(value) {
		return "", "", fmt.Errorf("%q must use path::symbol", value)
	}
	return value[:separator], value[separator+2:], nil
}
