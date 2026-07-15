package model

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	SchemaVersion           = 1
	MaxSQLiteInteger uint64 = 1<<63 - 1

	MaxIdentifierBytes     = 512
	MaxLabelBytes          = 128
	MaxSummaryBytes        = 8 << 10
	MaxContentBytes        = 8 << 10
	MaxArtifactRefs        = 16
	MaxCurrentArtifactRefs = 112
	MaxCausalityRefs       = 16
	MaxChildWorks          = 7
	MaxMembersPerChannel   = 8
	MaxChannelsPerNode     = 8
	MaxPublicationBytes    = 64 << 10
	MaxCanonicalJSONBytes  = 4 << 20
)

var (
	ErrInvalid   = errors.New("invalid model value")
	ErrInvariant = errors.New("model invariant violated")
	ErrLimit     = errors.New("model limit exceeded")
)

func invalid(field, detail string) error {
	return fmt.Errorf("%s: %w: %s", field, ErrInvalid, detail)
}

func invariant(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvariant, detail)
}

func limit(field string, got, max int) error {
	return fmt.Errorf("%s: %w: got %d bytes/items, max %d", field, ErrLimit, got, max)
}

func validateText(field, value string, max int, emptyOK bool) error {
	if !utf8.ValidString(value) {
		return invalid(field, "must be valid UTF-8")
	}
	if !emptyOK && value == "" {
		return invalid(field, "must not be empty")
	}
	if len(value) > max {
		return limit(field, len(value), max)
	}
	for _, r := range value {
		if r == 0 || (r < 0x20 && r != '\n' && r != '\t') {
			return invalid(field, "contains a forbidden control character")
		}
	}
	return nil
}

func validateIdentifier(field, value string) error {
	if err := validateText(field, value, MaxIdentifierBytes, false); err != nil {
		return err
	}
	for _, r := range value {
		if r <= 0x20 || r == 0x7f {
			return invalid(field, "must not contain whitespace or control characters")
		}
	}
	return nil
}

func validateSQLitePositive(field string, value uint64) error {
	if value == 0 {
		return invalid(field, "must be greater than zero")
	}
	if value > MaxSQLiteInteger {
		return fmt.Errorf("%s: %w: %d exceeds SQLite INTEGER max %d", field, ErrLimit, value, MaxSQLiteInteger)
	}
	return nil
}

func sortedUnique[T ~string](values []T) ([]T, error) {
	result := append([]T(nil), values...)
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j] < result[j-1]; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, invalid("collection", "duplicate value")
		}
	}
	return result, nil
}
