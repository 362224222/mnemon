package event

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrInvalidCandidate = errors.New("invalid Teamwork candidate")
	ErrInvalidStamp     = errors.New("invalid admission stamp")
	ErrSignature        = errors.New("invalid publication signature")
)

func validateContent(field, value string, required bool) error {
	if !utf8.ValidString(value) {
		return candidateError(field, model.ErrInvalid, "must be valid UTF-8")
	}
	if required && value == "" {
		return candidateError(field, model.ErrInvalid, "must not be empty")
	}
	if len(value) > model.MaxContentBytes {
		return candidateError(field, model.ErrLimit, "got %d bytes, max %d", len(value), model.MaxContentBytes)
	}
	for _, r := range value {
		if r == 0 || (r < 0x20 && r != '\n' && r != '\t') {
			return candidateError(field, model.ErrInvalid, "contains a forbidden control character")
		}
	}
	return nil
}

func validateToken(field, value string) error {
	if !utf8.ValidString(value) || value == "" {
		return candidateError(field, model.ErrInvalid, "must be a nonempty UTF-8 token")
	}
	if len(value) > model.MaxIdentifierBytes {
		return candidateError(field, model.ErrLimit, "got %d bytes, max %d", len(value), model.MaxIdentifierBytes)
	}
	for _, r := range value {
		if r <= 0x20 || r == 0x7f {
			return candidateError(field, model.ErrInvalid, "must not contain whitespace or control characters")
		}
	}
	return nil
}

func candidateError(field string, cause error, format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	return fmt.Errorf("%w: %s: %w: %s", ErrInvalidCandidate, field, cause, detail)
}

func stampError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidStamp, fmt.Sprintf(format, args...))
}
