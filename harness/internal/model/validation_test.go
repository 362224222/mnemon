package model

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		max     int
		emptyOK bool
		wantErr error
	}{
		{name: "valid", value: "review\nnotes", max: 32},
		{name: "empty allowed", value: "", max: 32, emptyOK: true},
		{name: "empty rejected", value: "", max: 32, wantErr: ErrInvalid},
		{name: "natural whitespace preserved", value: " review\n", max: 32},
		{name: "control", value: "review\x00", max: 32, wantErr: ErrInvalid},
		{name: "too large", value: strings.Repeat("x", 33), max: 32, wantErr: ErrLimit},
		{name: "invalid utf8", value: string([]byte{0xff}), max: 32, wantErr: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateText("value", test.value, test.max, test.emptyOK)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validateText() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestSortedUnique(t *testing.T) {
	t.Parallel()

	got, err := sortedUnique([]string{"c", "a", "b"})
	if err != nil {
		t.Fatalf("sortedUnique() error = %v", err)
	}
	if want := "a,b,c"; strings.Join(got, ",") != want {
		t.Fatalf("sortedUnique() = %v, want %s", got, want)
	}
	if _, err := sortedUnique([]string{"a", "a"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate error = %v, want ErrInvalid", err)
	}
}

func TestSQLiteIntegerBoundary(t *testing.T) {
	t.Parallel()

	if err := validateSQLitePositive("sequence", MaxSQLiteInteger); err != nil {
		t.Fatalf("max SQLite integer rejected: %v", err)
	}
	if err := validateSQLitePositive("sequence", MaxSQLiteInteger+1); !errors.Is(err, ErrLimit) {
		t.Fatalf("overflow error = %v, want ErrLimit", err)
	}
	if err := validateSQLitePositive("sequence", 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero error = %v, want ErrInvalid", err)
	}
}
