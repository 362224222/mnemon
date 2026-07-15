package model

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCanonicalizeJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr error
	}{
		{name: "sort and compact", input: ` { "z": 2, "a": {"b":true,"a":"<ok>"} } `, want: `{"a":{"a":"<ok>","b":true},"z":2}`},
		{name: "array order", input: `[3,1,2]`, want: `[3,1,2]`},
		{name: "negative zero", input: `-0`, want: `0`},
		{name: "surrogate pair", input: `"\ud83e\udde0"`, want: `"🧠"`},
		{name: "duplicate key", input: `{"a":1,"a":2}`, wantErr: ErrInvalid},
		{name: "lone high surrogate", input: `"\ud800"`, wantErr: ErrInvalid},
		{name: "lone low surrogate", input: `"\udc00"`, wantErr: ErrInvalid},
		{name: "float rejected", input: `1.5`, wantErr: ErrInvalid},
		{name: "trailing value", input: `{} {}`, wantErr: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := CanonicalizeJSON([]byte(test.input))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("CanonicalizeJSON() error = %v, want %v", err, test.wantErr)
			}
			if string(got) != test.want {
				t.Fatalf("CanonicalizeJSON() = %s, want %s", got, test.want)
			}
		})
	}
	if _, err := CanonicalizeJSON([]byte{'"', 0xff, '"'}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid UTF-8 error = %v, want ErrInvalid", err)
	}
}

func TestJSONIsImmutable(t *testing.T) {
	t.Parallel()

	input := []byte(`{"b":2,"a":1}`)
	value, err := NewJSON(input)
	if err != nil {
		t.Fatalf("NewJSON() error = %v", err)
	}
	input[2] = 'x'
	first := value.Bytes()
	first[0] = '['
	if got, want := value.String(), `{"a":1,"b":2}`; got != want {
		t.Fatalf("JSON mutated: got %s, want %s", got, want)
	}
}

func TestJSONLimit(t *testing.T) {
	t.Parallel()

	_, err := NewJSON([]byte(`"` + strings.Repeat("x", MaxCanonicalJSONBytes+1) + `"`))
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("NewJSON() error = %v, want ErrLimit", err)
	}
}

func TestDigest(t *testing.T) {
	t.Parallel()

	digest := Sum([]byte("mnemon"))
	parsed, err := ParseDigest(digest.String())
	if err != nil {
		t.Fatalf("ParseDigest() error = %v", err)
	}
	if parsed != digest || !bytes.Equal(parsed.Bytes(), digest.Bytes()) {
		t.Fatalf("digest round trip mismatch")
	}
	if _, err := ParseDigest(strings.ToUpper(digest.String())); !errors.Is(err, ErrInvalid) {
		t.Fatalf("uppercase digest error = %v, want ErrInvalid", err)
	}
}

func TestCanonicalTime(t *testing.T) {
	t.Parallel()

	local := time.Date(2026, 7, 16, 12, 0, 0, 123, time.FixedZone("local", 8*60*60))
	got, err := canonicalTime(local)
	if err != nil {
		t.Fatalf("canonicalTime() error = %v", err)
	}
	if got.Location() != time.UTC || formatTime(got) != "2026-07-16T04:00:00.000000123Z" {
		t.Fatalf("canonicalTime() = %s", formatTime(got))
	}
	for _, value := range []time.Time{
		time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		if _, err := canonicalTime(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("canonicalTime(%s) error = %v", value, err)
		}
	}
}
