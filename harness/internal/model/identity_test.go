package model

import (
	"errors"
	"strings"
	"testing"
)

func TestIdentifierParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "valid", value: "12D3KooW-example", ok: true},
		{name: "empty"},
		{name: "space", value: "peer id"},
		{name: "too long", value: strings.Repeat("x", MaxIdentifierBytes+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParsePeerID(test.value)
			if test.ok {
				if err != nil || got.String() != test.value {
					t.Fatalf("ParsePeerID() = %q, %v", got.String(), err)
				}
			} else if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrLimit) {
				t.Fatalf("ParsePeerID() error = %v", err)
			}
		})
	}
}

func TestIdentityReferences(t *testing.T) {
	t.Parallel()

	peer, _ := ParsePeerID("peer-a")
	epoch, _ := ParseOriginEpoch("epoch-a")
	eventID, _ := ParseEventID("event-a")
	workID, _ := ParseWorkID("work-a")
	grantID, err := ParseGrantID("grant-a")
	if err != nil || grantID.String() != "grant-a" {
		t.Fatalf("ParseGrantID() = %#v, %v", grantID, err)
	}

	work, err := NewWorkRef(peer, workID)
	if err != nil || work.HomePeerID() != peer || work.WorkID() != workID {
		t.Fatalf("NewWorkRef() = %#v, %v", work, err)
	}
	event, err := NewEventKey(peer, epoch, eventID)
	if err != nil || event.OriginPeerID() != peer || event.EventID() != eventID {
		t.Fatalf("NewEventKey() = %#v, %v", event, err)
	}
	if _, err := NewOriginKey(peer, epoch, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero origin sequence error = %v", err)
	}
}

func TestRecordHead(t *testing.T) {
	t.Parallel()

	if _, err := NewRecordHead(0, Sum([]byte("record"))); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero revision error = %v", err)
	}
	head, err := NewRecordHead(2, Sum([]byte("record")))
	if err != nil || head.Revision() != 2 {
		t.Fatalf("NewRecordHead() = %#v, %v", head, err)
	}
}
