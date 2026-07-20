package peer

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelJoinRemoteFailurePreservesCommitUnknownReservation(t *testing.T) {
	session := &channelJoinSession{reservationActive: true, releaseReservation: false,
		prepared: store.PrepareJoinedChannelResult{CommitUnknown: true}}
	session.completeRemoteFailure()
	if !session.completed || session.releaseReservation {
		t.Fatal("commit-unknown reservation was released after a remote failure")
	}

	session.prepared.CommitUnknown = false
	session.completeRemoteFailure()
	if !session.releaseReservation {
		t.Fatal("known precommit reservation was not released after a remote rejection")
	}
}
