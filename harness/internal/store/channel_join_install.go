package store

import (
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"strings"
	"time"
)

type InstallJoinedChannelSpec struct {
	AuthenticatedOwnerPeerID model.PeerID
	LocalAlias               string
	Descriptor               model.SignedChannelDescriptor
	Transcript               model.EnrollmentTranscript
	Receipt                  model.EnrollmentReceipt
	Members                  []model.Member
	At                       time.Time
}

type InstallJoinedChannelResult struct {
	Installed bool
	Status    ChannelEnrollmentStatus
	Channel   model.Channel
	Roster    model.VerifiedRoster
}

func mapChannelJoinError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "node_channel_limit") {
		return fmt.Errorf("%w: %v", ErrNodeChannelLimit, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		strings.Contains(message, "join reservation conflicts") ||
		strings.Contains(message, "conflicts with reserved join") ||
		strings.Contains(message, "join reservation attempt") ||
		strings.Contains(message, "join reservation state or time") ||
		errors.Is(err, ErrChannelAuthorityInvariant) || errors.Is(err, ErrChannelEnrollmentConflict) {
		return fmt.Errorf("%w: %v", ErrChannelJoinConflict, err)
	}
	return fmt.Errorf("install joined Channel: %w", err)
}
