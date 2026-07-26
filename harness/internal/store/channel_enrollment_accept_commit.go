package store

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// acceptedEnrollment carries the immutable prepared authority and signed
// artifacts from external signing into the fenced commit transaction.
type acceptedEnrollment struct {
	channel      model.Channel
	roster       model.VerifiedRoster
	member       model.Member
	useID        model.EnrollmentUseID
	receipt      model.EnrollmentReceipt
	joinIdentity model.Digest
	at           time.Time
	authority    verifiedChannelAuthority
	node         model.Node
	grant        durableEnrollmentGrant
}

func (s *Store) commitAcceptedEnrollment(ctx context.Context, spec AcceptChannelEnrollmentSpec,
	evidence acceptedEnrollment,
) (AcceptChannelEnrollmentResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptChannelEnrollmentResult{},
			fmt.Errorf("accept Channel enrollment: begin commit: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readAcceptEnrollmentAuthority(ctx, tx, spec.Transcript)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	grant, existing, replay, err := readAcceptEnrollmentGrant(ctx, tx, authority, spec)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if replay {
		return replayAcceptedEnrollment(ctx, tx, authority, existing, spec, grant,
			evidence.joinIdentity)
	}
	if err := authorizeFreshEnrollment(authority, grant, spec, evidence.at); err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if !sameAcceptedEnrollmentFence(evidence, authority, node, grant) {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentStale
	}
	if err := persistAcceptedEnrollment(ctx, tx, node, authority, grant, evidence); err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	committed, err := verifyCommittedEnrollment(ctx, tx, node, evidence.channel.ID(),
		evidence.roster.Head())
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	return AcceptChannelEnrollmentResult{Status: ChannelEnrollmentAccepted,
		Channel: committed.channel, Roster: committed.roster,
		Member: evidence.member, Receipt: evidence.receipt}, nil
}

func sameAcceptedEnrollmentFence(evidence acceptedEnrollment,
	authority verifiedChannelAuthority, node model.Node, grant durableEnrollmentGrant,
) bool {
	before, after := evidence.authority.channel, authority.channel
	return evidence.node.PeerID() == node.PeerID() &&
		bytes.Equal(before.Descriptor().WireJSON().Bytes(), after.Descriptor().WireJSON().Bytes()) &&
		before.LocalAlias() == after.LocalAlias() && before.RosterHead() == after.RosterHead() &&
		before.Status() == after.Status() && before.TopicState() == after.TopicState() &&
		before.UpdatedAt().Equal(after.UpdatedAt()) && evidence.grant.id == grant.id &&
		evidence.grant.channelID == grant.channelID &&
		bytes.Equal(evidence.grant.verifier.Bytes(), grant.verifier.Bytes()) &&
		evidence.grant.expiresAt.Equal(grant.expiresAt) &&
		evidence.grant.maxUses == grant.maxUses && evidence.grant.usedUses == grant.usedUses &&
		evidence.grant.status == grant.status && evidence.grant.createdAt.Equal(grant.createdAt) &&
		evidence.grant.closedAt == grant.closedAt && evidence.grant.useCount == grant.useCount
}
