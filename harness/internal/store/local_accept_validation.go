package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateAdmissionAuthority(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, acceptedAt time.Time,
) ([]byte, error) {
	snapshot := spec.Scope
	authority, err := requireFrozenAdmissionAuthority(ctx, tx, snapshot,
		acceptedAt, uint64(len(spec.Items)))
	if err != nil {
		return nil, err
	}
	if spec.Operation != nil {
		if err := requireActingAgentRunAuthority(ctx, tx, operation, authority.profile); err != nil {
			return nil, err
		}
	}
	if err := requireFrozenPublicationHead(ctx, tx, authority.node, snapshot, uint64(len(spec.Items))); err != nil {
		return nil, err
	}
	publicKey, err := requireFrozenOriginMemberHead(ctx, tx, authority.node, snapshot)
	if err != nil {
		return nil, err
	}
	if err := requireAdmissionAudienceBaselines(ctx, tx, authority.node,
		snapshot.ChannelID(), spec.Items); err != nil {
		return nil, err
	}
	return publicKey, nil
}

type frozenAdmissionAuthority struct {
	node    model.Node
	profile model.Profile
}

func requireFrozenAdmissionAuthority(ctx context.Context, tx *sql.Tx,
	snapshot LocalAdmissionScope, acceptedAt time.Time, count uint64,
) (frozenAdmissionAuthority, error) {
	node, err := readNode(ctx, tx)
	if err != nil || node.PeerID() != snapshot.Node().PeerID() ||
		node.OriginEpoch() != snapshot.Node().OriginEpoch() ||
		node.NextOriginSequence() != snapshot.FirstOriginSequence() ||
		node.ActiveAssetRevision() != snapshot.Node().ActiveAssetRevision() {
		return frozenAdmissionAuthority{}, fmt.Errorf("%w: Node authority changed", ErrAdmissionConflict)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil || !profile.Enabled() || !sameProfileIdentity(profile, snapshot.Profile()) ||
		!sameProfileAuthority(profile, snapshot.Profile()) ||
		profile.ActiveAssetRevision() != node.ActiveAssetRevision() {
		return frozenAdmissionAuthority{}, fmt.Errorf("%w: Profile authority changed", ErrAdmissionConflict)
	}
	if count > model.MaxSQLiteInteger || node.NextOriginSequence() > model.MaxSQLiteInteger-count ||
		acceptedAt.Before(node.UpdatedAt()) {
		return frozenAdmissionAuthority{}, fmt.Errorf("%w: origin sequence successor exhausted or clock regressed",
			ErrAdmissionConflict)
	}
	return frozenAdmissionAuthority{node: node, profile: profile}, nil
}

// requireActingAgentRunAuthority requires the operation's AgentRun to still be
// bound to the authenticated Profile runtime in a non-terminal status.
func requireActingAgentRunAuthority(ctx context.Context, tx *sql.Tx, operation model.Operation,
	profile model.Profile,
) error {
	var runProfile, runtime, runStatus string
	err := tx.QueryRowContext(ctx, `SELECT profile_id,runtime_kind,status FROM agent_runs WHERE run_id=?`,
		operation.AgentRunID().String()).Scan(&runProfile, &runtime, &runStatus)
	if err != nil || runProfile != profile.ID().String() || runtime != string(profile.Runtime()) ||
		(runStatus != "starting" && runStatus != "running" && runStatus != "runtime_finished") {
		return fmt.Errorf("%w: acting AgentRun authority changed", ErrAdmissionConflict)
	}
	return nil
}

// requireFrozenPublicationHead re-reads the Channel and publication epoch and
// rejects the batch when status, topic, roster head or source head drifted
// from the frozen admission snapshot.
func requireFrozenPublicationHead(ctx context.Context, tx *sql.Tx, node model.Node,
	snapshot LocalAdmissionScope, count uint64,
) error {
	var status, topic string
	var rosterRevision, sourceHead uint64
	var rosterHash []byte
	err := tx.QueryRowContext(ctx, `SELECT c.status,c.topic_state,c.roster_head_revision,c.roster_head_hash,
		p.source_head_channel_seq FROM channels c JOIN publication_epochs p ON p.channel_id=c.channel_id
		AND p.origin_peer_id=? AND p.origin_epoch=? WHERE c.channel_id=?`, node.PeerID().String(),
		node.OriginEpoch().String(), snapshot.ChannelID().String()).Scan(&status, &topic, &rosterRevision, &rosterHash, &sourceHead)
	if err != nil || status != string(model.ChannelActive) || topic != string(model.TopicJoined) ||
		rosterRevision != snapshot.PublicationRoster().Revision() || !bytes.Equal(rosterHash, snapshot.PublicationRoster().Digest().Bytes()) ||
		sourceHead+1 != snapshot.FirstChannelSequence() || sourceHead > model.MaxSQLiteInteger-count {
		return fmt.Errorf("%w: Channel or publication head changed", ErrAdmissionConflict)
	}
	return nil
}

// requireFrozenOriginMemberHead re-reads the local origin membership head and
// returns its durable public key for publication signature validation.
func requireFrozenOriginMemberHead(ctx context.Context, tx *sql.Tx, node model.Node,
	snapshot LocalAdmissionScope,
) ([]byte, error) {
	var memberRevision uint64
	var memberHash, publicKey []byte
	var epoch, memberStatus string
	err := tx.QueryRowContext(ctx, `SELECT revision,record_hash,origin_epoch,status,public_key FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, snapshot.ChannelID().String(),
		node.PeerID().String()).Scan(&memberRevision, &memberHash, &epoch, &memberStatus, &publicKey)
	if err != nil || memberRevision != snapshot.OriginMember().Revision() ||
		!bytes.Equal(memberHash, snapshot.OriginMember().Digest().Bytes()) || epoch != node.OriginEpoch().String() ||
		memberStatus != string(model.MemberActive) {
		return nil, fmt.Errorf("%w: origin member head changed", ErrAdmissionConflict)
	}
	return append([]byte(nil), publicKey...), nil
}

// requireAdmissionAudienceBaselines checks every distinct audience target of
// the batch for an active binding and confirmed outbound baseline.
func requireAdmissionAudienceBaselines(ctx context.Context, tx *sql.Tx, node model.Node,
	channel model.ChannelID, items []LocalAcceptanceItem,
) error {
	seen := make(map[model.PeerID]struct{})
	for _, item := range items {
		for _, target := range item.Publication.Event().Audience().Peers() {
			if _, ok := seen[target]; ok {
				continue
			}
			seen[target] = struct{}{}
			if err := requireConfirmedAudienceBaseline(ctx, tx, node, channel, target); err != nil {
				return err
			}
		}
	}
	return nil
}

// requireConfirmedAudienceBaseline requires one audience target to hold an
// active binding whose outbound DataBaseline this origin has already durably
// confirmed; any other state keeps the target out of new Event audiences.
func requireConfirmedAudienceBaseline(ctx context.Context, tx *sql.Tx, node model.Node,
	channel model.ChannelID, target model.PeerID,
) error {
	var binding string
	var confirmed sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT b.state,a.baseline_confirmed_at FROM peer_bindings b
		LEFT JOIN peer_pull_acks a ON a.channel_id=b.channel_id AND a.target_peer_id=b.peer_id
		AND a.origin_peer_id=? AND a.origin_epoch=? WHERE b.channel_id=? AND b.peer_id=?`,
		node.PeerID().String(), node.OriginEpoch().String(), channel.String(), target.String()).Scan(&binding, &confirmed)
	if err != nil || binding != string(model.BindingActive) || !confirmed.Valid {
		return fmt.Errorf("%w: target %s", ErrAudienceUnavailable, target.String())
	}
	return nil
}

func validateWorkItem(item LocalAcceptanceItem, event model.Event) error {
	mutatesWork := event.Type() == model.EventReviewOffered || event.Type() == model.EventReviewAccepted ||
		event.Type() == model.EventReviewDelivered || event.Type() == model.EventReviewReworkRequested ||
		event.Type() == model.EventReviewClosed || event.Type() == model.EventReviewDeclined ||
		event.Type() == model.EventReviewCancelled || event.Type() == model.EventReviewExpired
	if mutatesWork != (item.Work != nil) {
		return errors.New("commit local acceptance: Event Work mutation presence is invalid")
	}
	if item.Work != nil {
		if item.Work.Work.Ref() != event.Scope().WorkRef() || !item.Work.Work.UpdatedAt().Equal(event.AcceptedAt()) ||
			item.Work.Work.UpdatedBy() != event.ID() || item.Work.Work.StateData().String() != event.Payload().String() {
			return errors.New("commit local acceptance: Work mutation projection mismatch")
		}
		if event.Type() == model.EventReviewOffered && item.Work.ExpectedVersion != 0 {
			return errors.New("commit local acceptance: offered Work must be a creation")
		}
		if event.Type() != model.EventReviewOffered && item.Work.ExpectedVersion == 0 {
			return errors.New("commit local acceptance: home state Event requires Work CAS")
		}
	}
	return nil
}

func validateParticipantBinding(ctx context.Context, tx *sql.Tx, item LocalAcceptanceItem, event model.Event) error {
	payload, err := decodeClosedEventPayload(event)
	if err != nil {
		return err
	}
	scope := event.Scope()
	if event.Type() == model.EventReviewOffered {
		return validateOfferParticipantSnapshot(item, event, payload)
	}
	current, err := readReviewWork(ctx, tx, scope.WorkRef())
	if err != nil {
		return fmt.Errorf("commit local acceptance: current Work: %w", err)
	}
	if current.ChannelID() != scope.ChannelID() {
		return errors.New("commit local acceptance: Event payload does not bind current Work")
	}
	receiptOnly := event.Type() == model.EventReviewAcceptRejected || event.Type() == model.EventReviewOutcome
	if receiptOnly {
		if !receiptVersionAtOrBeforeCurrent(current, payload.WorkVersion, payload.Iteration) {
			return errors.New("commit local acceptance: receipt payload is ahead of or inconsistent with current Work")
		}
	} else if payload.WorkVersion != current.Version() || payload.Iteration != current.Iteration() {
		return errors.New("commit local acceptance: Event payload does not bind current Work")
	}
	home, reviewer := current.Ref().HomePeerID(), current.Participants().ReviewerPeerID()
	if event.Type() == model.EventReviewExpired && payload.DeadlineUnixNano != current.DeadlineUnixNano() {
		return errors.New("commit local acceptance: expiry payload changed frozen deadline")
	}
	if event.Type().ParticipantInput() {
		return validateReviewerInputBinding(event, current)
	}
	if scope.OriginPeerID() == home {
		if event.Audience().Len() != 1 || !event.Audience().Contains(reviewer) {
			return errors.New("commit local acceptance: home Event audience is not frozen reviewer")
		}
		return nil
	}
	if event.Type() == model.EventReviewOutcome && scope.OriginPeerID() == reviewer &&
		event.Audience().Len() == 1 && event.Audience().Contains(home) {
		return nil
	}
	return errors.New("commit local acceptance: Event origin is not a frozen Work participant")
}

// validateOfferParticipantSnapshot binds a review.offered Event to its created
// Work: initiator origin, frozen roster revision, single reviewer audience and
// the exact creation version, iteration and deadline.
func validateOfferParticipantSnapshot(item LocalAcceptanceItem, event model.Event,
	payload closedPayloadFacts,
) error {
	scope := event.Scope()
	work := item.Work.Work
	reviewer := work.Participants().ReviewerPeerID()
	if work.Participants().InitiatorPeerID() != scope.OriginPeerID() ||
		work.Participants().RosterRevision() != scope.PublicationRoster().Revision() ||
		event.Audience().Len() != 1 || !event.Audience().Contains(reviewer) ||
		payload.WorkVersion != 1 || payload.Iteration != 1 || payload.DeadlineUnixNano != work.DeadlineUnixNano() {
		return errors.New("commit local acceptance: offer participant snapshot mismatch")
	}
	return nil
}

// validateReviewerInputBinding requires participant input to originate from
// the frozen reviewer, address the Work home, and match a valid source state.
func validateReviewerInputBinding(event model.Event, current model.ReviewWork) error {
	home, reviewer := current.Ref().HomePeerID(), current.Participants().ReviewerPeerID()
	validState := (event.Type() == model.EventReviewAcceptRequested || event.Type() == model.EventReviewDeclineRequested) &&
		current.State() == model.WorkOffered
	validState = validState || event.Type() == model.EventReviewDeliveryReady &&
		(current.State() == model.WorkActive || current.State() == model.WorkRework)
	if event.Scope().OriginPeerID() != reviewer || event.Audience().Len() != 1 ||
		!event.Audience().Contains(home) || !validState {
		return errors.New("commit local acceptance: participant input is not frozen reviewer authority")
	}
	return nil
}
