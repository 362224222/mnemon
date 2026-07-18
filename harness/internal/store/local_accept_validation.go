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
	node, err := readNode(ctx, tx)
	if err != nil || node.PeerID() != snapshot.Node().PeerID() || node.OriginEpoch() != snapshot.Node().OriginEpoch() ||
		node.NextOriginSequence() != snapshot.FirstOriginSequence() || node.ActiveAssetRevision() != snapshot.Node().ActiveAssetRevision() {
		return nil, fmt.Errorf("%w: Node authority changed", ErrAdmissionConflict)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil || !profile.Enabled() || !sameProfileIdentity(profile, snapshot.Profile()) ||
		!sameProfileAuthority(profile, snapshot.Profile()) || profile.ActiveAssetRevision() != node.ActiveAssetRevision() {
		return nil, fmt.Errorf("%w: Profile authority changed", ErrAdmissionConflict)
	}
	if spec.Operation != nil {
		var runProfile, runtime, runStatus string
		err := tx.QueryRowContext(ctx, `SELECT profile_id,runtime_kind,status FROM agent_runs WHERE run_id=?`,
			operation.AgentRunID().String()).Scan(&runProfile, &runtime, &runStatus)
		if err != nil || runProfile != profile.ID().String() || runtime != string(profile.Runtime()) ||
			(runStatus != "starting" && runStatus != "running" && runStatus != "runtime_finished") {
			return nil, fmt.Errorf("%w: acting AgentRun authority changed", ErrAdmissionConflict)
		}
	}
	count := uint64(len(spec.Items))
	if node.NextOriginSequence() > model.MaxSQLiteInteger-count || acceptedAt.Before(node.UpdatedAt()) {
		return nil, fmt.Errorf("%w: origin sequence successor exhausted or clock regressed", ErrAdmissionConflict)
	}
	var status, topic string
	var rosterRevision, sourceHead uint64
	var rosterHash []byte
	err = tx.QueryRowContext(ctx, `SELECT c.status,c.topic_state,c.roster_head_revision,c.roster_head_hash,
		p.source_head_channel_seq FROM channels c JOIN publication_epochs p ON p.channel_id=c.channel_id
		AND p.origin_peer_id=? AND p.origin_epoch=? WHERE c.channel_id=?`, node.PeerID().String(),
		node.OriginEpoch().String(), snapshot.ChannelID().String()).Scan(&status, &topic, &rosterRevision, &rosterHash, &sourceHead)
	if err != nil || status != string(model.ChannelActive) || topic != string(model.TopicJoined) ||
		rosterRevision != snapshot.PublicationRoster().Revision() || !bytes.Equal(rosterHash, snapshot.PublicationRoster().Digest().Bytes()) ||
		sourceHead+1 != snapshot.FirstChannelSequence() || sourceHead > model.MaxSQLiteInteger-count {
		return nil, fmt.Errorf("%w: Channel or publication head changed", ErrAdmissionConflict)
	}
	var memberRevision uint64
	var memberHash, publicKey []byte
	var epoch, memberStatus string
	err = tx.QueryRowContext(ctx, `SELECT revision,record_hash,origin_epoch,status,public_key FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, snapshot.ChannelID().String(),
		node.PeerID().String()).Scan(&memberRevision, &memberHash, &epoch, &memberStatus, &publicKey)
	if err != nil || memberRevision != snapshot.OriginMember().Revision() ||
		!bytes.Equal(memberHash, snapshot.OriginMember().Digest().Bytes()) || epoch != node.OriginEpoch().String() ||
		memberStatus != string(model.MemberActive) {
		return nil, fmt.Errorf("%w: origin member head changed", ErrAdmissionConflict)
	}
	seen := make(map[model.PeerID]struct{})
	for _, item := range spec.Items {
		for _, target := range item.Publication.Event().Audience().Peers() {
			if _, ok := seen[target]; ok {
				continue
			}
			seen[target] = struct{}{}
			var binding string
			var confirmed sql.NullString
			err := tx.QueryRowContext(ctx, `SELECT b.state,a.baseline_confirmed_at FROM peer_bindings b
				LEFT JOIN peer_pull_acks a ON a.channel_id=b.channel_id AND a.target_peer_id=b.peer_id
				AND a.origin_peer_id=? AND a.origin_epoch=? WHERE b.channel_id=? AND b.peer_id=?`,
				node.PeerID().String(), node.OriginEpoch().String(), snapshot.ChannelID().String(), target.String()).Scan(&binding, &confirmed)
			if err != nil || binding != string(model.BindingActive) || !confirmed.Valid {
				return nil, fmt.Errorf("%w: target %s", ErrAudienceUnavailable, target.String())
			}
		}
	}
	return append([]byte(nil), publicKey...), nil
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
	current, err := readReviewWork(ctx, tx, scope.WorkRef())
	if err != nil {
		return fmt.Errorf("commit local acceptance: current Work: %w", err)
	}
	if current.ChannelID() != scope.ChannelID() || payload.WorkVersion != current.Version() || payload.Iteration != current.Iteration() {
		return errors.New("commit local acceptance: Event payload does not bind current Work")
	}
	home, reviewer := current.Ref().HomePeerID(), current.Participants().ReviewerPeerID()
	if event.Type() == model.EventReviewExpired && payload.DeadlineUnixNano != current.DeadlineUnixNano() {
		return errors.New("commit local acceptance: expiry payload changed frozen deadline")
	}
	if event.Type().ParticipantInput() {
		validState := (event.Type() == model.EventReviewAcceptRequested || event.Type() == model.EventReviewDeclineRequested) &&
			current.State() == model.WorkOffered
		validState = validState || event.Type() == model.EventReviewDeliveryReady &&
			(current.State() == model.WorkActive || current.State() == model.WorkRework)
		if scope.OriginPeerID() != reviewer || event.Audience().Len() != 1 || !event.Audience().Contains(home) || !validState {
			return errors.New("commit local acceptance: participant input is not frozen reviewer authority")
		}
		return nil
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

func validateOperationEvents(authority *LocalOperationAuthority, events []model.Event) error {
	if authority == nil {
		if len(events) != 1 {
			return errors.New("commit local acceptance: controller accepts exactly one Event")
		}
		switch events[0].Type() {
		case model.EventReviewAccepted, model.EventReviewAcceptRejected, model.EventReviewDelivered,
			model.EventReviewDeclined, model.EventReviewExpired, model.EventReviewOutcome:
			if len(events[0].CausedBy()) == 0 {
				return errors.New("commit local acceptance: controller Event requires source causality")
			}
			return nil
		default:
			return errors.New("commit local acceptance: Event type is not controller-authoritative")
		}
	}
	want := map[model.OperationKind]model.EventType{
		model.OperationTeamworkOffer: model.EventReviewOffered, model.OperationTeamworkAccept: model.EventReviewAcceptRequested,
		model.OperationTeamworkDecline: model.EventReviewDeclineRequested, model.OperationTeamworkDeliver: model.EventReviewDeliveryReady,
		model.OperationTeamworkRework: model.EventReviewReworkRequested, model.OperationTeamworkClose: model.EventReviewClosed,
		model.OperationTeamworkCancel: model.EventReviewCancelled,
	}[authority.Kind]
	if !want.Valid() {
		return errors.New("commit local acceptance: operation kind does not admit an Event")
	}
	if len(events) > 1 && want != model.EventReviewOffered {
		return errors.New("commit local acceptance: only teamwork.offer may expand a batch")
	}
	var previousReviewer model.PeerID
	for index, event := range events {
		if event.Type() != want {
			return fmt.Errorf("commit local acceptance: operation %s cannot emit %s", authority.Kind, event.Type())
		}
		if want == model.EventReviewOffered {
			if event.Audience().Len() != 1 {
				return errors.New("commit local acceptance: offer batch must use canonical unique reviewer order")
			}
			reviewer := event.Audience().Peers()[0]
			if index > 0 {
				comparison, err := model.ComparePeerIDs(previousReviewer, reviewer)
				if err != nil || comparison >= 0 {
					return errors.New("commit local acceptance: offer batch must use canonical unique reviewer order")
				}
			}
			previousReviewer = reviewer
			if index > 0 && !sameExpandedOfferSemantics(events[0], event) {
				return errors.New("commit local acceptance: expanded offers changed content, deadline, Artifact or causality")
			}
		} else if len(event.CausedBy()) == 0 {
			return errors.New("commit local acceptance: context action requires source causality")
		}
	}
	return nil
}

func sameExpandedOfferSemantics(left, right model.Event) bool {
	leftArtifacts, _ := model.JSONFrom(left.Artifacts())
	rightArtifacts, _ := model.JSONFrom(right.Artifacts())
	leftCauses, _ := model.JSONFrom(left.CausedBy())
	rightCauses, _ := model.JSONFrom(right.CausedBy())
	return left.Summary() == right.Summary() && left.Payload().String() == right.Payload().String() &&
		leftArtifacts.String() == rightArtifacts.String() && leftCauses.String() == rightCauses.String()
}

func validateAcceptanceArtifacts(ctx context.Context, tx *sql.Tx, operation model.Operation,
	spec LocalAcceptanceSpec, events []model.Event,
) ([]captureRoot, error) {
	var capture []captureRoot
	if value, ok := operation.Capture(); ok {
		var err error
		capture, err = parseOperationCapture(value)
		if err != nil {
			return nil, err
		}
	}
	captured := captureRootSet(capture)
	for _, root := range capture {
		verified, err := requireVerifiedArtifactRoot(ctx, tx, root.RootDigest)
		if err != nil || verified.ManifestDigest != root.ManifestDigest {
			return nil, fmt.Errorf("%w: checkpoint root or manifest is not verified", ErrCaptureMismatch)
		}
	}
	produced := make(map[model.Digest]struct{})
	referenced := make(map[model.Digest]struct{})
	roles := make(map[model.Digest]model.ArtifactRole)
	for _, event := range events {
		if !eventTypeAllowsArtifacts(event.Type()) && len(event.Artifacts()) != 0 {
			return nil, fmt.Errorf("%w: %s forbids Artifact roots", ErrArtifactReference, event.Type())
		}
		eventProduced := make(map[model.Digest]struct{})
		for _, ref := range event.Artifacts() {
			if prior, exists := roles[ref.RootDigest()]; exists && prior != ref.Role() {
				return nil, fmt.Errorf("%w: root has mixed roles in one batch", ErrCaptureMismatch)
			}
			roles[ref.RootDigest()] = ref.Role()
			switch ref.Role() {
			case model.ArtifactProduced:
				if spec.Operation == nil || operation.AgentRunID().IsZero() {
					return nil, fmt.Errorf("%w: produced root lacks operation Run authority", ErrCaptureMismatch)
				}
				if _, ok := captured[ref.RootDigest()]; !ok {
					return nil, fmt.Errorf("%w: produced root is absent from capture checkpoint", ErrCaptureMismatch)
				}
				produced[ref.RootDigest()] = struct{}{}
				eventProduced[ref.RootDigest()] = struct{}{}
			case model.ArtifactReferenced:
				if _, ok := captured[ref.RootDigest()]; ok {
					return nil, fmt.Errorf("%w: checkpoint root cannot be declared referenced", ErrCaptureMismatch)
				}
				if err := requireReusableArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
					return nil, err
				}
				referenced[ref.RootDigest()] = struct{}{}
			default:
				return nil, ErrArtifactReference
			}
		}
		if len(eventProduced) != len(captured) {
			return nil, fmt.Errorf("%w: each expanded Event must cover the exact capture closure", ErrCaptureMismatch)
		}
		for root := range captured {
			if _, ok := eventProduced[root]; !ok {
				return nil, fmt.Errorf("%w: Event omitted a captured root", ErrCaptureMismatch)
			}
		}
		if event.Type() == model.EventReviewDelivered {
			if err := requireExactDeliveredArtifactClosure(ctx, tx, event); err != nil {
				return nil, err
			}
		}
	}
	if len(produced) != len(captured) {
		return nil, fmt.Errorf("%w: produced roots are not the exact capture closure", ErrCaptureMismatch)
	}
	for root := range captured {
		if _, ok := produced[root]; !ok {
			return nil, fmt.Errorf("%w: capture root omitted from Events", ErrCaptureMismatch)
		}
	}
	authorized := spec.AuthorizedReferences
	wantReferences := sortedDigests(referenced)
	if len(authorized) != len(wantReferences) {
		return nil, fmt.Errorf("%w: referenced roots lack exact server authority", ErrArtifactReference)
	}
	for index, root := range authorized {
		if root.IsZero() || root != wantReferences[index] || (index > 0 && authorized[index-1].String() >= root.String()) {
			return nil, fmt.Errorf("%w: reference authority must be uniquely sorted and exact", ErrArtifactReference)
		}
	}
	return capture, nil
}

func eventTypeAllowsArtifacts(eventType model.EventType) bool {
	return eventType == model.EventReviewOffered || eventType == model.EventReviewDeliveryReady ||
		eventType == model.EventReviewDelivered || eventType == model.EventReviewReworkRequested
}

// requireExactDeliveredArtifactClosure prevents a home controller from
// substituting an unrelated reusable root. A delivered Event is the semantic
// receipt for one exact imported delivery.ready Event, so it must reuse that
// source closure byte-for-byte by digest and downgrade every relation to
// referenced rather than claiming producer provenance.
func requireExactDeliveredArtifactClosure(ctx context.Context, tx *sql.Tx, event model.Event) error {
	causes := event.CausedBy()
	if len(causes) != 1 {
		return fmt.Errorf("%w: delivered Event requires exactly one source request", ErrArtifactReference)
	}
	cause := causes[0]
	var sourceType string
	var artifactJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT event_type,artifact_roots_json FROM events WHERE event_id=?
		AND origin_peer_id=? AND origin_epoch=?`, cause.EventID().String(), cause.OriginPeerID().String(),
		cause.OriginEpoch().String()).Scan(&sourceType, &artifactJSON); err != nil {
		return fmt.Errorf("%w: read delivered source closure: %v", ErrArtifactReference, err)
	}
	if model.EventType(sourceType) != model.EventReviewDeliveryReady {
		return fmt.Errorf("%w: delivered source is not review.delivery.ready", ErrArtifactReference)
	}
	sourceRefs, err := parseCanonicalDerivationArtifactRefs(artifactJSON)
	if err != nil {
		return fmt.Errorf("%w: delivered source closure is not canonical", ErrArtifactReference)
	}
	deliveredRefs := event.Artifacts()
	if len(sourceRefs) != len(deliveredRefs) {
		return fmt.Errorf("%w: delivered closure differs from source request", ErrArtifactReference)
	}
	for index, source := range sourceRefs {
		if deliveredRefs[index].Role() != model.ArtifactReferenced ||
			deliveredRefs[index].RootDigest() != source.RootDigest() {
			return fmt.Errorf("%w: delivered closure differs from source request", ErrArtifactReference)
		}
	}
	return nil
}
