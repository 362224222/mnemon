package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	peerInboxSemanticCommitRequestDomain = "mnemon/r5/peer-inbox-semantic-commit-request/2"
	peerInboxSemanticDecisionDomain      = "mnemon/r5/peer-inbox-semantic-decision/1"
	peerInboxSemanticResponseIDDomain    = "mnemon/r5/peer-inbox-semantic-response-id/1"
	peerInboxSemanticHandlingIDDomain    = "mnemon/r5/peer-inbox-semantic-handling-id/1"
)

type peerInboxSemanticEventDecision struct {
	EventDigest       string `json:"event_digest"`
	EventID           string `json:"event_id"`
	OriginEpoch       string `json:"origin_epoch"`
	OriginPeerID      string `json:"origin_peer_id"`
	PublicationDigest string `json:"publication_digest"`
	Source            string `json:"source"`
}

type peerInboxSemanticWorkDecision struct {
	ChannelID        string `json:"channel_id"`
	DeadlineUnixNano int64  `json:"deadline_unix_nano"`
	HomePeerID       string `json:"home_peer_id"`
	InitiatorPeerID  string `json:"initiator_peer_id"`
	Iteration        uint8  `json:"iteration"`
	ReviewerPeerID   string `json:"reviewer_peer_id"`
	RosterRevision   uint64 `json:"roster_revision"`
	State            string `json:"state"`
	StateData        string `json:"state_data"`
	UpdatedAt        string `json:"updated_at"`
	UpdatedByEvent   string `json:"updated_by_event"`
	Version          uint64 `json:"version"`
	WorkID           string `json:"work_id"`
}

type peerInboxSemanticWorkIntentDecision struct {
	ChannelID          string `json:"channel_id"`
	DeadlineUnixNano   int64  `json:"deadline_unix_nano"`
	ExpectedIteration  uint8  `json:"expected_iteration"`
	ExpectedState      string `json:"expected_state"`
	ExpectedVersion    uint64 `json:"expected_version"`
	HomePeerID         string `json:"home_peer_id"`
	InitiatorPeerID    string `json:"initiator_peer_id"`
	NextIteration      uint8  `json:"next_iteration"`
	NextState          string `json:"next_state"`
	NextVersion        uint64 `json:"next_version"`
	ObservedAtUnixNano int64  `json:"observed_at_unix_nano"`
	ResponseOrdinal    uint8  `json:"response_ordinal"`
	ReviewerPeerID     string `json:"reviewer_peer_id"`
	RosterRevision     uint64 `json:"roster_revision"`
	Source             string `json:"source"`
	StateData          string `json:"state_data"`
	WorkID             string `json:"work_id"`
}

type peerInboxSemanticHandlingIntentDecision struct {
	HomePeerID      string `json:"home_peer_id"`
	LocalRole       string `json:"local_role"`
	ResponseOrdinal uint8  `json:"response_ordinal"`
	Source          string `json:"source"`
	SourceEventType string `json:"source_event_type"`
	WorkID          string `json:"work_id"`
}

type peerInboxSemanticSettlementDecision struct {
	Disposition   string `json:"disposition"`
	HomePeerID    string `json:"home_peer_id"`
	SourceEventID string `json:"source_event_id"`
	WorkID        string `json:"work_id"`
}

type peerInboxSemanticResponseIntentDecision struct {
	CauseEventID      string `json:"cause_event_id"`
	CauseOriginEpoch  string `json:"cause_origin_epoch"`
	CauseOriginPeerID string `json:"cause_origin_peer_id"`
	EventType         string `json:"event_type"`
	Payload           string `json:"payload"`
}

type peerInboxSemanticPlanDecision struct {
	Diagnostic  string                                    `json:"diagnostic"`
	Disposition string                                    `json:"disposition"`
	Handling    *peerInboxSemanticHandlingIntentDecision  `json:"handling"`
	InboxStatus string                                    `json:"inbox_status"`
	Responses   []peerInboxSemanticResponseIntentDecision `json:"responses"`
	Settlement  *peerInboxSemanticSettlementDecision      `json:"settlement"`
	Work        *peerInboxSemanticWorkIntentDecision      `json:"work"`
}

type peerInboxSemanticDecision struct {
	Attempt        uint32                           `json:"attempt"`
	CausalEvents   []peerInboxSemanticEventDecision `json:"causal_events"`
	CommittedAt    string                           `json:"committed_at"`
	DecidedAt      string                           `json:"decided_at"`
	DecisionSeed   string                           `json:"decision_seed"`
	Domain         string                           `json:"domain"`
	ImportedEvent  peerInboxSemanticEventDecision   `json:"imported_event"`
	InboxID        string                           `json:"inbox_id"`
	Plan           peerInboxSemanticPlanDecision    `json:"plan"`
	PriorWork      *peerInboxSemanticWorkDecision   `json:"prior_work"`
	ReceiptEventID string                           `json:"receipt_event_id"`
	RequestDigest  string                           `json:"request_digest"`
	Responses      []peerInboxSemanticEventDecision `json:"responses"`
	SchemaVersion  int                              `json:"schema_version"`
	SnapshotDigest string                           `json:"snapshot_digest"`
	Status         string                           `json:"status"`
}

type peerInboxSemanticAdmissionRequest struct {
	ActiveAssetRevision       string `json:"active_asset_revision"`
	ChannelID                 string `json:"channel_id"`
	Count                     uint8  `json:"count"`
	CredentialHash            string `json:"credential_hash"`
	FirstChannelSeq           uint64 `json:"first_channel_seq"`
	FirstOriginSeq            uint64 `json:"first_origin_seq"`
	HandlingBudget            string `json:"handling_budget"`
	Host                      string `json:"host"`
	NodeCreatedAt             string `json:"node_created_at"`
	NodeUpdatedAt             string `json:"node_updated_at"`
	OriginEpoch               string `json:"origin_epoch"`
	OriginMemberDigest        string `json:"origin_member_digest"`
	OriginMemberRevision      uint64 `json:"origin_member_revision"`
	PeerID                    string `json:"peer_id"`
	Principal                 string `json:"principal"`
	ProfileAssetRevision      string `json:"profile_asset_revision"`
	ProfileCreatedAt          string `json:"profile_created_at"`
	ProfileEnabled            bool   `json:"profile_enabled"`
	ProfileID                 string `json:"profile_id"`
	ProfileUpdatedAt          string `json:"profile_updated_at"`
	PublicationRosterDigest   string `json:"publication_roster_digest"`
	PublicationRosterRevision uint64 `json:"publication_roster_revision"`
	Runtime                   string `json:"runtime"`
	WorkspaceRoot             string `json:"workspace_root"`
}

func peerInboxSemanticPlanProjection(plan PeerInboxSemanticPlan) peerInboxSemanticPlanDecision {
	projection := peerInboxSemanticPlanDecision{Diagnostic: plan.Diagnostic(),
		Disposition: string(plan.Disposition()), InboxStatus: string(plan.InboxStatus()),
		Responses: make([]peerInboxSemanticResponseIntentDecision, 0, len(plan.Responses()))}
	if work, ok := plan.Work(); ok {
		participants := work.Participants()
		projection.Work = &peerInboxSemanticWorkIntentDecision{
			ChannelID: work.ChannelID().String(), DeadlineUnixNano: work.DeadlineUnixNano(),
			ExpectedIteration: work.ExpectedIteration(), ExpectedState: string(work.ExpectedState()),
			ExpectedVersion: work.ExpectedVersion(), HomePeerID: work.WorkRef().HomePeerID().String(),
			InitiatorPeerID: participants.InitiatorPeerID().String(), NextIteration: work.NextIteration(),
			NextState: string(work.NextState()), NextVersion: work.NextVersion(),
			ObservedAtUnixNano: work.ObservedAtUnixNano(), ResponseOrdinal: work.ResponseOrdinal(),
			ReviewerPeerID: participants.ReviewerPeerID().String(),
			RosterRevision: participants.RosterRevision(), Source: string(work.Source()),
			StateData: work.StateData().String(), WorkID: work.WorkRef().WorkID().String(),
		}
	}
	if handling, ok := plan.Handling(); ok {
		projection.Handling = &peerInboxSemanticHandlingIntentDecision{
			HomePeerID: handling.WorkRef().HomePeerID().String(), LocalRole: string(handling.LocalRole()),
			ResponseOrdinal: handling.ResponseOrdinal(), Source: string(handling.Source()),
			SourceEventType: string(handling.SourceEventType()), WorkID: handling.WorkRef().WorkID().String(),
		}
	}
	if settlement, ok := plan.Settlement(); ok {
		projection.Settlement = &peerInboxSemanticSettlementDecision{
			Disposition: string(settlement.Disposition()), HomePeerID: settlement.WorkRef().HomePeerID().String(),
			SourceEventID: settlement.SourceEventID().String(), WorkID: settlement.WorkRef().WorkID().String(),
		}
	}
	for _, response := range plan.Responses() {
		cause := response.Cause()
		projection.Responses = append(projection.Responses, peerInboxSemanticResponseIntentDecision{
			CauseEventID: cause.EventID().String(), CauseOriginEpoch: cause.OriginEpoch().String(),
			CauseOriginPeerID: cause.OriginPeerID().String(), EventType: string(response.EventType()),
			Payload: response.Payload().String(),
		})
	}
	return projection
}

func peerInboxSemanticWorkProjection(work model.ReviewWork) *peerInboxSemanticWorkDecision {
	participants := work.Participants()
	return &peerInboxSemanticWorkDecision{
		ChannelID: work.ChannelID().String(), DeadlineUnixNano: work.DeadlineUnixNano(),
		HomePeerID: work.Ref().HomePeerID().String(), InitiatorPeerID: participants.InitiatorPeerID().String(),
		Iteration: work.Iteration(), ReviewerPeerID: participants.ReviewerPeerID().String(),
		RosterRevision: participants.RosterRevision(), State: string(work.State()),
		StateData: work.StateData().String(), UpdatedAt: storeTime(work.UpdatedAt()),
		UpdatedByEvent: work.UpdatedBy().String(), Version: work.Version(), WorkID: work.Ref().WorkID().String(),
	}
}

func peerInboxSemanticWorkFromDecision(value *peerInboxSemanticWorkDecision) (model.ReviewWork, bool, error) {
	if value == nil {
		return model.ReviewWork{}, false, nil
	}
	channel, channelErr := model.ParseChannelID(value.ChannelID)
	home, homeErr := model.ParsePeerID(value.HomePeerID)
	workID, workErr := model.ParseWorkID(value.WorkID)
	initiator, initiatorErr := model.ParsePeerID(value.InitiatorPeerID)
	reviewer, reviewerErr := model.ParsePeerID(value.ReviewerPeerID)
	updatedBy, updatedByErr := model.ParseEventID(value.UpdatedByEvent)
	updatedAt, updatedAtErr := parseCanonicalStoreTime(value.UpdatedAt)
	stateData, stateErr := model.NewJSON([]byte(value.StateData))
	ref, refErr := model.NewWorkRef(home, workID)
	participants, participantsErr := model.NewParticipantSnapshot(channel, value.RosterRevision,
		initiator, reviewer)
	if channelErr != nil || homeErr != nil || workErr != nil || initiatorErr != nil || reviewerErr != nil ||
		updatedByErr != nil || updatedAtErr != nil || stateErr != nil || refErr != nil || participantsErr != nil {
		return model.ReviewWork{}, false, fmt.Errorf("%w: malformed prior Work projection",
			ErrPeerInboxSemanticInvariant)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: channel,
		Participants: participants, Version: value.Version, Iteration: value.Iteration,
		DeadlineUnixNano: value.DeadlineUnixNano, State: model.WorkState(value.State), StateData: stateData,
		UpdatedBy: updatedBy, UpdatedAt: updatedAt})
	if err != nil {
		return model.ReviewWork{}, false, fmt.Errorf("%w: prior Work projection: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return work, true, nil
}

func peerInboxSemanticEventProjection(event model.Event,
	publicationDigest model.Digest,
) peerInboxSemanticEventDecision {
	key := event.Key()
	return peerInboxSemanticEventDecision{EventDigest: event.Digest().String(),
		EventID: event.ID().String(), OriginEpoch: key.OriginEpoch().String(),
		OriginPeerID: key.OriginPeerID().String(), PublicationDigest: publicationDigest.String(),
		Source: string(event.Source())}
}

func peerInboxSemanticCommitRequestDigest(fence PeerInboxSemanticFence, plan PeerInboxSemanticPlan,
	scope LocalAdmissionScope, responses []model.SignedPublication,
) (model.Digest, error) {
	wires := make([]string, len(responses))
	for index, response := range responses {
		if response.Event().ID().IsZero() || response.WireJSON().IsZero() {
			return model.Digest{}, fmt.Errorf("%w: incomplete response publication",
				ErrPeerInboxSemanticInput)
		}
		wires[index] = response.WireJSON().String()
	}
	var admission *peerInboxSemanticAdmissionRequest
	if scope.Count() != 0 {
		node, profile := scope.Node(), scope.Profile()
		admission = &peerInboxSemanticAdmissionRequest{
			ActiveAssetRevision: node.ActiveAssetRevision(), ChannelID: scope.ChannelID().String(),
			Count: scope.Count(), CredentialHash: profile.CredentialHash().String(),
			FirstChannelSeq: scope.FirstChannelSequence(), FirstOriginSeq: scope.FirstOriginSequence(),
			HandlingBudget: profile.HandlingBudget().String(), Host: string(profile.Host()),
			NodeCreatedAt: storeTime(node.CreatedAt()), NodeUpdatedAt: storeTime(node.UpdatedAt()),
			OriginEpoch: node.OriginEpoch().String(), OriginMemberDigest: scope.OriginMember().Digest().String(),
			OriginMemberRevision: scope.OriginMember().Revision(), PeerID: node.PeerID().String(),
			Principal: profile.Principal(), ProfileAssetRevision: profile.ActiveAssetRevision(),
			ProfileCreatedAt: storeTime(profile.CreatedAt()),
			ProfileEnabled:   profile.Enabled(), ProfileID: profile.ID().String(),
			ProfileUpdatedAt:          storeTime(profile.UpdatedAt()),
			PublicationRosterDigest:   scope.PublicationRoster().Digest().String(),
			PublicationRosterRevision: scope.PublicationRoster().Revision(), Runtime: string(profile.Runtime()),
			WorkspaceRoot: profile.WorkspaceRoot(),
		}
	}
	canonical, err := model.JSONFrom(struct {
		Admission      *peerInboxSemanticAdmissionRequest `json:"admission"`
		Attempt        uint32                             `json:"attempt"`
		DecisionSeed   model.Digest                       `json:"decision_seed"`
		Domain         string                             `json:"domain"`
		InboxID        model.InboxID                      `json:"inbox_id"`
		LeaseOwner     string                             `json:"lease_owner"`
		LeaseUntil     string                             `json:"lease_until"`
		DecisionAt     string                             `json:"decision_at"`
		Plan           peerInboxSemanticPlanDecision      `json:"plan"`
		ResponseWires  []string                           `json:"response_wires"`
		SnapshotDigest model.Digest                       `json:"snapshot_digest"`
	}{admission, fence.attempt, peerInboxSemanticDecisionSeed(fence.semanticNonce),
		peerInboxSemanticCommitRequestDomain, fence.inboxID, fence.leaseOwner,
		storeTime(fence.leaseUntil), storeTime(plan.DecisionAt()),
		peerInboxSemanticPlanProjection(plan), wires, fence.snapshotDigest})
	if err != nil {
		return model.Digest{}, fmt.Errorf("%w: canonical semantic commit request: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return model.Sum(canonical.Bytes()), nil
}

func encodePeerInboxSemanticDecision(value peerInboxSemanticDecision) (model.JSON, error) {
	canonical, err := model.JSONFrom(value)
	if err != nil || canonical.IsZero() || len(canonical.Bytes()) > 65536 {
		return model.JSON{}, fmt.Errorf("%w: encode semantic decision: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return canonical, nil
}

func decodePeerInboxSemanticDecision(raw []byte) (peerInboxSemanticDecision, model.JSON, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return peerInboxSemanticDecision{}, model.JSON{}, fmt.Errorf("%w: decision is not canonical JSON",
			ErrPeerInboxSemanticInvariant)
	}
	var value peerInboxSemanticDecision
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return peerInboxSemanticDecision{}, model.JSON{}, fmt.Errorf("%w: decode semantic decision: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return peerInboxSemanticDecision{}, model.JSON{}, fmt.Errorf("%w: trailing semantic decision value",
			ErrPeerInboxSemanticInvariant)
	}
	rebuilt, err := encodePeerInboxSemanticDecision(value)
	if err != nil || !bytes.Equal(rebuilt.Bytes(), raw) || value.Domain != peerInboxSemanticDecisionDomain ||
		value.SchemaVersion != 1 || value.Attempt == 0 || value.CausalEvents == nil ||
		value.Responses == nil || value.Plan.Responses == nil ||
		!validPeerInboxSemanticDecisionShape(value) {
		return peerInboxSemanticDecision{}, model.JSON{}, fmt.Errorf("%w: semantic decision shape differs",
			ErrPeerInboxSemanticInvariant)
	}
	return value, canonical, nil
}

func validPeerInboxSemanticDecisionShape(value peerInboxSemanticDecision) bool {
	decidedAt, decidedErr := parseCanonicalStoreTime(value.DecidedAt)
	committedAt, committedErr := parseCanonicalStoreTime(value.CommittedAt)
	_, inboxErr := model.ParseInboxID(value.InboxID)
	_, seedErr := model.ParseDigest(value.DecisionSeed)
	_, requestErr := model.ParseDigest(value.RequestDigest)
	_, snapshotErr := model.ParseDigest(value.SnapshotDigest)
	if decidedErr != nil || committedErr != nil || committedAt.Before(decidedAt) ||
		inboxErr != nil || seedErr != nil || requestErr != nil || snapshotErr != nil ||
		!validPeerInboxSemanticEventDecision(value.ImportedEvent, model.EventSourceImported) ||
		len(value.Responses) > 2 || len(value.Responses) != len(value.Plan.Responses) {
		return false
	}
	disposition := PeerInboxSemanticDisposition(value.Plan.Disposition)
	wantStatus := map[PeerInboxSemanticDisposition]model.InboxStatus{
		PeerInboxSemanticApply:       model.InboxAccepted,
		PeerInboxSemanticReceiptOnly: model.InboxAccepted,
		PeerInboxSemanticReject:      model.InboxRejected,
		PeerInboxSemanticConflict:    model.InboxConflicted,
	}[disposition]
	status := model.InboxStatus(value.Status)
	if !disposition.Valid() || wantStatus == "" ||
		status != wantStatus || value.Plan.InboxStatus != value.Status ||
		(status == model.InboxAccepted) != (value.Plan.Diagnostic == "") ||
		(status != model.InboxAccepted &&
			(value.Plan.Diagnostic == "" || !validPublicationDiagnostic(value.Plan.Diagnostic))) {
		return false
	}
	seen := map[string]struct{}{value.ImportedEvent.EventID: {}}
	for _, evidence := range value.CausalEvents {
		if !validPeerInboxSemanticEventDecision(evidence, "") {
			return false
		}
	}
	for index, evidence := range value.Responses {
		if !validPeerInboxSemanticEventDecision(evidence, model.EventSourceLocal) {
			return false
		}
		if _, duplicate := seen[evidence.EventID]; duplicate {
			return false
		}
		seen[evidence.EventID] = struct{}{}
		intent := value.Plan.Responses[index]
		if !validPeerInboxSemanticResponseIntentDecision(intent) {
			return false
		}
	}
	if len(value.Responses) == 0 {
		return value.ReceiptEventID == ""
	}
	return value.ReceiptEventID == value.Responses[len(value.Responses)-1].EventID
}

func validPeerInboxSemanticEventDecision(value peerInboxSemanticEventDecision,
	wantSource model.EventSource,
) bool {
	_, eventErr := model.ParseEventID(value.EventID)
	_, eventDigestErr := model.ParseDigest(value.EventDigest)
	_, epochErr := model.ParseOriginEpoch(value.OriginEpoch)
	_, peerErr := model.ParsePeerID(value.OriginPeerID)
	_, publicationErr := model.ParseDigest(value.PublicationDigest)
	source := model.EventSource(value.Source)
	return eventErr == nil && eventDigestErr == nil && epochErr == nil && peerErr == nil &&
		publicationErr == nil && source.Valid() && (wantSource == "" || source == wantSource)
}

func validPeerInboxSemanticResponseIntentDecision(
	value peerInboxSemanticResponseIntentDecision,
) bool {
	_, eventErr := model.ParseEventID(value.CauseEventID)
	_, epochErr := model.ParseOriginEpoch(value.CauseOriginEpoch)
	_, peerErr := model.ParsePeerID(value.CauseOriginPeerID)
	payload, payloadErr := model.NewJSON([]byte(value.Payload))
	return eventErr == nil && epochErr == nil && peerErr == nil &&
		model.EventType(value.EventType).Valid() && payloadErr == nil &&
		payload.String() == value.Payload
}

func equalPeerInboxSemanticPlan(left, right peerInboxSemanticPlanDecision) bool {
	leftJSON, leftErr := model.JSONFrom(left)
	rightJSON, rightErr := model.JSONFrom(right)
	return leftErr == nil && rightErr == nil && leftJSON.String() == rightJSON.String()
}
