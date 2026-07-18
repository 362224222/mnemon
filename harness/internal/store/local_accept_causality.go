package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateLocalCausality(ctx context.Context, tx *sql.Tx, event model.Event) error {
	for _, cause := range event.CausedBy() {
		var present int
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id=?
			AND origin_peer_id=? AND origin_epoch=?)`, cause.EventID().String(),
			cause.OriginPeerID().String(), cause.OriginEpoch().String()).Scan(&present)
		if err != nil {
			return fmt.Errorf("commit local acceptance: caused_by EventKey is not durable: %w", err)
		}
		if present != 1 {
			return fmt.Errorf("%w: caused_by EventKey %s is not durable", ErrAdmissionConflict, cause.EventID())
		}
	}
	return nil
}

// validateLocalCausalSemantics closes the authority gap left by mere EventKey
// existence. Every context action cites the exact Event that established its
// current Work view, and every home controller decision cites the exact remote
// request it decides. Contextless offers have no causal authority; the
// context-bound offer path is checked against its frozen derivation parent by
// prepareLocalDerivations.
func validateLocalCausalSemantics(ctx context.Context, tx *sql.Tx, operation model.Operation,
	event model.Event,
) error {
	if ctx == nil || tx == nil {
		return errors.New("commit local acceptance: nil causality context or transaction")
	}
	if event.Type() == model.EventReviewOffered {
		_, contextBound := operation.ContextHash()
		if contextBound {
			if len(event.CausedBy()) != 1 {
				return causalConflict("context-bound offer must cite exactly one frozen parent Event")
			}
			return nil
		}
		if len(event.CausedBy()) != 0 {
			return causalConflict("contextless offer cannot claim source causality")
		}
		return nil
	}

	current, err := readReviewWork(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return fmt.Errorf("commit local acceptance: causal current Work: %w", err)
	}
	if current.ChannelID() != event.Scope().ChannelID() {
		return causalConflict("Event and current Work use different Channels")
	}

	switch event.Type() {
	case model.EventReviewAcceptRequested, model.EventReviewDeclineRequested,
		model.EventReviewDeliveryReady, model.EventReviewReworkRequested,
		model.EventReviewClosed, model.EventReviewCancelled:
		return requireCurrentWorkCause(ctx, tx, event, current)
	case model.EventReviewAccepted, model.EventReviewAcceptRejected,
		model.EventReviewDelivered, model.EventReviewDeclined:
		return requireControllerRequestCause(ctx, tx, event, current)
	case model.EventReviewExpired:
		if !current.State().DeadlineEligible() || event.AcceptedAt().UnixNano() < current.DeadlineUnixNano() {
			return causalConflict("expiry is not eligible at its trusted accepted time")
		}
		return requireCurrentWorkCause(ctx, tx, event, current)
	case model.EventReviewOutcome:
		return requireFallbackOutcomeCause(ctx, tx, event, current)
	default:
		return causalConflict("Event type is outside the closed Teamwork causality policy")
	}
}

type durableCausalEvent struct {
	key         model.EventKey
	channelID   model.ChannelID
	workRef     model.WorkRef
	origin      model.PeerID
	eventType   model.EventType
	source      model.EventSource
	audience    []model.PeerID
	payloadJSON []byte
}

func requireCurrentWorkCause(ctx context.Context, tx *sql.Tx, event model.Event,
	current model.ReviewWork,
) error {
	source, err := readSingleDurableCause(ctx, tx, event)
	if err != nil {
		return err
	}
	if source.key.EventID() != current.UpdatedBy() || source.channelID != current.ChannelID() ||
		source.workRef != current.Ref() || source.eventType != updateEventType(current.State()) {
		return causalConflict("Event does not cite the exact current Work update")
	}
	return nil
}

func requireControllerRequestCause(ctx context.Context, tx *sql.Tx, event model.Event,
	current model.ReviewWork,
) error {
	source, err := readSingleDurableCause(ctx, tx, event)
	if err != nil {
		return err
	}
	want := map[model.EventType]model.EventType{
		model.EventReviewAccepted:       model.EventReviewAcceptRequested,
		model.EventReviewAcceptRejected: model.EventReviewAcceptRequested,
		model.EventReviewDelivered:      model.EventReviewDeliveryReady,
		model.EventReviewDeclined:       model.EventReviewDeclineRequested,
	}[event.Type()]
	home := current.Ref().HomePeerID()
	if source.channelID != current.ChannelID() || source.workRef != current.Ref() ||
		source.source != model.EventSourceImported || source.origin != current.Participants().ReviewerPeerID() ||
		source.eventType != want || !isExactPeerAudience(source.audience, home) {
		return causalConflict("controller decision does not cite its exact imported participant request")
	}
	if event.Type() == model.EventReviewAcceptRejected {
		return requireReceiptSourceEcho(event, source, current)
	}
	version, iteration, err := decodeCausalVersion(source.payloadJSON)
	if err != nil || version != current.Version() || iteration != current.Iteration() {
		return causalConflict("participant request does not bind the current Work version and iteration")
	}
	return nil
}

func requireFallbackOutcomeCause(ctx context.Context, tx *sql.Tx, event model.Event,
	current model.ReviewWork,
) error {
	source, err := readSingleDurableCause(ctx, tx, event)
	if err != nil {
		return err
	}
	local := event.Scope().OriginPeerID()
	remote := current.Ref().HomePeerID()
	if local == remote {
		remote = current.Participants().ReviewerPeerID()
	}
	if source.channelID != current.ChannelID() || source.workRef != current.Ref() ||
		source.source != model.EventSourceImported || source.origin != remote ||
		source.eventType == model.EventReviewOutcome || !isExactPeerAudience(source.audience, local) {
		return causalConflict("fallback outcome does not cite one exact imported peer Event")
	}
	return requireReceiptSourceEcho(event, source, current)
}

// requireReceiptSourceEcho is the only path that permits a local controller
// Event to carry an older Work version. Receipt-only Events must echo the
// exact imported source request/fact they decide; the durable Work is used
// only as a monotonic upper bound, never as caller-selected stale authority.
func requireReceiptSourceEcho(event model.Event, source durableCausalEvent,
	current model.ReviewWork,
) error {
	receipt, err := decodeClosedEventPayload(event)
	if err != nil {
		return causalConflict("receipt Event payload is outside the closed Teamwork schema")
	}
	sourceVersion, sourceIteration, err := decodeCausalVersion(source.payloadJSON)
	if err != nil {
		return causalConflict("receipt source lacks a valid Work version and iteration")
	}
	if receipt.WorkVersion != sourceVersion || receipt.Iteration != sourceIteration {
		return causalConflict("receipt payload does not echo its exact imported source version and iteration")
	}
	if event.Type() == model.EventReviewOutcome && receipt.DecisionRef != source.key.EventID().String() {
		return causalConflict("outcome decision_ref does not identify its exact imported source Event")
	}
	if event.Type() == model.EventReviewAcceptRejected && (sourceVersion != 1 || sourceIteration != 1) {
		return causalConflict("accept rejection does not decide an initial offered Work request")
	}
	if !receiptVersionAtOrBeforeCurrent(current, sourceVersion, sourceIteration) {
		return causalConflict("receipt source is ahead of or inconsistent with current Work")
	}
	return nil
}

func receiptVersionAtOrBeforeCurrent(current model.ReviewWork, version uint64, iteration uint8) bool {
	if current.Ref().IsZero() || !validReceiptSourceTuple(version, iteration) ||
		version > current.Version() {
		return false
	}
	if version == current.Version() {
		return iteration == current.Iteration()
	}
	return true
}

// A receipt source describes the Work before an Event is applied. In the
// closed two-iteration state machine, iteration one can source Events only at
// versions 1..3 and iteration two only at versions 4..5. Versions 4/i1 and
// 6/i2 can be terminal result Works, but can never authorize a later source
// Event from that Work.
func validReceiptSourceTuple(version uint64, iteration uint8) bool {
	switch iteration {
	case 1:
		return version >= 1 && version <= 3
	case 2:
		return version >= 4 && version <= 5
	default:
		return false
	}
}

func readSingleDurableCause(ctx context.Context, tx *sql.Tx, event model.Event) (durableCausalEvent, error) {
	causes := event.CausedBy()
	if len(causes) != 1 {
		return durableCausalEvent{}, causalConflict("Event must cite exactly one source Event")
	}
	cause := causes[0]
	var channelText, homeText, workText, originText, epochText, eventTypeText, sourceText string
	var audienceJSON, payloadJSON []byte
	err := tx.QueryRowContext(ctx, `SELECT channel_id,work_home_peer_id,work_id,origin_peer_id,
		origin_epoch,event_type,source,audience_json,payload_json FROM events WHERE event_id=?
		AND origin_peer_id=? AND origin_epoch=?`, cause.EventID().String(), cause.OriginPeerID().String(),
		cause.OriginEpoch().String()).Scan(&channelText, &homeText, &workText, &originText, &epochText,
		&eventTypeText, &sourceText, &audienceJSON, &payloadJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return durableCausalEvent{}, causalConflict("source EventKey is not durable")
	}
	if err != nil {
		return durableCausalEvent{}, fmt.Errorf("commit local acceptance: read causal Event: %w", err)
	}
	channel, err := model.ParseChannelID(channelText)
	if err != nil {
		return durableCausalEvent{}, causalConflict("source Event has invalid Channel identity")
	}
	home, err := model.ParsePeerID(homeText)
	if err != nil {
		return durableCausalEvent{}, causalConflict("source Event has invalid Work home")
	}
	workID, err := model.ParseWorkID(workText)
	if err != nil {
		return durableCausalEvent{}, causalConflict("source Event has invalid Work identity")
	}
	work, err := model.NewWorkRef(home, workID)
	if err != nil {
		return durableCausalEvent{}, causalConflict("source Event has invalid Work reference")
	}
	origin, err := model.ParsePeerID(originText)
	if err != nil || origin != cause.OriginPeerID() {
		return durableCausalEvent{}, causalConflict("source Event origin does not match its EventKey")
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil || epoch != cause.OriginEpoch() {
		return durableCausalEvent{}, causalConflict("source Event epoch does not match its EventKey")
	}
	eventType := model.EventType(eventTypeText)
	source := model.EventSource(sourceText)
	if !eventType.Valid() || !source.Valid() {
		return durableCausalEvent{}, causalConflict("source Event has invalid closed enums")
	}
	audience, err := decodeCausalAudience(audienceJSON)
	if err != nil {
		return durableCausalEvent{}, causalConflict("source Event has invalid audience")
	}
	return durableCausalEvent{cause, channel, work, origin, eventType, source,
		audience, append([]byte(nil), payloadJSON...)}, nil
}

func decodeCausalAudience(raw []byte) ([]model.PeerID, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return nil, errors.New("invalid audience JSON")
	}
	var encoded []string
	if err := json.Unmarshal(raw, &encoded); err != nil || encoded == nil {
		return nil, errors.New("invalid audience JSON")
	}
	peers := make([]model.PeerID, len(encoded))
	for index, value := range encoded {
		peer, err := model.ParsePeerID(value)
		if err != nil {
			return nil, err
		}
		peers[index] = peer
	}
	audience, err := model.NewAudience(peers)
	if err != nil {
		return nil, err
	}
	rebuilt, err := model.JSONFrom(audience)
	if err != nil || !bytes.Equal(rebuilt.Bytes(), raw) {
		return nil, errors.New("noncanonical audience JSON")
	}
	return audience.Peers(), nil
}

func decodeCausalVersion(raw []byte) (uint64, uint8, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return 0, 0, errors.New("invalid Work version payload")
	}
	var value versionPayload
	if err := json.Unmarshal(raw, &value); err != nil || value.WorkVersion == 0 ||
		value.Iteration < 1 || value.Iteration > 2 {
		return 0, 0, errors.New("invalid Work version payload")
	}
	return value.WorkVersion, value.Iteration, nil
}

func isExactPeerAudience(peers []model.PeerID, peer model.PeerID) bool {
	return len(peers) == 1 && peers[0] == peer
}

func updateEventType(state model.WorkState) model.EventType {
	switch state {
	case model.WorkOffered:
		return model.EventReviewOffered
	case model.WorkActive:
		return model.EventReviewAccepted
	case model.WorkDelivered:
		return model.EventReviewDelivered
	case model.WorkRework:
		return model.EventReviewReworkRequested
	case model.WorkClosed:
		return model.EventReviewClosed
	case model.WorkDeclined:
		return model.EventReviewDeclined
	case model.WorkExpired:
		return model.EventReviewExpired
	case model.WorkCancelled:
		return model.EventReviewCancelled
	default:
		return ""
	}
}

func causalConflict(detail string) error {
	return fmt.Errorf("%w: %s", ErrAdmissionConflict, detail)
}
