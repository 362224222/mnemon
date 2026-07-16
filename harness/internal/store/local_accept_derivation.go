package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func prepareLocalDerivations(ctx context.Context, tx *sql.Tx, operation model.Operation,
	spec LocalAcceptanceSpec, events []model.Event,
) ([]model.WorkDerivation, model.ReviewWork, error) {
	if spec.Operation == nil {
		if spec.Derivation != nil {
			return nil, model.ReviewWork{}, errors.New("controller acceptance cannot create Work derivations")
		}
		return nil, model.ReviewWork{}, nil
	}
	_, contextBound := operation.ContextHash()
	requires := operation.Kind() == model.OperationTeamworkOffer && contextBound
	if requires != (spec.Derivation != nil) {
		return nil, model.ReviewWork{}, errors.New("context-bound offer requires exactly one derivation parent")
	}
	if !requires {
		return nil, model.ReviewWork{}, nil
	}
	expected := *spec.Derivation
	parent, err := readReviewWork(ctx, tx, expected.WorkRef)
	if err != nil || parent.ChannelID() != expected.ChannelID || parent.Version() != expected.ExpectedVersion ||
		parent.UpdatedBy() != expected.UpdatedByEvent || (parent.State() != model.WorkActive && parent.State() != model.WorkRework) {
		return nil, model.ReviewWork{}, fmt.Errorf("commit local acceptance: derivation parent changed: %w", err)
	}
	var sourceOrigin, sourceEpoch string
	err = tx.QueryRowContext(ctx, `SELECT origin_peer_id,origin_epoch FROM events WHERE event_id=? AND channel_id=?
		AND origin_peer_id=? AND work_home_peer_id=? AND work_id=?`, expected.UpdatedByEvent.String(),
		expected.ChannelID.String(), expected.WorkRef.HomePeerID().String(), expected.WorkRef.HomePeerID().String(),
		expected.WorkRef.WorkID().String()).Scan(&sourceOrigin, &sourceEpoch)
	if err != nil {
		return nil, model.ReviewWork{}, errors.New("commit local acceptance: derivation source Event is not exact parent update")
	}
	origin, err := model.ParsePeerID(sourceOrigin)
	if err != nil {
		return nil, model.ReviewWork{}, err
	}
	epoch, err := model.ParseOriginEpoch(sourceEpoch)
	if err != nil {
		return nil, model.ReviewWork{}, err
	}
	sourceKey, err := model.NewEventKey(origin, epoch, expected.UpdatedByEvent)
	if err != nil {
		return nil, model.ReviewWork{}, err
	}
	result := make([]model.WorkDerivation, len(spec.Items))
	var childChannel model.ChannelID
	seen := make(map[model.WorkRef]struct{}, len(spec.Items))
	for index, item := range spec.Items {
		if item.Work == nil || item.Work.ExpectedVersion != 0 || events[index].Type() != model.EventReviewOffered ||
			len(events[index].CausedBy()) != 1 || events[index].CausedBy()[0] != sourceKey {
			return nil, model.ReviewWork{}, errors.New("commit local acceptance: derivation child lacks exact parent causality")
		}
		child := item.Work.Work
		if index == 0 {
			childChannel = child.ChannelID()
		}
		if child.ChannelID() != childChannel || child.Ref().HomePeerID() != parent.Participants().ReviewerPeerID() ||
			child.Ref() == parent.Ref() {
			return nil, model.ReviewWork{}, errors.New("commit local acceptance: derivation child scope mismatch")
		}
		if _, exists := seen[child.Ref()]; exists {
			return nil, model.ReviewWork{}, errors.New("commit local acceptance: duplicate derivation child")
		}
		seen[child.Ref()] = struct{}{}
		result[index], err = model.NewWorkDerivation(model.WorkDerivationSpec{OperationID: operation.ID(),
			ChildOrdinal: uint8(index), ChildChannelID: child.ChannelID(), Child: child.Ref(),
			ParentChannelID: parent.ChannelID(), Parent: parent.Ref(), ParentVersion: parent.Version(),
			ParentEventID: parent.UpdatedBy(), CreatedAt: events[0].AcceptedAt()})
		if err != nil {
			return nil, model.ReviewWork{}, err
		}
	}
	return result, parent, nil
}
