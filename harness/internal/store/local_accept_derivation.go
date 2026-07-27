package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func prepareLocalDerivation(ctx context.Context, tx *sql.Tx, operation model.Operation,
	spec LocalAcceptanceSpec, events []model.Event,
) (model.WorkDerivation, model.ReviewWork, error) {
	if spec.Operation == nil {
		if spec.Derivation != nil {
			return model.WorkDerivation{}, model.ReviewWork{}, errors.New("controller acceptance cannot create Work derivations")
		}
		return model.WorkDerivation{}, model.ReviewWork{}, nil
	}
	_, contextBound := operation.ContextHash()
	requires := operation.Kind() == model.OperationTeamworkOffer && contextBound
	if requires != (spec.Derivation != nil) {
		return model.WorkDerivation{}, model.ReviewWork{}, errors.New("context-bound offer requires exactly one derivation parent")
	}
	if !requires {
		return model.WorkDerivation{}, model.ReviewWork{}, nil
	}
	if len(spec.Items) != 1 || len(events) != 1 {
		return model.WorkDerivation{}, model.ReviewWork{},
			errors.New("context-bound offer requires exactly one derivation child")
	}
	parent, sourceKey, err := readLocalDerivationParentAuthority(ctx, tx, *spec.Derivation)
	if err != nil {
		return model.WorkDerivation{}, model.ReviewWork{}, err
	}
	item, event := spec.Items[0], events[0]
	if item.Work == nil || item.Work.ExpectedVersion != 0 || event.Type() != model.EventReviewOffered ||
		len(event.CausedBy()) != 1 || event.CausedBy()[0] != sourceKey {
		return model.WorkDerivation{}, model.ReviewWork{},
			errors.New("commit local acceptance: derivation child lacks exact parent causality")
	}
	child := item.Work.Work
	if child.Ref().HomePeerID() != parent.Participants().ReviewerPeerID() || child.Ref() == parent.Ref() {
		return model.WorkDerivation{}, model.ReviewWork{},
			errors.New("commit local acceptance: derivation child scope mismatch")
	}
	result, err := model.NewWorkDerivation(model.WorkDerivationSpec{OperationID: operation.ID(),
		ChildChannelID: child.ChannelID(), Child: child.Ref(),
		ParentChannelID: parent.ChannelID(), Parent: parent.Ref(), ParentVersion: parent.Version(),
		ParentEventID: parent.UpdatedBy(), CreatedAt: event.AcceptedAt()})
	if err != nil {
		return model.WorkDerivation{}, model.ReviewWork{}, err
	}
	return result, parent, nil
}

func readLocalDerivationParentAuthority(ctx context.Context, tx *sql.Tx,
	expected LocalDerivationParent,
) (model.ReviewWork, model.EventKey, error) {
	parent, err := readReviewWork(ctx, tx, expected.WorkRef)
	if err != nil {
		return model.ReviewWork{}, model.EventKey{},
			fmt.Errorf("commit local acceptance: derivation parent: %w", err)
	}
	if parent.ChannelID() != expected.ChannelID || parent.Version() != expected.ExpectedVersion ||
		parent.UpdatedBy() != expected.UpdatedByEvent ||
		(parent.State() != model.WorkActive && parent.State() != model.WorkRework) {
		return model.ReviewWork{}, model.EventKey{},
			errors.New("commit local acceptance: derivation parent changed")
	}
	var sourceOrigin, sourceEpoch string
	err = tx.QueryRowContext(ctx, `SELECT origin_peer_id,origin_epoch FROM events WHERE event_id=? AND channel_id=?
		AND origin_peer_id=? AND work_home_peer_id=? AND work_id=?`, expected.UpdatedByEvent.String(),
		expected.ChannelID.String(), expected.WorkRef.HomePeerID().String(), expected.WorkRef.HomePeerID().String(),
		expected.WorkRef.WorkID().String()).Scan(&sourceOrigin, &sourceEpoch)
	if err != nil {
		return model.ReviewWork{}, model.EventKey{},
			errors.New("commit local acceptance: derivation source Event is not exact parent update")
	}
	origin, err := model.ParsePeerID(sourceOrigin)
	if err != nil {
		return model.ReviewWork{}, model.EventKey{}, err
	}
	epoch, err := model.ParseOriginEpoch(sourceEpoch)
	if err != nil {
		return model.ReviewWork{}, model.EventKey{}, err
	}
	sourceKey, err := model.NewEventKey(origin, epoch, expected.UpdatedByEvent)
	if err != nil {
		return model.ReviewWork{}, model.EventKey{}, err
	}
	return parent, sourceKey, nil
}
