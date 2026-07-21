package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func exactDeadlineCause(ctx context.Context, q rowQuerier,
	current model.ReviewWork,
) (model.EventKey, error) {
	key, err := exactReviewWorkUpdateCause(ctx, q, current)
	if err != nil {
		return model.EventKey{}, fmt.Errorf("%w: current Work update Event is not exact",
			ErrDeadlineResolution)
	}
	var sourceText string
	err = q.QueryRowContext(ctx, `SELECT source FROM events WHERE event_id=?
		AND origin_peer_id=? AND origin_epoch=?`, key.EventID().String(),
		key.OriginPeerID().String(), key.OriginEpoch().String()).Scan(&sourceText)
	if err != nil || sourceText != string(model.EventSourceLocal) {
		return model.EventKey{}, fmt.Errorf("%w: current Work update Event is not exact",
			ErrDeadlineResolution)
	}
	if deadlineCurrentEventType(current.State()) == "" {
		return model.EventKey{}, fmt.Errorf("%w: current Work update Event is not exact",
			ErrDeadlineResolution)
	}
	return key, nil
}

func (s *Store) ReviewWorkUpdateCause(ctx context.Context,
	current model.ReviewWork,
) (model.EventKey, error) {
	if s == nil || s.db == nil || ctx == nil {
		return model.EventKey{}, errors.New("ReviewWork update cause requires Store and context")
	}
	key, err := exactReviewWorkUpdateCause(ctx, s.db, current)
	if err != nil {
		return model.EventKey{}, fmt.Errorf("%w: current Work update Event is not exact",
			ErrAdmissionConflict)
	}
	return key, nil
}

func exactReviewWorkUpdateCause(ctx context.Context, q rowQuerier,
	current model.ReviewWork,
) (model.EventKey, error) {
	var originText, epochText, channelText, homeText, workText, eventTypeText, sourceText, acceptedText string
	err := q.QueryRowContext(ctx, `SELECT origin_peer_id,origin_epoch,channel_id,work_home_peer_id,work_id,
		event_type,source,accepted_at FROM events WHERE event_id=?`, current.UpdatedBy().String()).
		Scan(&originText, &epochText, &channelText, &homeText, &workText, &eventTypeText, &sourceText,
			&acceptedText)
	acceptedAt, timeErr := parseCanonicalStoreTime(acceptedText)
	if err != nil || channelText != current.ChannelID().String() ||
		homeText != current.Ref().HomePeerID().String() || workText != current.Ref().WorkID().String() ||
		originText != current.Ref().HomePeerID().String() ||
		model.EventType(eventTypeText) != updateEventType(current.State()) ||
		!model.EventSource(sourceText).Valid() || timeErr != nil ||
		!acceptedAt.Equal(current.UpdatedAt()) {
		return model.EventKey{}, ErrAdmissionConflict
	}
	origin, err := model.ParsePeerID(originText)
	if err != nil {
		return model.EventKey{}, err
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil {
		return model.EventKey{}, err
	}
	key, err := model.NewEventKey(origin, epoch, current.UpdatedBy())
	if err != nil {
		return model.EventKey{}, err
	}
	return key, nil
}
