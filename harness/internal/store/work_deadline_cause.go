package store

import (
	"context"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func exactDeadlineCause(ctx context.Context, q rowQuerier,
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
		model.EventType(eventTypeText) != deadlineCurrentEventType(current.State()) ||
		sourceText != string(model.EventSourceLocal) || timeErr != nil ||
		!acceptedAt.Equal(current.UpdatedAt()) {
		return model.EventKey{}, fmt.Errorf("%w: current Work update Event is not exact",
			ErrDeadlineResolution)
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
