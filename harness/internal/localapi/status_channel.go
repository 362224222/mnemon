package localapi

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

const (
	statusChannelReady    = "ready"
	statusChannelQueued   = "queued"
	statusChannelDegraded = "degraded"
	statusChannelTerminal = "terminal"
)

func newStatusChannels(snapshots []StatusChannelSnapshot) ([]StatusChannel, error) {
	response, err := node.NewStatusResponse(statusChannelWrapperSnapshot(snapshots))
	if err != nil {
		return nil, err
	}
	return response.Channels, nil
}

func newStatusChannel(snapshot StatusChannelSnapshot) (StatusChannel, error) {
	channels, err := newStatusChannels([]StatusChannelSnapshot{snapshot})
	if err != nil {
		return StatusChannel{}, err
	}
	if len(channels) != 1 {
		return StatusChannel{}, errors.New("local API: status Channel progress is invalid")
	}
	return channels[0], nil
}

func statusChannelWrapperSnapshot(channels []StatusChannelSnapshot) StatusSnapshot {
	return StatusSnapshot{
		ArtifactTransfer: StatusArtifactTransferSnapshot{
			MaximumPulls: StatusArtifactTransferPullLimit(),
		},
		AssetRevision:   model.Sum([]byte("status-channel-wrapper")).String(),
		ActivationReady: true,
		Runtime:         RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true},
		Channels:        channels}
}

func ValidateStatusChannels(channels []StatusChannel) error {
	return node.ValidateStatusChannels(channels)
}
