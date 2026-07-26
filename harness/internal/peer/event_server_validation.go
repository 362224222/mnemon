package peer

import (
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func validateEventSourcePage(localPeer model.PeerID, localPublicKey []byte,
	request PullRequest, page store.PeerPullPage,
) error {
	if !validEventSourcePageBounds(localPeer, localPublicKey, request, page) {
		return store.ErrPeerPullInvariant
	}
	if len(page.Publications) == 0 {
		if page.ScannedChannelSequence != request.AfterChannelSequence() ||
			page.SourceHead != request.AfterChannelSequence() {
			return store.ErrPeerPullInvariant
		}
		return nil
	}
	return validateEventSourcePublications(localPeer, localPublicKey, request, page)
}

func validEventSourcePageBounds(localPeer model.PeerID, localPublicKey []byte,
	request PullRequest, page store.PeerPullPage,
) bool {
	return !localPeer.IsZero() && len(localPublicKey) == 32 &&
		page.OriginEpoch == request.OriginEpoch() && page.SourceFloor != 0 &&
		page.SourceFloor <= model.MaxSQLiteInteger && page.SourceHead <= model.MaxSQLiteInteger &&
		page.SourceFloor-1 <= page.SourceHead &&
		request.AfterChannelSequence() >= page.SourceFloor-1 &&
		page.ScannedChannelSequence >= request.AfterChannelSequence() &&
		page.ScannedChannelSequence <= page.SourceHead &&
		page.AcknowledgedSequence == request.AfterChannelSequence() &&
		len(page.Publications) <= int(request.Limit())
}

func validateEventSourcePublications(localPeer model.PeerID, localPublicKey []byte,
	request PullRequest, page store.PeerPullPage,
) error {
	expected := request.AfterChannelSequence() + 1
	for _, publication := range page.Publications {
		key := publication.Key()
		if publication.WireJSON().IsZero() || key.ChannelID() != request.ChannelID() ||
			key.OriginPeerID() != localPeer || key.OriginEpoch() != request.OriginEpoch() ||
			key.ChannelSequence() != expected ||
			model.VerifyPublication(localPublicKey, publication) != nil {
			return store.ErrPeerPullInvariant
		}
		expected++
	}
	if expected-1 != page.ScannedChannelSequence {
		return store.ErrPeerPullInvariant
	}
	return nil
}
