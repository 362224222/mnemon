package peer

import (
	"context"
	"errors"
	"testing"
)

func TestChannelEnrollmentClientRejectsMissingStreamBeforeSessionState(t *testing.T) {
	var client *ChannelEnrollmentClient
	_, err := client.Join(context.Background(), nil, JoinChannelSpec{})
	var failure *ChannelProtocolFailure
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorInvalidToken || failure.Retryable() {
		t.Fatalf("missing stream failure = %#v", err)
	}
}
