package peer

import (
	"context"
	"errors"
	"testing"
)

func TestChannelEnrollmentClientRejectsMissingStreamBeforeSessionState(t *testing.T) {
	var client *ChannelEnrollmentClient
	_, err := client.join(context.Background(), nil, preparedChannelJoin{})
	var failure *ChannelProtocolFailure
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorInvalidToken || failure.Retryable() {
		t.Fatalf("missing stream failure = %#v", err)
	}
}
