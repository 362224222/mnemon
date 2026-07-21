package peer

import (
	"context"
	"errors"
	"testing"
)

func TestChannelMemberDispatchRejectsUnavailableTransport(t *testing.T) {
	t.Parallel()
	service := &ChannelMemberService{}
	if err := service.HandleChannelRequest(context.Background(), nil,
		ChannelFrame{}); !errors.Is(err, ErrChannelMemberProtocol) {
		t.Fatalf("unavailable dispatch error = %v", err)
	}
}
