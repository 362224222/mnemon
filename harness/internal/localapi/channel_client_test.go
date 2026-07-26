package localapi

import (
	"context"
	"strings"
	"testing"
)

func TestChannelClientRejectsSecretsAndInvalidRulesBeforeTransport(t *testing.T) {
	client := &Client{}
	if _, apiErr := client.JoinChannel(context.Background(),
		ChannelJoinRequest{Token: "mnch1_not-a-token"}); apiErr == nil || apiErr.Code != CodeInvalidToken {
		t.Fatalf("invalid join token = %#v", apiErr)
	}
	if _, apiErr := client.CreateChannel(context.Background(),
		ChannelCreateRequest{Name: strings.Repeat("x", 129)}); apiErr == nil ||
		apiErr.Code != CodeInvalidArgument {
		t.Fatalf("oversized Channel name = %#v", apiErr)
	}
	if _, apiErr := client.CreateChannelInvite(context.Background(),
		ChannelInviteRequest{ExpiresSeconds: 1}); apiErr == nil || apiErr.Code != CodeInvalidArgument {
		t.Fatalf("invalid invite duration = %#v", apiErr)
	}
	if _, apiErr := client.AbandonChannel(context.Background(), ChannelAbandonRequest{
		Channel: "alpha", ConfirmChannel: "beta", Force: true}); apiErr == nil ||
		apiErr.Code != CodeInvalidArgument {
		t.Fatalf("mismatched abandon confirmation = %#v", apiErr)
	}
}
