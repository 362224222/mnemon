package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestTakeJSONFlagWorksBeforeOrAfterChannelName(t *testing.T) {
	t.Parallel()
	args, enabled := takeJSONFlag([]string{"review", "--json"})
	if !enabled || len(args) != 1 || args[0] != "review" {
		t.Fatalf("takeJSONFlag() = %#v, %v", args, enabled)
	}
}

type abandonChannelClientStub struct {
	channelControlClient
	request localapi.ChannelAbandonRequest
}

func (client *abandonChannelClientStub) AbandonChannel(_ context.Context,
	request localapi.ChannelAbandonRequest,
) (localapi.ChannelAbandonResponse, *localapi.APIError) {
	client.request = request
	return localapi.ChannelAbandonResponse{SchemaVersion: localapi.SchemaVersion,
		Status: "abandoned", Channel: request.Channel,
		TransitionedAt: "2026-07-21T10:00:00Z"}, nil
}

func TestChannelAbandonRequiresHiddenExactConfirmation(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"--confirm-channel", "alpha"}, {"--force"},
		{"alpha", "--force", "--confirm-channel", "alpha"},
	} {
		var stdout, stderr bytes.Buffer
		app := &channelApp{stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr}
		exit := app.abandon(context.Background(), &abandonChannelClientStub{}, args)
		if exit == 0 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "requires --force") {
			t.Fatalf("unsafe abandon args %v = exit %d stdout=%q stderr=%q", args, exit,
				stdout.String(), stderr.String())
		}
	}
	var stdout, stderr bytes.Buffer
	client := &abandonChannelClientStub{}
	app := &channelApp{stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr}
	exit := app.abandon(context.Background(), client,
		[]string{"--force", "--confirm-channel", "alpha", "--json"})
	if exit != 0 || stderr.Len() != 0 || client.request.Channel != "alpha" ||
		client.request.ConfirmChannel != "alpha" || !client.request.Force ||
		!strings.Contains(stdout.String(), `"status":"abandoned"`) {
		t.Fatalf("confirmed abandon = exit %d request=%#v stdout=%q stderr=%q", exit,
			client.request, stdout.String(), stderr.String())
	}
}
