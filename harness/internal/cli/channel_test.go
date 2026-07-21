package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestTakeJSONFlagWorksBeforeOrAfterChannelName(t *testing.T) {
	t.Parallel()
	args, enabled := takeJSONFlag([]string{"review", "--json"})
	if !enabled || len(args) != 1 || args[0] != "review" {
		t.Fatalf("takeJSONFlag() = %#v, %v", args, enabled)
	}
}

func TestChannelStatusAliasJSONPreservesPublicEvidence(t *testing.T) {
	t.Parallel()
	channel := localapi.ChannelView{Alias: "alpha", ChannelIDDigest: "sha256:channel",
		Publications: []localapi.ChannelPublicationView{{Arrival: "gossip",
			AudiencePeerIDs: []string{"peer-target"}, IgnoredPeerIDs: []string{},
			OriginPeerID: "peer-origin", ImmediateTransportPeerID: "peer-relay",
			PublicationDigest: "sha256:publication", EventDigest: "sha256:event",
			SemanticOutcome: "accepted"}}}
	response := localapi.ChannelStatusResponse{SchemaVersion: localapi.SchemaVersion, Status: "ok",
		Channels: []localapi.ChannelView{channel, {Alias: "beta"}}}
	client := &channelStatusClientStub{response: response}
	var stdout, stderr bytes.Buffer
	app := &channelApp{stdin: bytes.NewReader(nil), stdout: &stdout, stderr: &stderr}
	if exit := app.status(context.Background(), client, []string{"alpha", "--json"}); exit != 0 {
		t.Fatalf("channel status exit = %d, stderr=%q", exit, stderr.String())
	}
	want := response
	want.Channels = []localapi.ChannelView{channel}
	raw, err := model.CanonicalMarshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); got != string(raw)+"\n" {
		t.Fatalf("channel status JSON = %q, want %q", got, string(raw)+"\n")
	}
}

type channelStatusClientStub struct {
	channelControlClient
	response localapi.ChannelStatusResponse
}

func (client *channelStatusClientStub) ReadChannelStatus(context.Context) (
	localapi.ChannelStatusResponse, *localapi.APIError,
) {
	return client.response, nil
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

func TestChannelLeaveReportsQueuedOwnerAcknowledgement(t *testing.T) {
	t.Parallel()
	client := &leaveChannelClientStub{response: localapi.ChannelLeaveResponse{
		SchemaVersion: localapi.SchemaVersion, Status: "leaving",
		Channel: localapi.ChannelView{Alias: "alpha", Membership: "leaving"}}}
	var stdout, stderr bytes.Buffer
	app := &channelApp{stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr}
	if exit := app.leave(context.Background(), client, []string{"alpha"}); exit != 0 ||
		stderr.Len() != 0 || stdout.String() !=
		"Leaving Channel alpha (owner acknowledgement queued)\n" {
		t.Fatalf("queued leave = exit %d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
}

type leaveChannelClientStub struct {
	channelControlClient
	response localapi.ChannelLeaveResponse
}

func (client *leaveChannelClientStub) LeaveChannel(context.Context,
	localapi.ChannelLeaveRequest,
) (localapi.ChannelLeaveResponse, *localapi.APIError) {
	return client.response, nil
}
