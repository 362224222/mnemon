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

func TestChannelStatusAliasJSONPreservesBoundedOperationalView(t *testing.T) {
	t.Parallel()
	channel := localapi.ChannelView{Alias: "alpha", ChannelIDDigest: "sha256:channel"}
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
	if strings.Contains(stdout.String(), `"publications"`) {
		t.Fatalf("Channel operational JSON contains publication history: %s", stdout.String())
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

func TestChannelCreateResponseLossReusesJournalUntilPresentation(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	client := &channelMutationClientStub{createErr: localapi.NewAPIError(
		localapi.CodeMnemondUnavailable, "mnemond local control is unavailable")}
	stdout, stderr, app := channelMutationTestApp(t, workspace, client)
	args := []string{"create", "review", "--json"}
	if exit := app.run(context.Background(), args); exit != 5 || stdout.Len() != 0 ||
		stderr.String() != "mnemond_unavailable: mnemond local control is unavailable\n" {
		t.Fatalf("lost create response = exit %d stdout=%q stderr=%q",
			exit, stdout.String(), stderr.String())
	}
	firstKey := client.createJournal.OperationKeyHash()
	assertJournalSuffixes(t, nodeState, []string{".pending"})

	client.createErr = nil
	client.createResponse = localapi.ChannelCreateResponse{SchemaVersion: localapi.SchemaVersion,
		Status: "created", Channel: localapi.ChannelView{Alias: "review",
			Topic: localapi.ChannelTopicView{Status: "joined"}},
		InviteToken: "mnch1_test-token"}
	stdout.Reset()
	stderr.Reset()
	if exit := app.run(context.Background(), args); exit != 0 ||
		client.createJournal.OperationKeyHash() != firstKey {
		t.Fatalf("create replay = exit %d key=%s stdout=%q", exit,
			client.createJournal.OperationKeyHash().String(), stdout.String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})

	stdout.Reset()
	if exit := app.run(context.Background(), args); exit != 0 ||
		client.createJournal.OperationKeyHash() == firstKey {
		t.Fatalf("intentional create after presentation = exit %d key=%s",
			exit, client.createJournal.OperationKeyHash().String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented", ".presented"})
}

func TestChannelInviteTerminalJournalSurvivesPresentationFailure(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	client := &channelMutationClientStub{inviteResponse: localapi.ChannelInviteResponse{
		SchemaVersion: localapi.SchemaVersion, Status: "created",
		Channel:     localapi.ChannelView{Alias: "review"},
		InviteToken: "mnch1_test-token"}}
	_, stderr, app := channelMutationTestApp(t, workspace, client)
	app.stdout = failingWriter{}
	args := []string{"invite", "--channel", "review", "--uses", "2", "--json"}
	if exit := app.run(context.Background(), args); exit != 1 || stderr.Len() != 0 {
		t.Fatalf("failed invite presentation = exit %d stderr=%q", exit, stderr.String())
	}
	firstKey := client.inviteJournal.OperationKeyHash()
	assertJournalSuffixes(t, nodeState, []string{".terminal"})

	stdout := &bytes.Buffer{}
	app.stdout = stdout
	if exit := app.run(context.Background(), args); exit != 0 ||
		client.inviteJournal.OperationKeyHash() != firstKey {
		t.Fatalf("invite terminal replay = exit %d key=%s stdout=%q", exit,
			client.inviteJournal.OperationKeyHash().String(), stdout.String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})
}

type channelMutationClientStub struct {
	channelControlClient
	createJournal  localapi.PendingJournal
	createResponse localapi.ChannelCreateResponse
	createErr      *localapi.APIError
	inviteJournal  localapi.PendingJournal
	inviteResponse localapi.ChannelInviteResponse
	inviteErr      *localapi.APIError
}

func (client *channelMutationClientStub) CreateChannel(_ context.Context,
	_ localapi.ChannelCreateRequest, journal localapi.PendingJournal,
) (localapi.ChannelCreateResponse, *localapi.APIError) {
	client.createJournal = journal
	return client.createResponse, client.createErr
}

func (client *channelMutationClientStub) CreateChannelInvite(_ context.Context,
	_ localapi.ChannelInviteRequest, journal localapi.PendingJournal,
) (localapi.ChannelInviteResponse, *localapi.APIError) {
	client.inviteJournal = journal
	return client.inviteResponse, client.inviteErr
}

func channelMutationTestApp(t *testing.T, workspace string,
	client channelControlClient,
) (*bytes.Buffer, *bytes.Buffer, *channelApp) {
	t.Helper()
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	app := &channelApp{stdin: bytes.NewReader(nil), stdout: stdout, stderr: stderr,
		deps: productionChannelDependencies()}
	app.deps.workingDirectory = func() (string, error) { return workspace, nil }
	app.deps.newClient = func(string) (channelControlClient, error) { return client, nil }
	app.deps.ensureDaemon = func(context.Context, string, string,
		daemonHealthClient,
	) *localapi.APIError {
		return nil
	}
	return stdout, stderr, app
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
