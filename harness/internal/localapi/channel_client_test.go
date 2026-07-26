package localapi

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelClientRejectsSecretsAndInvalidRulesBeforeTransport(t *testing.T) {
	client := &Client{}
	if _, apiErr := client.JoinChannel(context.Background(),
		ChannelJoinRequest{Token: "mnch1_not-a-token"}); apiErr == nil || apiErr.Code != CodeInvalidToken {
		t.Fatalf("invalid join token = %#v", apiErr)
	}
	if _, apiErr := client.CreateChannel(context.Background(),
		ChannelCreateRequest{Name: strings.Repeat("x", 129)}, PendingJournal{}); apiErr == nil ||
		apiErr.Code != CodeInvalidArgument {
		t.Fatalf("oversized Channel name = %#v", apiErr)
	}
	if _, apiErr := client.CreateChannelInvite(context.Background(),
		ChannelInviteRequest{ExpiresSeconds: 1}, PendingJournal{}); apiErr == nil ||
		apiErr.Code != CodeInvalidArgument {
		t.Fatalf("invalid invite duration = %#v", apiErr)
	}
	if _, apiErr := client.AbandonChannel(context.Background(), ChannelAbandonRequest{
		Channel: "alpha", ConfirmChannel: "beta", Force: true}); apiErr == nil ||
		apiErr.Code != CodeInvalidArgument {
		t.Fatalf("mismatched abandon confirmation = %#v", apiErr)
	}
}

type channelClientJournalFixture struct {
	client   *Client
	journals *PendingJournalStore
	calls    int
	paths    []string
	keys     []string
}

func newChannelClientJournalFixture(t *testing.T) *channelClientJournalFixture {
	t.Helper()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x71)
	installClientCredential(t, nodeState, credential)
	fixture := &channelClientJournalFixture{}
	stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
		request *http.Request,
	) {
		fixture.calls++
		fixture.paths = append(fixture.paths, request.URL.Path)
		fixture.keys = append(fixture.keys, request.Header.Get(operationKeyHeader))
		writeError(writer, NewAPIError(CodeActionNotAllowed, "captured Channel mutation"))
	}))
	t.Cleanup(stop)
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	journals, err := NewPendingJournalStore(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	fixture.client = client
	fixture.journals = journals
	return fixture
}

func (fixture *channelClientJournalFixture) journal(t *testing.T,
	digest model.Digest,
) PendingJournal {
	t.Helper()
	journal, _, err := fixture.journals.FindOrCreate(digest, nil)
	if err != nil {
		t.Fatal(err)
	}
	return journal
}

func TestChannelClientSendsVerifiedCreateJournalKey(t *testing.T) {
	fixture := newChannelClientJournalFixture(t)
	create := ChannelCreateRequest{Name: "alpha"}
	createDigest, _ := ChannelCreateRequestDigest(create)
	createJournal := fixture.journal(t, createDigest)
	if _, apiErr := fixture.client.CreateChannel(context.Background(),
		create, createJournal); apiErr == nil || apiErr.Code != CodeActionNotAllowed {
		t.Fatalf("captured create error = %#v", apiErr)
	}
	if fixture.calls != 1 || fixture.paths[0] != RouteChannelCreate ||
		fixture.keys[0] != createJournal.OperationKeyHeader() {
		t.Fatalf("create transport = calls %d paths %v keys %v",
			fixture.calls, fixture.paths, fixture.keys)
	}
}

func TestChannelClientRejectsChangedCreateBeforeTransport(t *testing.T) {
	fixture := newChannelClientJournalFixture(t)
	createDigest, _ := ChannelCreateRequestDigest(ChannelCreateRequest{Name: "alpha"})
	createJournal := fixture.journal(t, createDigest)
	if _, apiErr := fixture.client.CreateChannel(context.Background(),
		ChannelCreateRequest{Name: "changed"}, createJournal); apiErr == nil ||
		apiErr.Code != CodeOperationMismatch || fixture.calls != 0 {
		t.Fatalf("changed create = error %#v calls %d", apiErr, fixture.calls)
	}
}

func TestChannelClientSendsDistinctVerifiedInviteJournalKey(t *testing.T) {
	fixture := newChannelClientJournalFixture(t)
	createDigest, _ := ChannelCreateRequestDigest(ChannelCreateRequest{Name: "alpha"})
	createJournal := fixture.journal(t, createDigest)
	invite := ChannelInviteRequest{Channel: "alpha", ExpiresSeconds: 3600, Uses: 1}
	inviteDigest, _ := ChannelInviteRequestDigest(invite)
	inviteJournal := fixture.journal(t, inviteDigest)
	if _, apiErr := fixture.client.CreateChannelInvite(context.Background(),
		invite, inviteJournal); apiErr == nil || apiErr.Code != CodeActionNotAllowed {
		t.Fatalf("captured invite error = %#v", apiErr)
	}
	if fixture.calls != 1 || fixture.paths[0] != RouteChannelInvites ||
		fixture.keys[0] != inviteJournal.OperationKeyHeader() ||
		createJournal.OperationKeyHash() == inviteJournal.OperationKeyHash() {
		t.Fatalf("invite transport = calls %d paths %v keys %v",
			fixture.calls, fixture.paths, fixture.keys)
	}
}

func TestChannelClientSendsVerifiedLeaveJournalKey(t *testing.T) {
	fixture := newChannelClientJournalFixture(t)
	leave := ChannelLeaveRequest{Channel: "alpha"}
	leaveDigest, _ := ChannelLeaveRequestDigest(leave)
	leaveJournal := fixture.journal(t, leaveDigest)
	if _, apiErr := fixture.client.LeaveChannel(context.Background(),
		leave, leaveJournal); apiErr == nil ||
		apiErr.Code != CodeActionNotAllowed {
		t.Fatalf("captured leave error = %#v", apiErr)
	}
	if fixture.calls != 1 || fixture.paths[0] != RouteChannelLeave ||
		fixture.keys[0] != leaveJournal.OperationKeyHeader() {
		t.Fatalf("leave transport = calls %d paths %v keys %v",
			fixture.calls, fixture.paths, fixture.keys)
	}
}
