package localapi

import (
	"context"
	"net/http"
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

func TestChannelClientSendsVerifiedJournalKeyAndRejectsChangedRequestLocally(t *testing.T) {
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x71)
	installClientCredential(t, nodeState, credential)
	calls := 0
	var paths, keys []string
	stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
		request *http.Request,
	) {
		calls++
		paths = append(paths, request.URL.Path)
		keys = append(keys, request.Header.Get(operationKeyHeader))
		writeError(writer, NewAPIError(CodeActionNotAllowed, "captured Channel mutation"))
	}))
	defer stop()
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	journals, err := NewPendingJournalStore(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	create := ChannelCreateRequest{Name: "alpha"}
	createDigest, _ := ChannelCreateRequestDigest(create)
	createJournal, _, err := journals.FindOrCreate(createDigest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := client.CreateChannel(context.Background(), create, createJournal); apiErr == nil || apiErr.Code != CodeActionNotAllowed {
		t.Fatalf("captured create error = %#v", apiErr)
	}
	if calls != 1 || paths[0] != RouteChannelCreate ||
		keys[0] != createJournal.OperationKeyHeader() {
		t.Fatalf("create transport = calls %d paths %v keys %v", calls, paths, keys)
	}
	if _, apiErr := client.CreateChannel(context.Background(),
		ChannelCreateRequest{Name: "changed"}, createJournal); apiErr == nil || apiErr.Code != CodeOperationMismatch || calls != 1 {
		t.Fatalf("changed create = error %#v calls %d", apiErr, calls)
	}

	invite := ChannelInviteRequest{Channel: "alpha", ExpiresSeconds: 3600, Uses: 1}
	inviteDigest, _ := ChannelInviteRequestDigest(invite)
	inviteJournal, _, err := journals.FindOrCreate(inviteDigest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := client.CreateChannelInvite(context.Background(), invite, inviteJournal); apiErr == nil || apiErr.Code != CodeActionNotAllowed {
		t.Fatalf("captured invite error = %#v", apiErr)
	}
	if calls != 2 || paths[1] != RouteChannelInvites ||
		keys[1] != inviteJournal.OperationKeyHeader() ||
		createJournal.OperationKeyHash() == inviteJournal.OperationKeyHash() {
		t.Fatalf("invite transport = calls %d paths %v keys %v", calls, paths, keys)
	}

	leave := ChannelLeaveRequest{Channel: "alpha"}
	leaveDigest, _ := ChannelLeaveRequestDigest(leave)
	leaveJournal, _, err := journals.FindOrCreate(leaveDigest, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := client.LeaveChannel(context.Background(), leave, leaveJournal); apiErr == nil ||
		apiErr.Code != CodeActionNotAllowed {
		t.Fatalf("captured leave error = %#v", apiErr)
	}
	if calls != 3 || paths[2] != RouteChannelLeave ||
		keys[2] != leaveJournal.OperationKeyHeader() {
		t.Fatalf("leave transport = calls %d paths %v keys %v", calls, paths, keys)
	}
}
