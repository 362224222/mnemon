package nodecontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestHealthAdapterAcceptsOnlyCanonicalWire(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("health-assets")).String()
	tests := []struct {
		name     string
		response localapi.HealthResponse
		want     node.DaemonHealth
		wantErr  bool
	}{
		{name: "ready", response: localapi.HealthResponse{AssetRevision: revision,
			SchemaVersion: localapi.SchemaVersion, Status: "ready"},
			want: node.DaemonHealth{AssetRevision: revision, Ready: true}},
		{name: "not ready", response: localapi.HealthResponse{AssetRevision: revision,
			SchemaVersion: localapi.SchemaVersion, Status: "not_ready"},
			want: node.DaemonHealth{AssetRevision: revision}},
		{name: "schema", response: localapi.HealthResponse{AssetRevision: revision,
			SchemaVersion: localapi.SchemaVersion + 1, Status: "ready"}, wantErr: true},
		{name: "status", response: localapi.HealthResponse{AssetRevision: revision,
			SchemaVersion: localapi.SchemaVersion, Status: "starting"}, wantErr: true},
		{name: "revision", response: localapi.HealthResponse{AssetRevision: "invalid",
			SchemaVersion: localapi.SchemaVersion, Status: "ready"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := &healthClientStub{response: test.response}
			got, err := AdaptHealthClient(client).ProbeDaemonHealth(context.Background())
			if (err != nil) != test.wantErr || !test.wantErr && got != test.want {
				t.Fatalf("ProbeDaemonHealth() = (%#v, %v), want (%#v, error=%t)",
					got, err, test.want, test.wantErr)
			}
			if client.calls != 1 {
				t.Fatalf("ProbeHealth calls = %d, want 1", client.calls)
			}
		})
	}
}

func TestHealthAdapterClassifiesOnlyUnavailableTransport(t *testing.T) {
	t.Parallel()
	unavailable := localapi.NewAPIError(localapi.CodeMnemondUnavailable, "daemon is unavailable")
	_, err := AdaptHealthClient(&healthClientStub{apiErr: unavailable}).
		ProbeDaemonHealth(context.Background())
	if !errors.Is(err, node.ErrDaemonControlUnavailable) || !errors.Is(err, unavailable) {
		t.Fatalf("unavailable ProbeDaemonHealth() error = %v", err)
	}

	invalid := localapi.NewAPIError(localapi.CodeInternal, "invalid health")
	_, err = AdaptHealthClient(&healthClientStub{apiErr: invalid}).
		ProbeDaemonHealth(context.Background())
	if err != invalid || errors.Is(err, node.ErrDaemonControlUnavailable) {
		t.Fatalf("non-transport ProbeDaemonHealth() error = %v", err)
	}

	var typedNil *healthClientStub
	_, err = AdaptHealthClient(typedNil).ProbeDaemonHealth(context.Background())
	if err == nil || errors.Is(err, node.ErrDaemonControlUnavailable) {
		t.Fatalf("typed-nil ProbeDaemonHealth() error = %v", err)
	}
}

func TestLifecycleAdapterSendsExactAuthorityAndValidatesDigestReceipt(t *testing.T) {
	t.Parallel()
	authority := nodecontrolTestAuthority(t)
	wire, err := AuthorityResponse(authority)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := localapi.AuthorityDigest(wire)
	if err != nil {
		t.Fatal(err)
	}
	client := &mutationShutdownClientStub{response: localapi.ShutdownResponse{
		AuthorityDigest: digest.String(), SchemaVersion: localapi.SchemaVersion, Status: "stopping",
	}}
	if err := AdaptLifecycleClient(client).ShutdownDaemonForMutation(
		context.Background(), authority); err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 || client.request != wire {
		t.Fatalf("ShutdownForMutation request/calls = (%#v, %d), want (%#v, 1)",
			client.request, client.calls, wire)
	}
}

func TestLifecycleAdapterRejectsNoncanonicalShutdownResponse(t *testing.T) {
	t.Parallel()
	authority := nodecontrolTestAuthority(t)
	wire, err := AuthorityResponse(authority)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := localapi.AuthorityDigest(wire)
	if err != nil {
		t.Fatal(err)
	}
	base := localapi.ShutdownResponse{AuthorityDigest: digest.String(),
		SchemaVersion: localapi.SchemaVersion, Status: "stopping"}
	tests := []struct {
		name   string
		mutate func(*localapi.ShutdownResponse)
	}{
		{name: "schema", mutate: func(response *localapi.ShutdownResponse) {
			response.SchemaVersion++
		}},
		{name: "status", mutate: func(response *localapi.ShutdownResponse) {
			response.Status = "stopped"
		}},
		{name: "malformed digest", mutate: func(response *localapi.ShutdownResponse) {
			response.AuthorityDigest = "invalid"
		}},
		{name: "different digest", mutate: func(response *localapi.ShutdownResponse) {
			response.AuthorityDigest = model.Sum([]byte("different-authority")).String()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			response := base
			test.mutate(&response)
			client := &mutationShutdownClientStub{response: response}
			if err := AdaptLifecycleClient(client).ShutdownDaemonForMutation(
				context.Background(), authority); err == nil {
				t.Fatal("noncanonical shutdown response was accepted")
			}
			if client.calls != 1 || client.request != wire {
				t.Fatalf("ShutdownForMutation request/calls = (%#v, %d)", client.request, client.calls)
			}
		})
	}
}

func TestLifecycleAdapterClassifiesUnavailableAndRejectsTypedNil(t *testing.T) {
	t.Parallel()
	authority := nodecontrolTestAuthority(t)
	unavailable := localapi.NewAPIError(localapi.CodeMnemondUnavailable, "daemon is unavailable")
	err := AdaptLifecycleClient(&mutationShutdownClientStub{apiErr: unavailable}).
		ShutdownDaemonForMutation(context.Background(), authority)
	if !errors.Is(err, node.ErrDaemonControlUnavailable) || !errors.Is(err, unavailable) {
		t.Fatalf("unavailable ShutdownDaemonForMutation() error = %v", err)
	}

	invalid := localapi.NewAPIError(localapi.CodeInternal, "invalid shutdown")
	err = AdaptLifecycleClient(&mutationShutdownClientStub{apiErr: invalid}).
		ShutdownDaemonForMutation(context.Background(), authority)
	if err != invalid || errors.Is(err, node.ErrDaemonControlUnavailable) {
		t.Fatalf("non-transport ShutdownDaemonForMutation() error = %v", err)
	}

	var typedNil *mutationShutdownClientStub
	err = AdaptLifecycleClient(typedNil).ShutdownDaemonForMutation(context.Background(), authority)
	if err == nil || errors.Is(err, node.ErrDaemonControlUnavailable) {
		t.Fatalf("typed-nil ShutdownDaemonForMutation() error = %v", err)
	}
}

func TestAdaptersRejectMalformedUnavailableErrors(t *testing.T) {
	t.Parallel()
	authority := nodecontrolTestAuthority(t)
	operation, err := model.ParseOperationID("operation-malformed-unavailable")
	if err != nil {
		t.Fatal(err)
	}
	operationText := operation.String()
	for _, test := range []struct {
		name   string
		mutate func(*localapi.APIError)
	}{
		{name: "schema", mutate: func(value *localapi.APIError) { value.SchemaVersion++ }},
		{name: "status", mutate: func(value *localapi.APIError) { value.Status = "failed" }},
		{name: "retryable", mutate: func(value *localapi.APIError) { value.Retryable = false }},
		{name: "message", mutate: func(value *localapi.APIError) { value.Message = " invalid" }},
		{name: "operation", mutate: func(value *localapi.APIError) { value.OperationID = &operationText }},
		{name: "replay", mutate: func(value *localapi.APIError) {
			value.Replayed, value.OperationID = true, &operationText
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			malformed := *localapi.NewAPIError(localapi.CodeMnemondUnavailable, "daemon is unavailable")
			test.mutate(&malformed)
			_, healthErr := AdaptHealthClient(&healthClientStub{apiErr: &malformed}).
				ProbeDaemonHealth(context.Background())
			lifecycleErr := AdaptLifecycleClient(&mutationShutdownClientStub{apiErr: &malformed}).
				ShutdownDaemonForMutation(context.Background(), authority)
			if healthErr == nil || lifecycleErr == nil ||
				errors.Is(healthErr, node.ErrDaemonControlUnavailable) ||
				errors.Is(lifecycleErr, node.ErrDaemonControlUnavailable) {
				t.Fatalf("malformed unavailable errors = (health=%v, lifecycle=%v)",
					healthErr, lifecycleErr)
			}
		})
	}
}

func TestAuthorityAdapterRoundTripsCanonicalNodeValue(t *testing.T) {
	t.Parallel()
	authority := nodecontrolTestAuthority(t)
	wire, err := AuthorityResponse(authority)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Authority(wire)
	if err != nil || got != authority {
		t.Fatalf("Authority() = (%#v, %v), want %#v", got, err, authority)
	}
	if semantic, err := got.Digest(); err != nil || semantic.IsZero() {
		t.Fatalf("Authority digest = (%s, %v)", semantic.String(), err)
	}
}

type healthClientStub struct {
	response localapi.HealthResponse
	apiErr   *localapi.APIError
	calls    int
}

func (client *healthClientStub) ProbeHealth(context.Context) (
	localapi.HealthResponse, *localapi.APIError,
) {
	client.calls++
	return client.response, client.apiErr
}

type mutationShutdownClientStub struct {
	request  localapi.AuthorityResponse
	response localapi.ShutdownResponse
	apiErr   *localapi.APIError
	calls    int
}

func (client *mutationShutdownClientStub) ShutdownForMutation(_ context.Context,
	request localapi.AuthorityResponse,
) (localapi.ShutdownResponse, *localapi.APIError) {
	client.calls++
	client.request = request
	return client.response, client.apiErr
}

func nodecontrolTestAuthority(t *testing.T) node.Authority {
	t.Helper()
	peerID, err := model.ParsePeerID("12D3KooWK8F9TXQDMzHsjod8VZiSs17mFZuZUCPnu6otGxJNDnaP")
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("assets")).String()
	return node.Authority{Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		Enabled: true, AssetRevision: revision,
		UpdatedAt: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC), PeerID: peerID,
		ActiveAssetRevision: revision}
}
