package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type fakeStatusClient struct {
	responses []localapi.StatusResponse
	errors    []*localapi.APIError
	reads     int
	probes    int
}

func (client *fakeStatusClient) ProbeHealth(context.Context) (localapi.HealthResponse,
	*localapi.APIError,
) {
	client.probes++
	return localapi.HealthResponse{}, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"fake health unavailable")
}

func (client *fakeStatusClient) ReadStatus(context.Context) (localapi.StatusResponse,
	*localapi.APIError,
) {
	index := client.reads
	client.reads++
	var response localapi.StatusResponse
	var apiErr *localapi.APIError
	if index < len(client.responses) {
		response = client.responses[index]
	}
	if index < len(client.errors) {
		apiErr = client.errors[index]
	}
	return response, apiErr
}

func TestStatusAppReportsTrustedOnlineStateAndExitClassWithoutEnsure(t *testing.T) {
	workspace, nodeState := statusWorkspace(t)
	tests := []struct {
		name     string
		snapshot localapi.StatusSnapshot
		wantExit int
	}{
		{name: "ready", snapshot: localapi.StatusSnapshot{AssetRevision: statusRevision(),
			ActivationReady: true,
			Runtime:         localapi.RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}}},
		{name: "activation", snapshot: localapi.StatusSnapshot{AssetRevision: statusRevision(),
			ActivationIssue: "asset_revision_mismatch",
			Runtime:         localapi.RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}},
			wantExit: 3},
		{name: "recovering", snapshot: localapi.StatusSnapshot{AssetRevision: statusRevision(),
			ActivationReady: true,
			Runtime:         localapi.RuntimeStatusSnapshot{Running: true, Healthy: true, Recovering: true}},
			wantExit: 5},
		{name: "failed", snapshot: localapi.StatusSnapshot{AssetRevision: statusRevision(),
			ActivationReady: true,
			Runtime:         localapi.RuntimeStatusSnapshot{Issue: "managed_runtime_failed"}},
			wantExit: 1},
	}
	for index := range tests {
		tests[index].snapshot.ArtifactTransfer = cliStatusArtifactTransferSnapshot(0)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := localapi.NewStatusResponse(test.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			client := &fakeStatusClient{responses: []localapi.StatusResponse{response}}
			app, stdout, stderr, ensured := newStatusTestApp(workspace, nodeState, client)
			exit := app.run(context.Background(), nil)
			want, _ := model.CanonicalMarshal(response)
			if exit != test.wantExit || stdout.String() != string(append(want, '\n')) ||
				stderr.Len() != 0 || client.reads != 1 || *ensured != 0 {
				t.Fatalf("status = exit %d stdout %q stderr %q reads=%d ensured=%d",
					exit, stdout.String(), stderr.String(), client.reads, *ensured)
			}
		})
	}
}

func TestStatusAppEnsuresOnlyAfterTransportUnavailabilityAndReadsAgain(t *testing.T) {
	workspace, nodeState := statusWorkspace(t)
	ready, err := localapi.NewStatusResponse(localapi.StatusSnapshot{
		ArtifactTransfer: cliStatusArtifactTransferSnapshot(0), AssetRevision: statusRevision(),
		ActivationReady: true,
		Runtime:         localapi.RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}})
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStatusClient{responses: []localapi.StatusResponse{{}, ready},
		errors: []*localapi.APIError{localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"socket unavailable"), nil}}
	app, stdout, stderr, ensured := newStatusTestApp(workspace, nodeState, client)
	if exit := app.run(context.Background(), nil); exit != 0 || stderr.Len() != 0 ||
		client.reads != 2 || *ensured != 1 || client.probes != 0 {
		t.Fatalf("ensured status = exit %d stdout %q stderr %q reads=%d probes=%d ensured=%d",
			exit, stdout.String(), stderr.String(), client.reads, client.probes, *ensured)
	}
}

func cliStatusArtifactTransferSnapshot(active int) localapi.StatusArtifactTransferSnapshot {
	return localapi.StatusArtifactTransferSnapshot{ActivePulls: active,
		MaximumPulls: localapi.StatusArtifactTransferPullLimit()}
}

func TestStatusAppFailsClosedWithoutEnsureForNontransportErrors(t *testing.T) {
	workspace, nodeState := statusWorkspace(t)
	tests := []struct {
		name     string
		client   *fakeStatusClient
		ensure   *localapi.APIError
		wantExit int
		wantText string
	}{
		{name: "authentication", client: &fakeStatusClient{errors: []*localapi.APIError{
			localapi.NewAPIError(localapi.CodeAuthenticationFailed, "profile authentication failed")}},
			wantExit: 3, wantText: "authentication_failed: profile authentication failed\n"},
		{name: "ensure failure", client: &fakeStatusClient{errors: []*localapi.APIError{
			localapi.NewAPIError(localapi.CodeMnemondUnavailable, "socket unavailable")}},
			ensure: localapi.NewAPIError(localapi.CodeAssetRevisionMismatch,
				"managed Node authority or installed assets are invalid"), wantExit: 3,
			wantText: "asset_revision_mismatch: managed Node authority or installed assets are invalid\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			ensured := 0
			app := &statusApp{stdout: &stdout, stderr: &stderr, deps: statusDependencies{
				workingDirectory: func() (string, error) { return workspace, nil },
				newClient: func(got string) (statusControlClient, error) {
					if got != nodeState {
						t.Fatalf("Node state = %q", got)
					}
					return test.client, nil
				},
				ensureDaemon: func(context.Context, string, string,
					daemonHealthClient,
				) *localapi.APIError {
					ensured++
					return test.ensure
				},
			}}
			exit := app.run(context.Background(), nil)
			wantEnsure := 0
			if test.ensure != nil {
				wantEnsure = 1
			}
			if exit != test.wantExit || stdout.Len() != 0 || stderr.String() != test.wantText ||
				ensured != wantEnsure || test.client.reads != 1 {
				t.Fatalf("failed status = exit %d stdout %q stderr %q reads=%d ensured=%d",
					exit, stdout.String(), stderr.String(), test.client.reads, ensured)
			}
		})
	}
}

func TestStatusAppValidatesInvocationWorkspaceAndClientState(t *testing.T) {
	workspace, nodeState := statusWorkspace(t)
	tests := []struct {
		name      string
		args      []string
		work      func() (string, error)
		clientErr error
		wantExit  int
		wantText  string
	}{
		{name: "arguments", args: []string{"--json"},
			work: func() (string, error) { return workspace, nil }, wantExit: 2,
			wantText: "invalid_argument: status accepts no arguments\n"},
		{name: "workspace", work: func() (string, error) { return t.TempDir(), nil }, wantExit: 5,
			wantText: "mnemond_unavailable: Mnemon Harness is not set up in this workspace\n"},
		{name: "credential", work: func() (string, error) { return workspace, nil },
			clientErr: localapi.ErrUnsafeClientState, wantExit: 3,
			wantText: "authentication_failed: Mnemon Harness client state is unavailable\n"},
		{name: "client", work: func() (string, error) { return workspace, nil },
			clientErr: errors.New("client unavailable"), wantExit: 5,
			wantText: "mnemond_unavailable: mnemond local control is unavailable\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := &statusApp{stdout: &stdout, stderr: &stderr, deps: statusDependencies{
				workingDirectory: test.work,
				newClient: func(got string) (statusControlClient, error) {
					if got != nodeState {
						t.Fatalf("Node state = %q", got)
					}
					return nil, test.clientErr
				},
				ensureDaemon: func(context.Context, string, string,
					daemonHealthClient,
				) *localapi.APIError {
					t.Fatal("invalid invocation attempted daemon ensure")
					return nil
				},
			}}
			exit := app.run(context.Background(), test.args)
			if exit != test.wantExit || stdout.Len() != 0 || stderr.String() != test.wantText {
				t.Fatalf("invalid status = exit %d stdout %q stderr %q", exit,
					stdout.String(), stderr.String())
			}
		})
	}
}

func newStatusTestApp(workspace, nodeState string,
	client *fakeStatusClient,
) (*statusApp, *bytes.Buffer, *bytes.Buffer, *int) {
	var stdout, stderr bytes.Buffer
	ensured := 0
	app := &statusApp{stdout: &stdout, stderr: &stderr, deps: statusDependencies{
		workingDirectory: func() (string, error) { return filepath.Join(workspace, "nested"), nil },
		newClient: func(got string) (statusControlClient, error) {
			if got != nodeState {
				return nil, errors.New("wrong Node state")
			}
			return client, nil
		},
		ensureDaemon: func(_ context.Context, gotWorkspace, gotNodeState string,
			gotClient daemonHealthClient,
		) *localapi.APIError {
			if gotWorkspace != workspace || gotNodeState != nodeState || gotClient != client {
				return localapi.NewAPIError(localapi.CodeInternal, "wrong ensure scope")
			}
			ensured++
			return nil
		},
	}}
	return app, &stdout, &stderr, &ensured
}

func statusWorkspace(t *testing.T) (string, string) {
	t.Helper()
	workspace := t.TempDir()
	physical, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	workspace = physical
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	return workspace, nodeState
}

func statusRevision() string { return model.Sum([]byte("status-cli-assets")).String() }
