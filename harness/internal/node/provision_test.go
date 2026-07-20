package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestPrepareNodeStateCreatesOnlyASerializableOwnerDirectorySkeleton(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	const callers = 20
	results := make(chan string, callers)
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			nodeState, err := PrepareNodeState(workspace)
			if err != nil {
				errorsFound <- err
				return
			}
			results <- nodeState
		}()
	}
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("PrepareNodeState() error = %v", err)
	}
	want := filepath.Join(workspace, ".mnemon", "harness", "node")
	for result := range results {
		if result != want {
			t.Errorf("PrepareNodeState() = %q, want %q", result, want)
		}
	}
	for _, path := range []string{filepath.Join(workspace, ".mnemon", "harness"), want} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("prepared directory %s = (%v, %v)", path, info, err)
		}
	}
	entries, err := os.ReadDir(want)
	if err != nil || len(entries) != 0 {
		t.Fatalf("prepared Node authority entries = (%v, %v)", entries, err)
	}
}

func TestPrepareNodeStateRejectsUnsafeParentsWithoutCreatingAuthority(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	if err := os.Mkdir(filepath.Join(workspace, ".mnemon"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(workspace, ".mnemon", "harness")); err != nil {
		t.Fatal(err)
	}
	if nodeState, err := PrepareNodeState(workspace); nodeState != "" || !errors.Is(err, ErrProvision) {
		t.Fatalf("PrepareNodeState() = (%q, %v)", nodeState, err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".mnemon", "harness", "node.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe prepare created authority: %v", err)
	}
}

func TestProvisionCreatesAndReplaysOneDisabledWorkspaceAuthority(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 6, 0, 0, 0, time.UTC)
	options := ProvisionOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, Clock: controllerTestClock{at},
		Credentials: testProfileCredentials{}}
	first, err := Provision(context.Background(), options)
	if err != nil || !first.Created || !first.CredentialCreated || first.Profile.Enabled() ||
		first.Profile.Host() != model.HostCodex || first.Profile.Runtime() != model.RuntimeCodexAppServer ||
		first.Profile.WorkspaceRoot() != workspace || first.Node.PeerID().IsZero() ||
		first.Node.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
		t.Fatalf("first Provision() = (%#v, %v)", first, err)
	}
	assertProvisionModes(t, workspace, first.NodeState)
	identityBefore, err := os.Lstat(filepath.Join(first.NodeState, identityKeyName))
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(first.NodeState, "profiles", model.TeamworkProfileID().String()+".token")
	tokenBefore, err := os.Lstat(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := localapi.VerifyProfileCredential(first.NodeState, first.Profile.CredentialHash()); err != nil {
		t.Fatal(err)
	}

	secondOptions := options
	secondOptions.Clock = controllerTestClock{at.Add(24 * time.Hour)}
	second, err := Provision(context.Background(), secondOptions)
	if err != nil || second.Created || second.CredentialCreated || second.Node.PeerID() != first.Node.PeerID() ||
		second.Node.OriginEpoch() != first.Node.OriginEpoch() || second.Profile.Principal() != first.Profile.Principal() ||
		!second.Profile.CreatedAt().Equal(first.Profile.CreatedAt()) {
		t.Fatalf("replayed Provision() = (%#v, %v)", second, err)
	}
	identityAfter, _ := os.Lstat(filepath.Join(first.NodeState, identityKeyName))
	tokenAfter, _ := os.Lstat(tokenPath)
	if !os.SameFile(identityBefore, identityAfter) || !os.SameFile(tokenBefore, tokenAfter) {
		t.Fatal("replayed Provision replaced identity or credential")
	}
	st, err := store.Open(context.Background(), filepath.Join(first.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if err != nil || closeErr != nil || authority.Node.PeerID() != first.Node.PeerID() || authority.Profile.Enabled() {
		t.Fatalf("durable authority = (%#v, %v, close %v)", authority, err, closeErr)
	}
}

func TestProvisionSerializesConcurrentFirstSetup(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	options := provisionTestOptions(t, workspace, model.HostCodex)
	const callers = 12
	type outcome struct {
		result ProvisionResult
		err    error
	}
	started := make(chan struct{})
	outcomes := make(chan outcome, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			<-started
			result, err := Provision(context.Background(), options)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(started)
	wait.Wait()
	close(outcomes)

	created, credentialCreated := 0, 0
	var peerID model.PeerID
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Errorf("concurrent Provision() error = %v", outcome.err)
			continue
		}
		if outcome.result.Created {
			created++
		}
		if outcome.result.CredentialCreated {
			credentialCreated++
		}
		if peerID.IsZero() {
			peerID = outcome.result.Node.PeerID()
		} else if outcome.result.Node.PeerID() != peerID {
			t.Errorf("concurrent Provision() returned PeerID %s, want %s",
				outcome.result.Node.PeerID().String(), peerID.String())
		}
	}
	if created != 1 || credentialCreated != 1 {
		t.Fatalf("concurrent creation counts = Node %d, credential %d", created, credentialCreated)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	state, err := inspectMeshEndpointState(nodeState, peerID)
	if err != nil || state.stateKind() != meshEndpointStatePending {
		t.Fatalf("concurrent endpoint state = (%d, %v)", state.stateKind(), err)
	}
}

func TestProvisionCancellationDoesNotCreateUnfencedAuthority(t *testing.T) {
	t.Run("pre-cancelled validation", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		cause := errors.New("setup was cancelled")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		if result, err := Provision(ctx, provisionTestOptions(t, workspace, model.HostCodex)); result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) ||
			!errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatalf("cancelled Provision() = (%#v, %v)", result, err)
		}
		if _, err := os.Lstat(filepath.Join(workspace, ".mnemon")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pre-cancelled Provision created state: %v", err)
		}
	})

	t.Run("contended ensure lock", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		nodeState, err := PrepareNodeState(workspace)
		if err != nil {
			t.Fatal(err)
		}
		holder, err := acquireEnsureLock(context.Background(), nodeState, daemonLifecyclePoll)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.close()

		clockCalled := make(chan struct{})
		options := provisionTestOptions(t, workspace, model.HostCodex)
		options.Clock = provisionSignalClock{at: options.Clock.Now(), called: clockCalled}
		ctx, cancel := context.WithCancelCause(context.Background())
		resultFound := make(chan struct {
			result ProvisionResult
			err    error
		}, 1)
		go func() {
			result, err := Provision(ctx, options)
			resultFound <- struct {
				result ProvisionResult
				err    error
			}{result: result, err: err}
		}()
		<-clockCalled
		time.Sleep(3 * daemonLifecyclePoll)
		select {
		case early := <-resultFound:
			t.Fatalf("contended Provision returned before cancellation: %#v, %v", early.result, early.err)
		default:
		}
		assertProvisionAuthorityAbsent(t, nodeState)
		cause := errors.New("stop waiting for setup lock")
		cancel(cause)
		select {
		case found := <-resultFound:
			if found.result != (ProvisionResult{}) || !errors.Is(found.err, ErrProvision) ||
				!errors.Is(found.err, context.Canceled) || !errors.Is(found.err, cause) {
				t.Fatalf("contended Provision() = (%#v, %v)", found.result, found.err)
			}
		case <-time.After(time.Second):
			t.Fatal("contended Provision did not observe cancellation")
		}
		assertProvisionAuthorityAbsent(t, nodeState)
	})

	t.Run("existing verification cancellation", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		first, err := Provision(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		cause := errors.New("cancel existing replay")
		ctx, cancel := context.WithCancelCause(context.Background())
		options.Credentials = cancelingProvisionCredentials{cancel: func() { cancel(cause) }}
		if result, err := Provision(ctx, options); result != (ProvisionResult{}) ||
			!errors.Is(err, ErrProvision) || !errors.Is(err, context.Canceled) ||
			!errors.Is(err, cause) {
			t.Fatalf("cancelled existing Provision() = (%#v, %v)", result, err)
		}
		state, err := inspectMeshEndpointState(first.NodeState, first.Node.PeerID())
		if err != nil || state.stateKind() != meshEndpointStatePending {
			t.Fatalf("cancelled existing endpoint state = (%d, %v)", state.stateKind(), err)
		}
	})
}

func TestProvisionObservesTerminalCancellationAfterEndpointPublication(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	endpointPath := filepath.Join(workspace, ".mnemon", "harness", "node", meshEndpointPendingName)
	ctx := &endpointPublicationContext{Context: context.Background(), endpointPath: endpointPath,
		done: make(chan struct{})}
	result, err := Provision(ctx, provisionTestOptions(t, workspace, model.HostCodex))
	if result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) || !errors.Is(err, context.Canceled) {
		t.Fatalf("terminally cancelled Provision() = (%#v, %v)", result, err)
	}
	identity, loadErr := LoadIdentity(filepath.Dir(endpointPath))
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	state, inspectErr := inspectMeshEndpointState(filepath.Dir(endpointPath), identity.PeerID())
	if inspectErr != nil || state.stateKind() != meshEndpointStatePending {
		t.Fatalf("published endpoint after cancellation = (%d, %v)", state.stateKind(), inspectErr)
	}
}

func TestProvisionClearsResultWhenEnsureLockReleaseFails(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	options := provisionTestOptions(t, workspace, model.HostCodex)
	cause := errors.New("injected ensure lock close failure")
	result, err := provision(context.Background(), options,
		failingCloseProvisionEnsureOwner{cause: cause})
	if result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) || !errors.Is(err, cause) {
		t.Fatalf("provision() = (%#v, %v)", result, err)
	}
	replayed, err := Provision(context.Background(), options)
	if err != nil || replayed.Created {
		t.Fatalf("Provision() after close failure = (%#v, %v)", replayed, err)
	}
}

func TestProvisionRecoversEveryMeshEndpointBootstrapState(t *testing.T) {
	states := []struct {
		name string
		kind meshEndpointStateKind
	}{
		{name: "absent", kind: meshEndpointStatePending},
		{name: "pending", kind: meshEndpointStatePending},
		{name: "final with pending", kind: meshEndpointStateFinalWithPending},
		{name: "final", kind: meshEndpointStateFinal},
	}
	for index, tc := range states {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			workspace := newProvisionWorkspace(t)
			options := provisionTestOptions(t, workspace, model.HostCodex)
			first, err := Provision(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			desired := mustMeshEndpointPending(t, first.Node.PeerID(),
				defaultProvisionMeshListener, nil)
			port := 4401 + index
			final := mustMeshEndpoint(t, first.Node.PeerID(),
				fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
				[]string{fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)})
			switch tc.name {
			case "absent":
				if err := os.Remove(filepath.Join(first.NodeState, meshEndpointPendingName)); err != nil {
					t.Fatal(err)
				}
			case "final with pending", "final":
				created, err := publishMeshEndpointFinal(first.NodeState, desired, final)
				if err != nil || !created {
					t.Fatalf("publish final = (%t, %v)", created, err)
				}
				if tc.name == "final" {
					if err := retireMeshEndpointPending(first.NodeState, desired, final); err != nil {
						t.Fatal(err)
					}
				}
			}
			options.Credentials = verifyOnlyProvisionCredentials{}
			replayed, err := Provision(context.Background(), options)
			if err != nil || replayed.Created || replayed.CredentialCreated ||
				replayed.Node.PeerID() != first.Node.PeerID() {
				t.Fatalf("replayed Provision() = (%#v, %v)", replayed, err)
			}
			state, err := inspectMeshEndpointState(first.NodeState, first.Node.PeerID())
			if err != nil || state.stateKind() != tc.kind {
				t.Fatalf("recovered endpoint state = (%d, %v), want %d",
					state.stateKind(), err, tc.kind)
			}
		})
	}
}

func TestProvisionRejectsProjectionAndHostAuthorityDrift(t *testing.T) {
	t.Run("missing identity is not repaired", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		first, err := Provision(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		identityPath := filepath.Join(first.NodeState, identityKeyName)
		if err := os.Remove(identityPath); err != nil {
			t.Fatal(err)
		}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("missing identity Provision() error = %v", err)
		}
		if _, err := os.Lstat(identityPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("existing Provision repaired identity: %v", err)
		}
	})
	t.Run("identity replacement", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		first, err := Provision(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(first.NodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(first.NodeState); err != nil {
			t.Fatal(err)
		}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("identity drift Provision() error = %v", err)
		}
	})
	t.Run("enabled Host switch", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		first, err := Provision(context.Background(), options)
		if err != nil {
			t.Fatal(err)
		}
		st, err := store.Open(context.Background(), filepath.Join(first.NodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		spec := first.Profile.Spec()
		spec.Enabled = true
		spec.UpdatedAt = first.Profile.UpdatedAt().Add(time.Second)
		enabled, _ := model.NewProfile(spec)
		if _, err := st.ActivateProfile(context.Background(), enabled,
			first.Profile.UpdatedAt(), enabled.UpdatedAt()); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		options.Host = model.HostClaudeCode
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("Host switch Provision() error = %v", err)
		}
	})
}

func TestProvisionRejectsUnsafeWorkspaceStateAndInvalidClock(t *testing.T) {
	t.Run("relative workspace", func(t *testing.T) {
		options := ProvisionOptions{Workspace: ".", Host: model.HostCodex, AssetRevision: "asset-r5"}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("Provision() error = %v", err)
		}
	})
	t.Run("Harness symlink", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		if err := os.Mkdir(filepath.Join(workspace, ".mnemon"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(workspace, ".mnemon", "harness")); err != nil {
			t.Fatal(err)
		}
		if _, err := Provision(context.Background(), provisionTestOptions(t, workspace, model.HostCodex)); !errors.Is(err, ErrProvision) {
			t.Fatalf("Provision() error = %v", err)
		}
	})
	t.Run("invalid clock", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		options := provisionTestOptions(t, workspace, model.HostCodex)
		options.Clock = controllerTestClock{}
		if _, err := Provision(context.Background(), options); !errors.Is(err, ErrProvision) {
			t.Fatalf("Provision() error = %v", err)
		}
	})
	t.Run("existing safe Mnemon directory", func(t *testing.T) {
		workspace := newProvisionWorkspace(t)
		mnemonDir := filepath.Join(workspace, ".mnemon")
		if err := os.Mkdir(mnemonDir, 0o755); err != nil {
			t.Fatal(err)
		}
		result, err := Provision(context.Background(), provisionTestOptions(t, workspace, model.HostCodex))
		if err != nil {
			t.Fatal(err)
		}
		info, _ := os.Stat(mnemonDir)
		if info.Mode().Perm() != 0o755 || result.NodeState == "" {
			t.Fatalf("existing .mnemon mode/result = %04o %#v", info.Mode().Perm(), result)
		}
	})
}

func provisionTestOptions(t *testing.T, workspace string, host model.HostKind) ProvisionOptions {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	return ProvisionOptions{Workspace: workspace, Host: host,
		AssetRevision: bundle.Manifest().AssetRevision,
		Clock:         controllerTestClock{time.Date(2026, 7, 17, 7, 0, 0, 0, time.UTC)},
		Credentials:   testProfileCredentials{}}
}

func newProvisionWorkspace(t *testing.T) string {
	t.Helper()
	workspace := t.TempDir()
	if err := os.Chmod(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	return workspace
}

func assertProvisionModes(t *testing.T, workspace, nodeState string) {
	t.Helper()
	for _, path := range []string{filepath.Join(workspace, ".mnemon", "harness"), nodeState,
		filepath.Join(nodeState, "profiles")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("owner directory %s = %v, %v", path, info, err)
		}
	}
	for _, path := range []string{filepath.Join(nodeState, identityKeyName), filepath.Join(nodeState, "node.db"),
		filepath.Join(nodeState, "node.db.writer.lock"), filepath.Join(nodeState, ensureLockName),
		filepath.Join(nodeState, meshEndpointPendingName),
		filepath.Join(nodeState, "profiles", model.TeamworkProfileID().String()+".token")} {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("private file %s = %v, %v", path, info, err)
		}
	}
}

type cancelingProvisionCredentials struct{ cancel func() }

func (cancelingProvisionCredentials) Ensure(string) (model.Digest, bool, error) {
	return model.Digest{}, false, errors.New("existing Provision called credential Ensure")
}

func (credentials cancelingProvisionCredentials) Verify(nodeState string, expected model.Digest) error {
	if err := localapi.VerifyProfileCredential(nodeState, expected); err != nil {
		return err
	}
	credentials.cancel()
	return nil
}

type endpointPublicationContext struct {
	context.Context
	endpointPath string
	done         chan struct{}
	once         sync.Once
}

func (ctx *endpointPublicationContext) Done() <-chan struct{} {
	if _, err := os.Lstat(ctx.endpointPath); err == nil {
		ctx.once.Do(func() { close(ctx.done) })
	}
	return ctx.done
}

func (ctx *endpointPublicationContext) Err() error {
	_ = ctx.Done()
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

type provisionSignalClock struct {
	at     time.Time
	called chan<- struct{}
}

type failingCloseProvisionEnsureOwner struct{ cause error }

func (failingCloseProvisionEnsureOwner) acquire(ctx context.Context,
	nodeState string,
) (*ensureLock, error) {
	return acquireProvisionEnsureLock(ctx, nodeState)
}

func (owner failingCloseProvisionEnsureOwner) close(lock *ensureLock) error {
	return errors.Join(lock.close(), owner.cause)
}

func (clock provisionSignalClock) Now() time.Time {
	close(clock.called)
	return clock.at
}

func assertProvisionAuthorityAbsent(t *testing.T, nodeState string) {
	t.Helper()
	for _, name := range []string{identityKeyName, "node.db", "node.db.writer.lock", "profiles",
		meshEndpointPendingName, meshEndpointName} {
		if _, err := os.Lstat(filepath.Join(nodeState, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("contended Provision created %s: %v", name, err)
		}
	}
}
