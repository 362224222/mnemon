package node

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenDaemonDataPlaneRejectsMissingAuthority(t *testing.T) {
	t.Parallel()
	if runtime, err := openDaemonDataPlane(context.Background(), context.Background(),
		nil, nil, nil, nil, nil); err == nil || runtime != nil {
		t.Fatalf("openDaemonDataPlane() = (%#v, %v)", runtime, err)
	}
}

func TestDaemonInboxTriggerCoalescesEveryOwnedWorker(t *testing.T) {
	t.Parallel()
	first, second := &dataPlaneTriggerFixture{}, &dataPlaneTriggerFixture{}
	daemonInboxTrigger{first, nil, second}.Trigger()
	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("Trigger calls = (%d, %d)", first.calls, second.calls)
	}
}

type dataPlaneTriggerFixture struct{ calls int }

func (trigger *dataPlaneTriggerFixture) Trigger() { trigger.calls++ }

func TestDaemonDataPlaneConvergesDirectionalChannelBaselines(t *testing.T) {
	ownerFixture := newDaemonFixture(t, true)
	joinerFixture := newDaemonFixture(t, true)
	owner := openServingDataPlaneDaemon(t, ownerFixture)
	joiner := openServingDataPlaneDaemon(t, joinerFixture)
	ownerClient, err := NewClient(ownerFixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	joinerClient, err := NewClient(joinerFixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	created, apiErr := ownerClient.CreateChannel(context.Background(),
		ChannelCreateRequest{Name: "data-plane-integration"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := joinerClient.JoinChannel(context.Background(),
		ChannelJoinRequest{Token: created.InviteToken}); apiErr != nil {
		t.Fatal(apiErr)
	}
	waitForConvergedDataPlaneChannel(t, ownerClient, owner)
	waitForConvergedDataPlaneChannel(t, joinerClient, joiner)
	if err := owner.Daemon.Close(); err != nil {
		t.Fatal(err)
	}
	if err := joiner.Daemon.Close(); err != nil {
		t.Fatal(err)
	}
}

type servingDataPlaneDaemon struct {
	*Daemon
	serveErr <-chan error
}

func openServingDataPlaneDaemon(t *testing.T, fixture daemonFixture) *servingDataPlaneDaemon {
	t.Helper()
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: wallClock{}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- daemon.Serve(context.Background()) }()
	waitControllerSocket(t, filepath.Join(daemon.NodeState(), "control.sock"), serveErr)
	t.Cleanup(func() {
		if err := daemon.Close(); err != nil {
			t.Errorf("close data-plane daemon: %v", err)
		}
	})
	return &servingDataPlaneDaemon{Daemon: daemon, serveErr: serveErr}
}

func waitForConvergedDataPlaneChannel(t *testing.T, client *Client,
	daemon *servingDataPlaneDaemon,
) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last ChannelStatusResponse
	var lastErr *APIError
	for time.Now().Before(deadline) {
		select {
		case err := <-daemon.serveErr:
			t.Fatalf("data-plane daemon exited before convergence: %v", err)
		default:
		}
		status, apiErr := client.ReadChannelStatus(context.Background())
		last, lastErr = status, apiErr
		if apiErr == nil && len(status.Channels) == 1 &&
			status.Channels[0].Topic.Status == "joined" &&
			status.Channels[0].Topic.ReadyMembers == 2 {
			ready := true
			for _, member := range status.Channels[0].Members {
				if !member.BaselineReady {
					ready = false
					break
				}
			}
			if ready {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	var members ChannelMemberReconcilerSnapshot
	for _, spec := range daemon.channels.data.plane.workers {
		if worker, ok := spec.worker.(*ChannelMemberReconciler); ok {
			members = worker.Snapshot()
		}
	}
	t.Fatalf("Channel did not converge both directional baselines: status=%#v api_error=%v worker=%#v",
		last, lastErr, members)
}
