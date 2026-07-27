package node

import (
	"sync"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
)

type testArtifactTransferObserver struct {
	snapshot StatusArtifactTransferSnapshot
	observed bool
}

func (observer testArtifactTransferObserver) artifactTransferObservation() (StatusArtifactTransferSnapshot, bool) {
	return observer.snapshot, observer.observed
}

func zeroArtifactTransferObserver() artifactTransferObserver {
	return testArtifactTransferObserver{snapshot: StatusArtifactTransferSnapshot{
		MaximumPulls: peer.HermeticLimits().NodeArtifactPulls,
	}, observed: true}
}

type testArtifactReceiverSnapshotter struct {
	mu       sync.Mutex
	snapshot peer.ArtifactReceiverSnapshot
}

func (receiver *testArtifactReceiverSnapshotter) Snapshot() peer.ArtifactReceiverSnapshot {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	return receiver.snapshot
}

func (receiver *testArtifactReceiverSnapshotter) set(snapshot peer.ArtifactReceiverSnapshot) {
	receiver.mu.Lock()
	receiver.snapshot = snapshot
	receiver.mu.Unlock()
}

func TestDaemonDataPlaneArtifactTransferObservationTracksReceiverLifecycle(t *testing.T) {
	t.Parallel()
	maximum := peer.HermeticLimits().NodeArtifactPulls
	receiver := &testArtifactReceiverSnapshotter{
		snapshot: peer.ArtifactReceiverSnapshot{State: peer.ArtifactReceiverIdle},
	}
	runtime := &daemonDataPlaneRuntime{
		artifactReceiver: receiver, maximumArtifactPulls: maximum,
	}
	assertArtifactTransferObservation(t, runtime, 0, maximum)

	receiver.set(peer.ArtifactReceiverSnapshot{
		State: peer.ArtifactReceiverRunning, InFlightPulls: 3,
	})
	assertArtifactTransferObservation(t, runtime, 3, maximum)

	receiver.set(peer.ArtifactReceiverSnapshot{State: peer.ArtifactReceiverStopped})
	assertArtifactTransferObservation(t, runtime, 0, maximum)
}

func TestDaemonDataPlaneArtifactTransferObservationFailsClosed(t *testing.T) {
	t.Parallel()
	maximum := peer.HermeticLimits().NodeArtifactPulls
	tests := []struct {
		name    string
		runtime *daemonDataPlaneRuntime
	}{
		{name: "nil runtime"},
		{name: "missing receiver", runtime: &daemonDataPlaneRuntime{
			maximumArtifactPulls: maximum,
		}},
		{name: "missing limit", runtime: &daemonDataPlaneRuntime{
			artifactReceiver: &testArtifactReceiverSnapshotter{
				snapshot: peer.ArtifactReceiverSnapshot{State: peer.ArtifactReceiverIdle},
			},
		}},
		{name: "oversize limit", runtime: &daemonDataPlaneRuntime{
			artifactReceiver: &testArtifactReceiverSnapshotter{
				snapshot: peer.ArtifactReceiverSnapshot{State: peer.ArtifactReceiverIdle},
			},
			maximumArtifactPulls: maximum + 1,
		}},
		{name: "unknown state", runtime: &daemonDataPlaneRuntime{
			artifactReceiver: &testArtifactReceiverSnapshotter{
				snapshot: peer.ArtifactReceiverSnapshot{
					State: peer.ArtifactReceiverState("unknown"),
				},
			},
			maximumArtifactPulls: maximum,
		}},
		{name: "inactive with pull", runtime: &daemonDataPlaneRuntime{
			artifactReceiver: &testArtifactReceiverSnapshotter{
				snapshot: peer.ArtifactReceiverSnapshot{
					State: peer.ArtifactReceiverIdle, InFlightPulls: 1,
				},
			},
			maximumArtifactPulls: maximum,
		}},
		{name: "negative pulls", runtime: &daemonDataPlaneRuntime{
			artifactReceiver: &testArtifactReceiverSnapshotter{
				snapshot: peer.ArtifactReceiverSnapshot{
					State: peer.ArtifactReceiverRunning, InFlightPulls: -1,
				},
			},
			maximumArtifactPulls: maximum,
		}},
		{name: "oversize pulls", runtime: &daemonDataPlaneRuntime{
			artifactReceiver: &testArtifactReceiverSnapshotter{
				snapshot: peer.ArtifactReceiverSnapshot{
					State: peer.ArtifactReceiverRunning, InFlightPulls: maximum + 1,
				},
			},
			maximumArtifactPulls: maximum,
		}},
	}
	for _, test := range tests {
		if observation, ok := test.runtime.artifactTransferObservation(); ok ||
			observation != (StatusArtifactTransferSnapshot{}) {
			t.Fatalf("%s observation = (%#v, %t)", test.name, observation, ok)
		}
	}
}

func TestStatusArtifactTransferProjectionIsClosedAndBounded(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("artifact-transfer-status")).String()
	maximum := peer.HermeticLimits().NodeArtifactPulls
	for _, active := range []int{0, 2} {
		response, err := NewStatusResponse(StatusSnapshot{
			ArtifactTransfer: StatusArtifactTransferSnapshot{
				ActivePulls: active, MaximumPulls: maximum,
			},
			AssetRevision:   revision,
			ActivationReady: true,
			Runtime: RuntimeStatusSnapshot{
				Running: true, Ready: true, Healthy: true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.ArtifactTransfer == nil ||
			*response.ArtifactTransfer != (StatusArtifactTransfer{ActivePulls: active}) ||
			ValidateStatusArtifactTransfer(*response.ArtifactTransfer) != nil {
			t.Fatalf("active %d projection = %#v", active, response.ArtifactTransfer)
		}
	}

	for _, snapshot := range []StatusArtifactTransferSnapshot{
		{},
		{ActivePulls: -1, MaximumPulls: maximum},
		{ActivePulls: maximum + 1, MaximumPulls: maximum},
		{ActivePulls: 2, MaximumPulls: 1},
		{MaximumPulls: maximum + 1},
	} {
		if _, err := NewStatusResponse(StatusSnapshot{
			ArtifactTransfer: snapshot,
			AssetRevision:    revision,
			ActivationReady:  true,
			Runtime: RuntimeStatusSnapshot{
				Running: true, Ready: true, Healthy: true,
			},
		}); err == nil {
			t.Fatalf("invalid transfer snapshot accepted: %#v", snapshot)
		}
	}
	for _, observation := range []StatusArtifactTransfer{
		{ActivePulls: -1},
		{ActivePulls: maximum + 1},
	} {
		if ValidateStatusArtifactTransfer(observation) == nil {
			t.Fatalf("invalid transfer response accepted: %#v", observation)
		}
	}
	response, err := NewStatusResponse(StatusSnapshot{
		ArtifactTransfer: StatusArtifactTransferSnapshot{MaximumPulls: maximum},
		AssetRevision:    revision,
		ActivationReady:  true,
		Runtime: RuntimeStatusSnapshot{
			Running: true, Ready: true, Healthy: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response.ArtifactTransfer = nil
	if ValidateStatusResponse(response) == nil {
		t.Fatal("status response accepted a missing Artifact transfer observation")
	}
}

func assertArtifactTransferObservation(t *testing.T, runtime *daemonDataPlaneRuntime,
	active, maximum int,
) {
	t.Helper()
	observation, ok := runtime.artifactTransferObservation()
	if !ok || observation != (StatusArtifactTransferSnapshot{
		ActivePulls: active, MaximumPulls: maximum,
	}) {
		t.Fatalf("Artifact transfer observation = (%#v, %t)", observation, ok)
	}
}
