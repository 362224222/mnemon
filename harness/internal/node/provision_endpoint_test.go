package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReconcileProvisionMeshEndpointRejectsContentDrift(t *testing.T) {
	states := []struct {
		name string
		kind meshEndpointStateKind
	}{
		{name: "absent observation", kind: meshEndpointStateAbsent},
		{name: "pending observation", kind: meshEndpointStatePending},
		{name: "final with pending observation", kind: meshEndpointStateFinalWithPending},
		{name: "final observation", kind: meshEndpointStateFinal},
	}
	for _, tc := range states {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			workspace := newProvisionWorkspace(t)
			first, err := Provision(context.Background(), provisionTestOptions(t, workspace, model.HostCodex))
			if err != nil {
				t.Fatal(err)
			}
			desired := mustMeshEndpointPending(t, first.Node.PeerID(), defaultProvisionMeshListener, nil)
			firstFinal := mustMeshEndpoint(t, first.Node.PeerID(), "/ip4/0.0.0.0/tcp/4601",
				[]string{"/ip4/127.0.0.1/tcp/4601"})
			setProvisionEndpointState(t, first.NodeState, desired, firstFinal, tc.kind)
			desired, frozen, err := inspectProvisionMeshEndpoint(first.NodeState, first.Node.PeerID())
			if err != nil || frozen.stateKind() != tc.kind {
				t.Fatalf("inspectProvisionMeshEndpoint() kind = (%d, %v)", frozen.stateKind(), err)
			}

			secondPending := mustMeshEndpointPending(t, first.Node.PeerID(),
				"/ip4/0.0.0.0/tcp/4602", nil)
			secondFinal := mustMeshEndpoint(t, first.Node.PeerID(), "/ip4/0.0.0.0/tcp/4602",
				[]string{"/ip4/127.0.0.1/tcp/4602"})
			driftProvisionEndpointState(t, first.NodeState, desired, secondPending, secondFinal, tc.kind)
			drifted, err := inspectMeshEndpointState(first.NodeState, first.Node.PeerID())
			if err != nil {
				t.Fatal(err)
			}
			if err := reconcileProvisionMeshEndpoint(first.NodeState, desired, frozen); !errors.Is(err, errMeshEndpointConflict) {
				t.Fatalf("reconcileProvisionMeshEndpoint() error = %v", err)
			}
			current, err := inspectMeshEndpointState(first.NodeState, first.Node.PeerID())
			if err != nil || !sameProvisionMeshEndpointState(current, drifted) {
				t.Fatalf("reconcile replaced drift: state=%d err=%v", current.stateKind(), err)
			}
		})
	}
}

func setProvisionEndpointState(t *testing.T, nodeState string, pending meshEndpointPending,
	final meshEndpoint, kind meshEndpointStateKind,
) {
	t.Helper()
	switch kind {
	case meshEndpointStateAbsent:
		removeProvisionEndpointFile(t, nodeState, meshEndpointPendingName)
	case meshEndpointStatePending:
		return
	case meshEndpointStateFinalWithPending, meshEndpointStateFinal:
		if created, err := publishMeshEndpointFinal(nodeState, pending, final); err != nil || !created {
			t.Fatalf("publish first final = (%t, %v)", created, err)
		}
		if kind == meshEndpointStateFinal {
			if err := retireMeshEndpointPending(nodeState, pending, final); err != nil {
				t.Fatal(err)
			}
		}
	default:
		t.Fatalf("unsupported endpoint state %d", kind)
	}
}

func driftProvisionEndpointState(t *testing.T, nodeState string, desired,
	secondPending meshEndpointPending, secondFinal meshEndpoint, kind meshEndpointStateKind,
) {
	t.Helper()
	switch kind {
	case meshEndpointStateAbsent:
		publishProvisionPending(t, nodeState, secondPending)
	case meshEndpointStatePending:
		removeProvisionEndpointFile(t, nodeState, meshEndpointPendingName)
		publishProvisionPending(t, nodeState, secondPending)
	case meshEndpointStateFinalWithPending:
		removeProvisionEndpointFile(t, nodeState, meshEndpointName)
		publishProvisionFinal(t, nodeState, desired, secondFinal)
	case meshEndpointStateFinal:
		removeProvisionEndpointFile(t, nodeState, meshEndpointName)
		publishProvisionPending(t, nodeState, desired)
		publishProvisionFinal(t, nodeState, desired, secondFinal)
		if err := retireMeshEndpointPending(nodeState, desired, secondFinal); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported endpoint state %d", kind)
	}
}

func publishProvisionPending(t *testing.T, nodeState string, pending meshEndpointPending) {
	t.Helper()
	if created, err := publishMeshEndpointPending(nodeState, pending); err != nil || !created {
		t.Fatalf("publish drifted pending = (%t, %v)", created, err)
	}
}

func publishProvisionFinal(t *testing.T, nodeState string, pending meshEndpointPending,
	final meshEndpoint,
) {
	t.Helper()
	if created, err := publishMeshEndpointFinal(nodeState, pending, final); err != nil || !created {
		t.Fatalf("publish drifted final = (%t, %v)", created, err)
	}
}

func removeProvisionEndpointFile(t *testing.T, nodeState, name string) {
	t.Helper()
	if err := os.Remove(filepath.Join(nodeState, name)); err != nil {
		t.Fatal(err)
	}
}
