package node

import (
	"bytes"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// inspectProvisionMeshEndpoint freezes the private endpoint authority while no
// Store writer is held. Provision later proves the matching Store preimage and
// reconciles only from this exact observation while it still owns ensure.lock.
func inspectProvisionMeshEndpoint(nodeState string, peerID model.PeerID) (
	meshEndpointPending, meshEndpointState, error,
) {
	desired, err := newMeshEndpointPending(meshEndpointPendingSpec{PeerID: peerID,
		ListenAddrs: []string{defaultProvisionMeshListener}})
	if err != nil {
		return meshEndpointPending{}, meshEndpointState{}, err
	}
	frozen, err := inspectMeshEndpointState(nodeState, peerID)
	if err != nil {
		return meshEndpointPending{}, meshEndpointState{}, err
	}
	if !compatibleProvisionMeshEndpoint(desired, frozen) {
		return meshEndpointPending{}, meshEndpointState{}, errMeshEndpointConflict
	}
	return desired, frozen, nil
}

func reconcileProvisionMeshEndpoint(nodeState string, desired meshEndpointPending,
	frozen meshEndpointState,
) error {
	if !compatibleProvisionMeshEndpoint(desired, frozen) {
		return errMeshEndpointConflict
	}
	if frozen.stateKind() == meshEndpointStateAbsent {
		if _, err := publishMeshEndpointPending(nodeState, desired); err != nil {
			return err
		}
	}
	current, err := inspectMeshEndpointState(nodeState, desired.peerIDValue())
	if err != nil {
		return err
	}
	if frozen.stateKind() == meshEndpointStateAbsent {
		pending, ok := current.pendingAuthority()
		if !ok || current.stateKind() != meshEndpointStatePending ||
			!bytes.Equal(pending.canonicalJSON(), desired.canonicalJSON()) {
			return errMeshEndpointConflict
		}
		return nil
	}
	if !sameProvisionMeshEndpointState(current, frozen) {
		return errMeshEndpointConflict
	}
	return nil
}

func compatibleProvisionMeshEndpoint(desired meshEndpointPending, state meshEndpointState) bool {
	switch state.stateKind() {
	case meshEndpointStateAbsent:
		return true
	case meshEndpointStatePending:
		pending, _ := state.pendingAuthority()
		return bytes.Equal(pending.canonicalJSON(), desired.canonicalJSON())
	case meshEndpointStateFinalWithPending:
		pending, _ := state.pendingAuthority()
		final, _ := state.finalAuthority()
		return bytes.Equal(pending.canonicalJSON(), desired.canonicalJSON()) &&
			meshEndpointAdvances(pending.value, final.value)
	case meshEndpointStateFinal:
		final, _ := state.finalAuthority()
		return meshEndpointAdvances(desired.value, final.value)
	default:
		return false
	}
}

func sameProvisionMeshEndpointState(left, right meshEndpointState) bool {
	if left.stateKind() != right.stateKind() {
		return false
	}
	leftPending, leftHasPending := left.pendingAuthority()
	rightPending, rightHasPending := right.pendingAuthority()
	leftFinal, leftHasFinal := left.finalAuthority()
	rightFinal, rightHasFinal := right.finalAuthority()
	return leftHasPending == rightHasPending && leftHasFinal == rightHasFinal &&
		(!leftHasPending || bytes.Equal(leftPending.canonicalJSON(), rightPending.canonicalJSON())) &&
		(!leftHasFinal || bytes.Equal(leftFinal.canonicalJSON(), rightFinal.canonicalJSON()))
}
