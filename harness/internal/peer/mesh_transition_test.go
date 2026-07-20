package peer

import (
	"errors"
	"testing"
)

func TestMeshAuthorityTransitionRejectsZeroAuthority(t *testing.T) {
	var transition *MeshAuthorityTransition
	if !errors.Is(transition.Wait(), ErrMeshRuntime) ||
		!errors.Is(transition.Install(), ErrMeshRuntime) ||
		!errors.Is(transition.Abort(), ErrMeshRuntime) {
		t.Fatal("zero Mesh authority transition did not fail closed")
	}
	select {
	case <-transition.Done():
	default:
		t.Fatal("zero Mesh authority transition Done did not return a closed channel")
	}
}
