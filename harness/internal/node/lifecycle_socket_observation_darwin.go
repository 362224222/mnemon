//go:build darwin

package node

import (
	"errors"
	"os"
)

// observeLifecycleSocket uses a validated path-relative snapshot because
// Darwin rejects open(2) on Unix-domain socket filesystem entries. The held
// lifecycle lease excludes authorized replacement; callers additionally
// require replacement detection, offline writer proof, and final absence.
func observeLifecycleSocket(state *identityNodeState) (
	*controlSocketObservation, bool, error,
) {
	info, err := state.root.Lstat(controlSocketName)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if err := validateLifecycleSocket(info, state.ownerUID); err != nil {
		return nil, false, err
	}
	return &controlSocketObservation{identity: info}, true, nil
}
