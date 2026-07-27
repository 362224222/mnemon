package artifact

import (
	"errors"
	"time"
)

var ErrStagingStoreInvariant = errors.New("Artifact staging Store invariant")

// StagingSweepSpec bounds the compatibility sweep for expired relational
// staging ownership. Physical owner directories use the Store cleanup claim
// protocol instead.
type StagingSweepSpec struct {
	Cutoff   time.Time
	At       time.Time
	MaxRoots int
}

// StagingSweepResult contains only the remaining compatibility sweep result.
type StagingSweepResult struct {
	ExpiredPins      int
	OwnerProjections int
	Roots            int
	Blocks           int
}
