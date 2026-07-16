package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrLocalAuthority = errors.New("local Node authority is unavailable")

// LocalAuthority is one read-only snapshot of the singleton Node and its T0
// Teamwork Profile. It is the daemon composition input, not a second config
// source; all mutations remain on the existing Store transaction paths.
type LocalAuthority struct {
	Node    model.Node
	Profile model.Profile
}

func (s *Store) ReadLocalAuthority(ctx context.Context) (LocalAuthority, error) {
	if s == nil || s.db == nil || ctx == nil {
		return LocalAuthority{}, fmt.Errorf("%w: Store is unavailable", ErrLocalAuthority)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return LocalAuthority{}, fmt.Errorf("%w: begin: %v", ErrLocalAuthority, err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return LocalAuthority{}, fmt.Errorf("%w: read Node: %v", ErrLocalAuthority, err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return LocalAuthority{}, fmt.Errorf("%w: read Profile: %v", ErrLocalAuthority, err)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return LocalAuthority{}, fmt.Errorf("%w: Node and Profile asset revisions differ", ErrLocalAuthority)
	}
	if err := tx.Commit(); err != nil {
		return LocalAuthority{}, fmt.Errorf("%w: commit read: %v", ErrLocalAuthority, err)
	}
	return LocalAuthority{Node: node, Profile: profile}, nil
}
