package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrProfileAuthentication = errors.New("Profile authentication failed")

// AuthenticateProfile maps a transport credential digest to durable Profile
// authority without leaking whether a Profile exists or which field differed.
// Activation is deliberately checked by the caller after authentication so a
// valid but drifted installation receives the stable activation diagnostic.
func (s *Store) AuthenticateProfile(ctx context.Context, credential model.Digest) (model.Profile, error) {
	if s == nil || s.db == nil || ctx == nil || credential.IsZero() {
		return model.Profile{}, ErrProfileAuthentication
	}
	profile, err := readProfile(ctx, s.db)
	if err != nil {
		return model.Profile{}, fmt.Errorf("%w: durable authority unavailable", ErrProfileAuthentication)
	}
	want := profile.CredentialHash().Bytes()
	got := credential.Bytes()
	matched := subtle.ConstantTimeCompare(want, got)
	clear(want)
	clear(got)
	if matched != 1 {
		return model.Profile{}, ErrProfileAuthentication
	}
	return profile, nil
}
