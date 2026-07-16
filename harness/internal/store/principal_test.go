package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAuthenticateProfileUsesConstantIdentityDigest(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	want := model.Sum([]byte("credential-one"))
	insertAuthenticatedProfile(t, st, want, true)
	profile, err := st.AuthenticateProfile(context.Background(), want)
	if err != nil || profile.Principal() != "principal-one" || profile.ID() != model.TeamworkProfileID() {
		t.Fatalf("authenticated Profile = %#v, %v", profile, err)
	}
	if _, err := st.AuthenticateProfile(context.Background(), model.Sum([]byte("wrong"))); !errors.Is(err, ErrProfileAuthentication) {
		t.Fatalf("wrong credential error = %v", err)
	}
	if _, err := st.AuthenticateProfile(context.Background(), model.Digest{}); !errors.Is(err, ErrProfileAuthentication) {
		t.Fatalf("zero credential error = %v", err)
	}
}

func TestAuthenticateProfileReturnsValidDisabledAuthority(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	want := model.Sum([]byte("credential-disabled"))
	insertAuthenticatedProfile(t, st, want, false)
	profile, err := st.AuthenticateProfile(context.Background(), want)
	if err != nil || profile.Enabled() {
		t.Fatalf("disabled authority = enabled %t, error %v", profile.Enabled(), err)
	}
}

func insertAuthenticatedProfile(t *testing.T, st *Store, credential model.Digest, enabled bool) {
	t.Helper()
	if _, err := st.db.Exec(`INSERT INTO profiles(profile_id,principal,workspace_root,host,runtime_kind,
		credential_hash,active_asset_rev,handling_budget_json,enabled,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, model.TeamworkProfileID().String(), "principal-one", "/workspace",
		string(model.HostCodex), string(model.RuntimeCodexAppServer), credential.Bytes(), "asset-one",
		model.DefaultHandlingBudget().JSON().Bytes(), boolInt(enabled), "2026-01-01T00:00:00.000000000Z",
		"2026-01-01T00:00:00.000000000Z"); err != nil {
		t.Fatal(err)
	}
}
