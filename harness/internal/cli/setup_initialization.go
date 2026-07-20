package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type setupAuthorityObservation struct {
	authority localapi.AuthorityResponse
	client    setupAuthorityClient
	source    setupAuthoritySource
	found     bool
	fallback  *localapi.APIError
}

type setupAuthoritySource uint8

const (
	setupAuthorityOnline setupAuthoritySource = iota + 1
	setupAuthorityOffline
)

// observeSetupAuthority distinguishes the live control plane from the closed
// companion. An offline authority is never merely trusted: Initialize must
// first replay Provision so a crash between Store initialization and mesh
// endpoint publication converges before setup performs any later mutation.
func (app *setupApp) observeSetupAuthority(ctx context.Context, nodeState string,
	companion setupCompanion,
) (setupAuthorityObservation, *localapi.APIError) {
	observed, terminal := app.observeAuthority(ctx, nodeState, companion)
	if terminal != nil || !observed.found || observed.source == setupAuthorityOnline {
		return observed, terminal
	}
	durable := observed.authority
	rawBefore, err := canonicalSetupAuthority(durable)
	if err != nil {
		return setupAuthorityObservation{}, setupAuthError("managed Node authority is invalid")
	}
	replayed, err := companion.Initialize(ctx, model.HostKind(durable.Host),
		durable.AssetRevision)
	if err != nil || replayed.Created || replayed.Host != durable.Host ||
		replayed.AssetRevision != durable.AssetRevision ||
		replayed.SchemaVersion != model.SchemaVersion || replayed.Status != "initialized" {
		return setupAuthorityObservation{}, setupAuthError(
			"managed Node initialization replay failed")
	}
	after, err := companion.Inspect(ctx)
	if err != nil {
		return setupAuthorityObservation{}, setupAuthError(
			"managed Node authority could not be inspected")
	}
	rawAfter, err := canonicalSetupAuthority(after)
	if err != nil || !bytes.Equal(rawBefore, rawAfter) {
		return setupAuthorityObservation{}, setupAuthError(
			"managed Node initialization replay changed authority")
	}
	observed.authority = after
	return observed, nil
}

func (app *setupApp) observeAuthority(ctx context.Context, nodeState string,
	companion setupCompanion,
) (setupAuthorityObservation, *localapi.APIError) {
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		return setupAuthorityObservation{fallback: setupAuthError(
			"managed Profile credential is unavailable")}, nil
	}
	authority, apiErr := client.ReadAuthority(ctx)
	if apiErr == nil {
		return setupAuthorityObservation{authority: authority, client: client,
			source: setupAuthorityOnline, found: true}, nil
	}
	if apiErr.Code != localapi.CodeMnemondUnavailable {
		return setupAuthorityObservation{}, normalizeSetupAPIError(apiErr)
	}
	authority, err = companion.Inspect(ctx)
	if err != nil {
		return setupAuthorityObservation{client: client, fallback: setupAuthError(
			"managed Node authority could not be inspected")}, nil
	}
	return setupAuthorityObservation{authority: authority, client: client,
		source: setupAuthorityOffline, found: true}, nil
}

func canonicalSetupAuthority(authority localapi.AuthorityResponse) ([]byte, error) {
	if _, err := localapi.AuthorityDigest(authority); err != nil {
		return nil, err
	}
	return model.CanonicalMarshal(authority)
}

// setupCanInitialize permits only two closed crash states: no database with
// either no credential or one valid projected credential, and an exact R5
// Store whose Node/Profile pair is wholly absent. Existing authority returns
// false; partial, corrupt, version-zero, or unsafe authority returns an error.
func setupCanInitialize(nodeState string) (bool, error) {
	state, err := openSetupLockNodeState(nodeState)
	if err != nil {
		return false, err
	}
	if err := state.close(); err != nil {
		return false, err
	}
	path := filepath.Join(nodeState, "node.db")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return setupCanInitializeCredential(nodeState)
	}
	if err != nil {
		return false, err
	}
	if _, err := validateSetupOwnerPath(info, 0o600, false); err != nil {
		return false, err
	}
	fresh, err := setupClassifyExistingStore(path, info)
	if err != nil || !fresh {
		return false, err
	}
	return setupCanInitializeCredential(nodeState)
}

func setupCanInitializeCredential(nodeState string) (bool, error) {
	credential := filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+".token")
	if _, err := os.Lstat(credential); errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	// A projected credential may legitimately precede node.db after a crashed
	// initialize. The closed client validator freezes its owner, mode, shape,
	// and digest before Provision is allowed to reuse it.
	if _, err := localapi.NewClient(nodeState); err != nil {
		return false, nil
	}
	return true, nil
}

func setupClassifyExistingStore(path string, databaseBefore os.FileInfo) (bool, error) {
	guardPath := path + ".writer.lock"
	guardBefore, err := os.Lstat(guardPath)
	if err != nil {
		return false, err
	}
	if _, err := validateSetupOwnerPath(guardBefore, 0o600, false); err != nil {
		return false, err
	}
	st, err := store.OpenExisting(context.Background(), path)
	if err != nil {
		return false, err
	}
	classification, classifyErr := st.ClassifyNodeInitialization(context.Background())
	databaseAfter, databaseErr := os.Lstat(path)
	if databaseErr == nil && !os.SameFile(databaseBefore, databaseAfter) {
		databaseErr = errors.New("Store database identity changed")
	}
	if databaseErr == nil {
		_, databaseErr = validateSetupOwnerPath(databaseAfter, 0o600, false)
	}
	guardAfter, guardErr := os.Lstat(guardPath)
	if guardErr == nil && !os.SameFile(guardBefore, guardAfter) {
		guardErr = errors.New("Store writer guard identity changed")
	}
	if guardErr == nil {
		_, guardErr = validateSetupOwnerPath(guardAfter, 0o600, false)
	}
	closeErr := st.Close()
	if classifyErr != nil || databaseErr != nil || guardErr != nil || closeErr != nil {
		return false, errors.Join(classifyErr, databaseErr, guardErr, closeErr)
	}
	switch classification {
	case store.NodeInitializationFresh:
		return true, nil
	case store.NodeInitializationExisting:
		return false, nil
	default:
		return false, errors.New("Store returned an unknown initialization state")
	}
}
