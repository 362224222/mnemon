package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub"
)

// replicas.json is the mnemon-hub form of the replica grant (sync-abi-v1 §2, dual-form rule): the same
// fields and semantics as a replica-agent channel binding entry — principal, credential_ref, scopes.
// It is operator-supplied (nothing writes it); rotation = edit the credential file + restart.

type replicasDoc struct {
	SchemaVersion int            `json:"schema_version"`
	Replicas      []replicaEntry `json:"replicas"`
}

type replicaEntry struct {
	Principal     string       `json:"principal"`
	CredentialRef string       `json:"credential_ref"`
	Scopes        []replicaRef `json:"scopes"`
}

type replicaRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// loadReplicas reads + validates replicas.json fail-closed (house decoder rules: unknown fields
// rejected) and assembles the grant map + the bearer token->principal map. Fail-closed gates:
// the file must not be world-readable (it names the hub's credential files — keep it 0600 in a
// 0700 dir, mirroring the channel credential posture); every entry needs a principal, a
// credential_ref, and a NON-EMPTY scope list (an empty grant would fail open on pull); principals
// and tokens must be unique. credential_ref resolves relative to the replicas.json directory
// (or absolute).
func loadReplicas(path string) (mnemonhub.GrantMap, map[string]contract.ActorID, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("stat replicas config: %w", err)
	}
	if info.Mode().Perm()&0o004 != 0 {
		return nil, nil, fmt.Errorf("replicas config %s is world-readable (mode %04o); chmod it to 0600 (dir 0700)", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read replicas config: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var doc replicasDoc
	if err := dec.Decode(&doc); err != nil {
		return nil, nil, fmt.Errorf("parse replicas config %s: %w", path, err)
	}
	if doc.SchemaVersion != 1 {
		return nil, nil, fmt.Errorf("replicas config schema_version %d unsupported (want 1)", doc.SchemaVersion)
	}
	if len(doc.Replicas) == 0 {
		return nil, nil, fmt.Errorf("replicas config %s declares no replicas", path)
	}
	grants := mnemonhub.GrantMap{}
	tokens := map[string]contract.ActorID{}
	baseDir := filepath.Dir(path)
	for i, e := range doc.Replicas {
		principal := contract.ActorID(strings.TrimSpace(e.Principal))
		if principal == "" {
			return nil, nil, fmt.Errorf("replica[%d]: principal is required", i)
		}
		if _, dup := grants[principal]; dup {
			return nil, nil, fmt.Errorf("replica[%d]: duplicate principal %q", i, principal)
		}
		if strings.TrimSpace(e.CredentialRef) == "" {
			return nil, nil, fmt.Errorf("replica[%d] (%s): credential_ref is required", i, principal)
		}
		if len(e.Scopes) == 0 {
			return nil, nil, fmt.Errorf("replica[%d] (%s): scopes must be non-empty (fail closed)", i, principal)
		}
		scopes := make([]contract.ResourceRef, 0, len(e.Scopes))
		for _, s := range e.Scopes {
			if strings.TrimSpace(s.Kind) == "" || strings.TrimSpace(s.ID) == "" {
				return nil, nil, fmt.Errorf("replica[%d] (%s): scope entries require kind and id", i, principal)
			}
			scopes = append(scopes, contract.ResourceRef{Kind: contract.ResourceKind(s.Kind), ID: contract.ResourceID(s.ID)})
		}
		tokPath := e.CredentialRef
		if !filepath.IsAbs(tokPath) {
			tokPath = filepath.Join(baseDir, tokPath)
		}
		// The credential file holds the ACTUAL bearer secret — guard it like replicas.json itself
		// (which only NAMES these files). A world-readable token leaks the credential to any local
		// user, so refuse it fail-closed (keep it 0600 in a 0700 dir).
		if tokInfo, err := os.Stat(tokPath); err != nil {
			return nil, nil, fmt.Errorf("replica[%d] (%s): stat credential_ref %s: %w", i, principal, e.CredentialRef, err)
		} else if tokInfo.Mode().Perm()&0o004 != 0 {
			return nil, nil, fmt.Errorf("credential file %s is world-readable; chmod 0600", tokPath)
		}
		tokRaw, err := os.ReadFile(tokPath)
		if err != nil {
			return nil, nil, fmt.Errorf("replica[%d] (%s): read credential_ref %s: %w", i, principal, e.CredentialRef, err)
		}
		tok := strings.TrimSpace(string(tokRaw))
		if tok == "" {
			return nil, nil, fmt.Errorf("replica[%d] (%s): credential_ref %s is empty", i, principal, e.CredentialRef)
		}
		if owner, clash := tokens[tok]; clash {
			return nil, nil, fmt.Errorf("replica[%d] (%s): bearer token also bound to %q", i, principal, owner)
		}
		tokens[tok] = principal
		grants[principal] = contract.ReplicaGrant{Principal: principal, Token: tok, Scopes: scopes}
	}
	return grants, tokens, nil
}
