package githubbackend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
)

type Config struct {
	Store  exchange.PublicationStore
	Repo   string
	Branch string
	Scopes []contract.ResourceRef
}

type Backend struct {
	store  exchange.PublicationStore
	repo   string
	branch string
	scopes []contract.ResourceRef
}

func New(cfg Config) (*Backend, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("publication store is required")
	}
	repo := strings.TrimSpace(cfg.Repo)
	if repo == "" {
		return nil, fmt.Errorf("publication repo is required")
	}
	branch, err := exchange.NormalizePublicationBranch(cfg.Branch)
	if err != nil {
		return nil, err
	}
	return &Backend{
		store:  cfg.Store,
		repo:   repo,
		branch: branch,
		scopes: append([]contract.ResourceRef(nil), cfg.Scopes...),
	}, nil
}

func (b *Backend) SyncPush(req contract.SyncPushRequest) (contract.SyncPushResponse, error) {
	replicaID := strings.TrimSpace(req.ReplicaID)
	if replicaID == "" {
		return contract.SyncPushResponse{}, fmt.Errorf("sync push requires replica_id")
	}
	var resp contract.SyncPushResponse
	for _, env := range req.Events {
		material, diagnostic := b.syncedEventMaterial(env)
		if diagnostic != "" {
			resp.Rejected = append(resp.Rejected, eventExchangeResult(env, "rejected", diagnostic))
			continue
		}
		if material.OriginReplicaID != replicaID {
			return contract.SyncPushResponse{}, fmt.Errorf("sync push replica_id %q does not match event origin %q", replicaID, material.OriginReplicaID)
		}
		if diagnostic := validateSyncedMaterial(material); diagnostic != "" {
			resp.Rejected = append(resp.Rejected, eventExchangeResult(env, "rejected", diagnostic))
			continue
		}
		if diagnostic := b.scopeDiagnostic(material.ResourceRef); diagnostic != "" {
			resp.Rejected = append(resp.Rejected, eventExchangeResult(env, "rejected", diagnostic))
			continue
		}
		path, err := exchange.PublicationEventPath(env)
		if err != nil {
			resp.Rejected = append(resp.Rejected, eventExchangeResult(env, "rejected", err.Error()))
			continue
		}
		body, err := json.Marshal(env)
		if err != nil {
			return contract.SyncPushResponse{}, err
		}
		put, err := b.store.PutEvent(context.Background(), b.branch, path, body)
		if err != nil {
			return contract.SyncPushResponse{}, err
		}
		switch {
		case put.Created, put.ExistsSame:
			resp.Accepted = append(resp.Accepted, eventExchangeResult(env, "accepted", ""))
		case put.Conflict:
			resp.Conflicts = append(resp.Conflicts, eventExchangeResult(env, "conflict", "publication event path already exists with different content"))
		default:
			resp.Rejected = append(resp.Rejected, eventExchangeResult(env, "rejected", "publication store returned no put verdict"))
		}
	}
	return resp, nil
}

func (b *Backend) SyncPull(req contract.SyncPullRequest) (contract.SyncPullResponse, error) {
	replicaID := strings.TrimSpace(req.ReplicaID)
	if replicaID == "" {
		return contract.SyncPullResponse{}, fmt.Errorf("sync pull requires replica_id")
	}
	scopes := append([]contract.ResourceRef(nil), req.Scopes...)
	if len(b.scopes) > 0 {
		var err error
		scopes, err = contract.ClampRefs(contract.ActorID("github-publication"), b.scopes, req.Scopes)
		if err != nil {
			return contract.SyncPullResponse{}, fmt.Errorf("sync scope: %w", err)
		}
	}
	list, err := b.store.ListEvents(context.Background(), b.branch, exchange.PublicationEventRoot, req.RemoteCursor)
	if err != nil {
		return contract.SyncPullResponse{}, err
	}
	resp := contract.SyncPullResponse{NextCursor: list.NextCursor}
	for _, stored := range list.Events {
		env, result, ok := decodeStoredEvent(stored)
		if !ok {
			resp.Diagnostics = append(resp.Diagnostics, result)
			continue
		}
		material, diagnostic := b.syncedEventMaterial(env)
		if diagnostic != "" {
			resp.Diagnostics = append(resp.Diagnostics, eventExchangeResult(env, "invalid", diagnostic))
			continue
		}
		if material.OriginReplicaID == replicaID {
			continue
		}
		if diagnostic := validateSyncedMaterial(material); diagnostic != "" {
			resp.Diagnostics = append(resp.Diagnostics, eventExchangeResult(env, "invalid", diagnostic))
			continue
		}
		if len(scopes) > 0 && !refAllowed(scopes, material.ResourceRef) {
			resp.Diagnostics = append(resp.Diagnostics, eventExchangeResult(env, "rejected", fmt.Sprintf("ref %s/%s is outside configured publication scope", material.ResourceRef.Kind, material.ResourceRef.ID)))
			continue
		}
		resp.Events = append(resp.Events, env)
	}
	return resp, nil
}

func (b *Backend) SyncStatus() (contract.SyncStatusResponse, error) {
	return contract.SyncStatusResponse{RemoteWorkspace: b.repo + ":" + b.branch}, nil
}

func (b *Backend) syncedEventMaterial(env eventmodel.EventEnvelope) (contract.SyncedEventMaterial, string) {
	material, err := contract.SyncedEventMaterialFromEnvelope(env)
	if err != nil {
		return contract.SyncedEventMaterial{}, err.Error()
	}
	return material, ""
}

func (b *Backend) scopeDiagnostic(ref contract.ResourceRef) string {
	if len(b.scopes) == 0 || refAllowed(b.scopes, ref) {
		return ""
	}
	return fmt.Sprintf("ref %s/%s is outside configured publication scope", ref.Kind, ref.ID)
}

func validateSyncedMaterial(material contract.SyncedEventMaterial) string {
	switch {
	case strings.TrimSpace(material.OriginReplicaID) == "":
		return "origin_replica_id is required"
	case strings.TrimSpace(material.LocalDecisionID) == "":
		return "local_decision_id is required"
	case strings.TrimSpace(string(material.Actor)) == "":
		return "actor is required"
	case strings.TrimSpace(string(material.ResourceRef.Kind)) == "" || strings.TrimSpace(string(material.ResourceRef.ID)) == "":
		return "resource_ref is required"
	case material.Fields == nil:
		return "fields are required"
	case strings.TrimSpace(material.FieldsDigest) == "":
		return "fields_digest is required"
	case material.FieldsDigest != syncedFieldsDigest(material.Fields):
		return "fields_digest does not match fields"
	default:
		return ""
	}
}

func decodeStoredEvent(stored exchange.PublicationStoredEvent) (eventmodel.EventEnvelope, contract.EventExchangeResult, bool) {
	var env eventmodel.EventEnvelope
	if err := json.Unmarshal(stored.Body, &env); err != nil {
		return eventmodel.EventEnvelope{}, contract.EventExchangeResult{
			EventID:    stored.Path,
			Status:     "invalid",
			Diagnostic: "publication event json is invalid: " + err.Error(),
		}, false
	}
	return env, contract.EventExchangeResult{}, true
}

func eventExchangeResult(env eventmodel.EventEnvelope, status, diagnostic string) contract.EventExchangeResult {
	result := contract.EventExchangeResultFromEnvelope(env, status, diagnostic)
	if strings.TrimSpace(result.EventID) == "" {
		result.EventID = env.Event.ID
	}
	return result
}

func refAllowed(scopes []contract.ResourceRef, ref contract.ResourceRef) bool {
	for _, scope := range scopes {
		if scope == ref {
			return true
		}
	}
	return false
}

func syncedFieldsDigest(fields map[string]any) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
