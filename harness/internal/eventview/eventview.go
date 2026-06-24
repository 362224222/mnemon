package eventview

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type EventView struct {
	Ref       string
	Digest    string
	Resources []contract.ResourceVersion
	Content   []ResourceContent
	Feedback  []contract.Decision // pull channel (Invariant #8)
}

// ResourceContent carries the scoped fields for a materialized resource. It is populated from the
// same refs used for Resources, so content and versions share one server-enforced scope.
type ResourceContent struct {
	Ref     contract.ResourceRef `json:"ref"`
	Version contract.Version     `json:"version"`
	Fields  map[string]any       `json:"fields"`
}

// Build materializes a read-only view over refs for forActor. The context digest folds, per resource in a
// stable order, Kind:ID:Version AND the canonical field bytes (D8/S10) — so a content tamper that preserves
// the version is still detectable (a digest covering only Kind:ID:Version would miss it).
func Build(s *store.Store, refs []contract.ResourceRef, forActor contract.ActorID) EventView {
	type item struct {
		rv     contract.ResourceVersion
		fields map[string]any
	}
	items := make([]item, 0, len(refs))
	for _, r := range refs {
		v, fields, _ := s.GetResource(r)
		items = append(items, item{contract.ResourceVersion{Ref: r, Version: v}, fields})
	}
	sort.Slice(items, func(i, j int) bool {
		return string(items[i].rv.Ref.Kind)+string(items[i].rv.Ref.ID) < string(items[j].rv.Ref.Kind)+string(items[j].rv.Ref.ID)
	})
	rv := make([]contract.ResourceVersion, 0, len(items))
	content := make([]ResourceContent, 0, len(items))
	h := sha256.New()
	for _, it := range items {
		rv = append(rv, it.rv)
		if it.rv.Version > 0 {
			content = append(content, ResourceContent{Ref: it.rv.Ref, Version: it.rv.Version, Fields: it.fields})
		}
		b, _ := json.Marshal(it.fields) // json.Marshal sorts map keys -> canonical, deterministic bytes
		fmt.Fprintf(h, "%s:%s:%d:%s;", it.rv.Ref.Kind, it.rv.Ref.ID, it.rv.Version, b)
	}
	dig := hex.EncodeToString(h.Sum(nil))
	fb, _ := s.DecisionsForActor(forActor)
	return EventView{Ref: "proj_" + dig[:12], Digest: dig, Resources: rv, Content: content, Feedback: fb}
}

// ScopedView builds the server-enforced, scoped event view for a subscription (S9): ONLY sub.Refs are
// materialized, so an out-of-scope resource can never cross the wire. Identity (forActor) is the
// subscription's actor — the server passes the AUTHENTICATED principal here, never a client-named scope.
// (PrivacyTier is reserved for a future per-resource tier filter; today the ref set IS the scope.)
func ScopedView(s *store.Store, sub contract.Subscription) EventView {
	return Build(s, sub.Refs, sub.Actor)
}
