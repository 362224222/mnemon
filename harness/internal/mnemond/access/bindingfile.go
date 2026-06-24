package access

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// DefaultBindingFile is the canonical channel-binding manifest path under the project root (P3.1).
const DefaultBindingFile = ".mnemon/harness/channel/bindings.json"

// LoadedBindings is the result of parsing a binding file: the channel bindings plus the bearer
// token -> principal map assembled from the bindings' credential_ref token files (for a
// TokenAuthenticator). The bindings feed RuntimeConfig.Bindings + SubsFromBindings; the tokens feed
// the server's authenticator.
type LoadedBindings struct {
	Bindings []ChannelBinding
	Tokens   map[string]contract.ActorID
}

// bindingFileDoc is the on-disk schema (snake_case JSON). It is the SERIALIZED form of ChannelBinding
// + a credential ref; the loader maps it to the engine types so the file format is a thin adapter,
// not a second binding model.
type bindingFileDoc struct {
	SchemaVersion int                `json:"schema_version"`
	Bindings      []bindingFileEntry `json:"bindings"`
}

type bindingFileEntry struct {
	Principal            string       `json:"principal"`
	ActorKind            string       `json:"actor_kind"`
	Transport            string       `json:"transport"`
	Endpoint             string       `json:"endpoint"`
	AllowedVerbs         []string     `json:"allowed_verbs"`
	AllowedObservedTypes []string     `json:"allowed_observed_types"`
	SubscriptionScope    []bindingRef `json:"subscription_scope"`
	IdempotencyNamespace string       `json:"idempotency_namespace"`
	CredentialRef        string       `json:"credential_ref"`
	Budget               string       `json:"budget,omitempty"` // context-budget tier (P4): hot|warm|digest-only; empty = hot
}

type bindingRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// LoadBindingFile reads + validates the channel-binding manifest at path and assembles the bindings
// and bearer-token map. Relative credential_ref token paths resolve against root (the project root,
// absolute ones are used verbatim. It validates each entry
// (principal, known actor kind / verbs / transport, http endpoint non-empty), the schema version,
// and cross-entry uniqueness (principal, idempotency namespace, bearer token).
func LoadBindingFile(root, path string) (LoadedBindings, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return LoadedBindings{}, fmt.Errorf("read binding file: %w", err)
	}
	var doc bindingFileDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return LoadedBindings{}, fmt.Errorf("parse binding file %s: %w", path, err)
	}
	if doc.SchemaVersion != 1 {
		return LoadedBindings{}, fmt.Errorf("binding file schema_version %d unsupported (want 1)", doc.SchemaVersion)
	}
	bindings := make([]ChannelBinding, 0, len(doc.Bindings))
	tokens := map[string]contract.ActorID{}
	for i, e := range doc.Bindings {
		b, err := e.toBinding()
		if err != nil {
			return LoadedBindings{}, fmt.Errorf("binding[%d] (%s): %w", i, e.Principal, err)
		}
		bindings = append(bindings, b)
		if ref := strings.TrimSpace(e.CredentialRef); ref != "" {
			tokPath := ref
			if !filepath.IsAbs(tokPath) {
				tokPath = filepath.Join(root, tokPath)
			}
			tokRaw, err := os.ReadFile(tokPath)
			if err != nil {
				return LoadedBindings{}, fmt.Errorf("binding[%d] (%s): read credential_ref %s: %w", i, e.Principal, ref, err)
			}
			tok := strings.TrimSpace(string(tokRaw))
			if tok == "" {
				return LoadedBindings{}, fmt.Errorf("binding[%d] (%s): credential_ref %s is empty", i, e.Principal, ref)
			}
			if owner, clash := tokens[tok]; clash {
				return LoadedBindings{}, fmt.Errorf("binding[%d] (%s): bearer token also bound to %q", i, e.Principal, owner)
			}
			tokens[tok] = b.Principal
		}
	}
	// NewBindingSet enforces principal + idempotency-namespace uniqueness (and re-validates each).
	if _, err := NewBindingSet(bindings...); err != nil {
		return LoadedBindings{}, err
	}
	return LoadedBindings{Bindings: bindings, Tokens: tokens}, nil
}

func (e bindingFileEntry) toBinding() (ChannelBinding, error) {
	kind, err := parseActorKind(e.ActorKind)
	if err != nil {
		return ChannelBinding{}, err
	}
	transport, err := parseTransport(e.Transport)
	if err != nil {
		return ChannelBinding{}, err
	}
	if transport == TransportHTTP && strings.TrimSpace(e.Endpoint) == "" {
		return ChannelBinding{}, fmt.Errorf("http transport requires a non-empty endpoint")
	}
	verbs := make([]Verb, 0, len(e.AllowedVerbs))
	for _, v := range e.AllowedVerbs {
		pv, err := parseVerb(v)
		if err != nil {
			return ChannelBinding{}, err
		}
		verbs = append(verbs, pv)
	}
	scope := make([]contract.ResourceRef, 0, len(e.SubscriptionScope))
	for _, r := range e.SubscriptionScope {
		scope = append(scope, contract.ResourceRef{Kind: contract.ResourceKind(r.Kind), ID: contract.ResourceID(r.ID)})
	}
	b := ChannelBinding{
		Principal:            contract.ActorID(e.Principal),
		ActorKind:            kind,
		Transport:            transport,
		Endpoint:             e.Endpoint,
		AllowedVerbs:         verbs,
		AllowedObservedTypes: e.AllowedObservedTypes,
		SubscriptionScope:    scope,
		IdempotencyNamespace: e.IdempotencyNamespace,
		Budget:               contract.BudgetTier(e.Budget), // empty stays empty (= hot); Validate rejects an unknown tier
	}
	if err := b.Validate(); err != nil {
		return ChannelBinding{}, err
	}
	return b, nil
}

func parseActorKind(s string) (contract.ActorKind, error) {
	switch contract.ActorKind(s) {
	case contract.KindHostAgent:
		return contract.KindHostAgent, nil
	case contract.KindControlAgent:
		return contract.KindControlAgent, nil
	case contract.KindReplicaAgent:
		return contract.KindReplicaAgent, nil
	default:
		return "", fmt.Errorf("unknown actor_kind %q", s)
	}
}

func parseTransport(s string) (Transport, error) {
	switch Transport(s) {
	case TransportLocal:
		return TransportLocal, nil
	case TransportHTTP:
		return TransportHTTP, nil
	case TransportMTLS:
		return TransportMTLS, nil
	default:
		return "", fmt.Errorf("unknown transport %q", s)
	}
}

func parseVerb(s string) (Verb, error) {
	switch Verb(s) {
	case VerbObserve:
		return VerbObserve, nil
	case VerbPull:
		return VerbPull, nil
	case VerbRender:
		return VerbRender, nil
	case VerbStatus:
		return VerbStatus, nil
	case VerbSyncPush:
		return VerbSyncPush, nil
	case VerbSyncPull:
		return VerbSyncPull, nil
	case VerbSyncStatus:
		return VerbSyncStatus, nil
	default:
		return "", fmt.Errorf("unknown verb %q", s)
	}
}

// toEntry is the inverse of toBinding: it serializes a ChannelBinding (+ optional credentialRef) to
// the on-disk entry form, so UpsertBinding round-trips through the same schema LoadBindingFile reads.
func toEntry(b ChannelBinding, credentialRef string) bindingFileEntry {
	verbs := make([]string, len(b.AllowedVerbs))
	for i, v := range b.AllowedVerbs {
		verbs[i] = string(v)
	}
	scope := make([]bindingRef, len(b.SubscriptionScope))
	for i, r := range b.SubscriptionScope {
		scope[i] = bindingRef{Kind: string(r.Kind), ID: string(r.ID)}
	}
	return bindingFileEntry{
		Principal:            string(b.Principal),
		ActorKind:            string(b.ActorKind),
		Transport:            string(b.Transport),
		Endpoint:             b.Endpoint,
		AllowedVerbs:         verbs,
		AllowedObservedTypes: b.AllowedObservedTypes,
		SubscriptionScope:    scope,
		IdempotencyNamespace: b.IdempotencyNamespace,
		CredentialRef:        credentialRef,
		Budget:               string(b.Budget),
	}
}

// UpsertBinding inserts or replaces (by principal) b in the manifest at path, creating the file
// (schema_version 1) when absent and PRESERVING every other entry + their order — so `setup` manages
// exactly its own principal and never clobbers a user-added or sibling-loop binding. credentialRef is
// the token-file ref to record (project-relative or absolute, "" for header auth).
// MergeBinding upserts b into the binding file, UNIONing the verbs / observed types / subscription
// scope with any existing binding for the same principal — so installing skill after memory keeps the
// memory grant rather than replacing it. The existing credential ref is kept when credentialRef is
// empty (an idempotent token). It is the additive variant of UpsertBinding (which replaces).
func MergeBinding(path string, b ChannelBinding, credentialRef string) error {
	if err := b.Validate(); err != nil {
		return err
	}
	doc, err := readBindingDocOrEmpty(path)
	if err != nil {
		return err
	}
	for i := range doc.Bindings {
		if doc.Bindings[i].Principal == string(b.Principal) {
			if existing, err := doc.Bindings[i].toBinding(); err == nil {
				b.AllowedVerbs = unionVerbs(existing.AllowedVerbs, b.AllowedVerbs)
				b.AllowedObservedTypes = unionStrings(existing.AllowedObservedTypes, b.AllowedObservedTypes)
				b.SubscriptionScope = unionRefs(existing.SubscriptionScope, b.SubscriptionScope)
			}
			// credentialRef reflects the CURRENT --token intent (a path when enabled, "" when disabled);
			// it is set verbatim, so a rerun with --token=false clears the stale credential rather than
			// leaving the server on TokenAuthenticator while the hooks switch to the trusted header.
			doc.Bindings[i] = toEntry(b, credentialRef)
			return writeBindingDoc(path, doc)
		}
	}
	doc.Bindings = append(doc.Bindings, toEntry(b, credentialRef))
	return writeBindingDoc(path, doc)
}

func unionVerbs(a, b []Verb) []Verb {
	seen := map[Verb]bool{}
	out := make([]Verb, 0, len(a)+len(b))
	for _, vs := range [][]Verb{a, b} {
		for _, v := range vs {
			if !seen[v] {
				seen[v] = true
				out = append(out, v)
			}
		}
	}
	return out
}

func unionStrings(a, b []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(a)+len(b))
	for _, ss := range [][]string{a, b} {
		for _, s := range ss {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	return out
}

func unionRefs(a, b []contract.ResourceRef) []contract.ResourceRef {
	seen := map[contract.ResourceRef]bool{}
	out := make([]contract.ResourceRef, 0, len(a)+len(b))
	for _, rs := range [][]contract.ResourceRef{a, b} {
		for _, r := range rs {
			if !seen[r] {
				seen[r] = true
				out = append(out, r)
			}
		}
	}
	return out
}

func UpsertBinding(path string, b ChannelBinding, credentialRef string) error {
	if err := b.Validate(); err != nil {
		return err
	}
	doc, err := readBindingDocOrEmpty(path)
	if err != nil {
		return err
	}
	entry := toEntry(b, credentialRef)
	replaced := false
	for i := range doc.Bindings {
		if doc.Bindings[i].Principal == entry.Principal {
			doc.Bindings[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		doc.Bindings = append(doc.Bindings, entry)
	}
	return writeBindingDoc(path, doc)
}

// RemoveBinding removes the principal's entry from the manifest at path, preserving all others, and
// reports whether an entry was removed. The file is left in place (with an empty bindings list when
// it held only that entry), so a user-managed manifest is never surprised away by an uninstall.
func RemoveBinding(path string, principal contract.ActorID) (bool, error) {
	doc, err := readBindingDocOrEmpty(path)
	if err != nil {
		return false, err
	}
	kept := doc.Bindings[:0]
	removed := false
	for _, e := range doc.Bindings {
		if contract.ActorID(e.Principal) == principal {
			removed = true
			continue
		}
		kept = append(kept, e)
	}
	if !removed {
		return false, nil
	}
	doc.Bindings = kept
	return true, writeBindingDoc(path, doc)
}

func readBindingDocOrEmpty(path string) (bindingFileDoc, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return bindingFileDoc{SchemaVersion: 1}, nil
	}
	if err != nil {
		return bindingFileDoc{}, fmt.Errorf("read binding file: %w", err)
	}
	var doc bindingFileDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return bindingFileDoc{}, fmt.Errorf("parse binding file %s: %w", path, err)
	}
	if doc.SchemaVersion == 0 {
		doc.SchemaVersion = 1
	}
	return doc, nil
}

func writeBindingDoc(path string, doc bindingFileDoc) error {
	doc.SchemaVersion = 1
	if doc.Bindings == nil {
		doc.Bindings = []bindingFileEntry{}
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// SubsFromBindings derives the per-principal subscription scopes from the bindings, so the runtime's
// served scope and the binding manifest come from ONE source (the binding file).
func SubsFromBindings(bindings []ChannelBinding) map[contract.ActorID]contract.Subscription {
	subs := make(map[contract.ActorID]contract.Subscription, len(bindings))
	for _, b := range bindings {
		subs[b.Principal] = contract.Subscription{Actor: b.Principal, Refs: b.SubscriptionScope, Budget: b.Budget}
	}
	return subs
}
