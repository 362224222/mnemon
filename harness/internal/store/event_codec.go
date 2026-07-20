package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrPublicationInvalid = errors.New("local signed publication is invalid")

type captureRoot struct {
	RootDigest     model.Digest
	ManifestDigest model.Digest
}

func validateLocalPublication(publication model.SignedPublication, originPublicKey []byte) (model.Event, error) {
	event := publication.Event()
	if event.ID().IsZero() || event.Source() != model.EventSourceLocal {
		return model.Event{}, fmt.Errorf("%w: publication is not a local Event", ErrPublicationInvalid)
	}
	rebuiltEvent, err := model.NewEvent(event.Spec())
	if err != nil || rebuiltEvent.Digest() != event.Digest() ||
		!bytes.Equal(rebuiltEvent.CanonicalJSON().Bytes(), event.CanonicalJSON().Bytes()) {
		return model.Event{}, fmt.Errorf("%w: Event projection mismatch", ErrPublicationInvalid)
	}
	rebuiltBody, err := model.NewPublicationBody(rebuiltEvent)
	if err != nil || rebuiltBody.Digest() != publication.Digest() ||
		!bytes.Equal(rebuiltBody.CanonicalJSON().Bytes(), publication.CanonicalJSON().Bytes()) {
		return model.Event{}, fmt.Errorf("%w: publication body mismatch", ErrPublicationInvalid)
	}
	signature := publication.OriginSignature()
	if err := model.VerifyPublication(originPublicKey, publication); err != nil {
		return model.Event{}, fmt.Errorf("%w: %v", ErrPublicationInvalid, err)
	}
	rebuiltPublication, err := model.AttachSignature(rebuiltBody, signature)
	if err != nil || !bytes.Equal(rebuiltPublication.WireJSON().Bytes(), publication.WireJSON().Bytes()) {
		return model.Event{}, fmt.Errorf("%w: signed wire frame mismatch", ErrPublicationInvalid)
	}
	return rebuiltEvent, nil
}

func insertAcceptedEvent(ctx context.Context, tx *sqlTx, publication model.SignedPublication) error {
	event := publication.Event()
	scope := event.Scope()
	audience, err := model.JSONFrom(event.Audience())
	if err != nil {
		return fmt.Errorf("encode Event audience: %w", err)
	}
	resource, err := model.JSONFrom(scope.WorkRef())
	if err != nil {
		return fmt.Errorf("encode Event resource: %w", err)
	}
	artifacts, err := model.JSONFrom(event.Artifacts())
	if err != nil {
		return fmt.Errorf("encode Event Artifact roots: %w", err)
	}
	causedBy, err := model.JSONFrom(event.CausedBy())
	if err != nil {
		return fmt.Errorf("encode Event causality: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO events(
		event_id,schema_version,channel_id,origin_peer_id,origin_epoch,origin_seq,channel_seq,
		origin_member_revision,origin_member_record_hash,publication_roster_revision,publication_roster_hash,
		source,actor_principal,event_type,audience_json,resource_json,work_home_peer_id,work_id,summary,
		payload_json,artifact_roots_json,caused_by_json,canonical_event_json,event_digest,
		canonical_publication_json,publication_digest,origin_signature,created_at,accepted_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		event.ID().String(), model.SchemaVersion, scope.ChannelID().String(), scope.OriginPeerID().String(),
		scope.OriginEpoch().String(), scope.OriginSequence(), scope.ChannelSequence(),
		scope.OriginMember().Revision(), scope.OriginMember().Digest().Bytes(),
		scope.PublicationRoster().Revision(), scope.PublicationRoster().Digest().Bytes(), string(event.Source()),
		event.ActorPrincipal(), string(event.Type()), audience.Bytes(), resource.Bytes(),
		scope.WorkRef().HomePeerID().String(), scope.WorkRef().WorkID().String(), event.Summary(),
		event.Payload().Bytes(), artifacts.Bytes(), causedBy.Bytes(), event.CanonicalJSON().Bytes(),
		event.Digest().Bytes(), publication.CanonicalJSON().Bytes(), publication.Digest().Bytes(),
		publication.OriginSignature(), storeTime(event.CreatedAt()), storeTime(event.AcceptedAt()))
	if err != nil {
		return fmt.Errorf("insert accepted Event %s: %w", event.ID().String(), err)
	}
	return nil
}

// sqlTx is the transaction surface used here. The alias keeps the projection
// codec independent of Store ownership while retaining one concrete writer.
type sqlTx = sql.Tx

func insertPublicationEvidence(ctx context.Context, tx *sql.Tx, event model.Event) error {
	return insertPublicationEvidenceDisposition(ctx, tx, event, "pending", "")
}

func deterministicDeliveryID(event model.EventID, target model.PeerID) string {
	digest := sha256.Sum256([]byte(event.String() + "\x00" + target.String()))
	return "delivery-" + hex.EncodeToString(digest[:])
}

func insertArtifactOwnerPin(ctx context.Context, tx *sql.Tx, root model.Digest, kind, owner string, at time.Time) error {
	if _, err := requireVerifiedArtifactRoot(ctx, tx, root); err != nil {
		return err
	}
	if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,created_at)
		VALUES(?,?,?,?)`, root.String(), kind, owner, storeTime(at))
	if err != nil {
		return fmt.Errorf("insert %s Artifact pin: %w", kind, err)
	}
	return nil
}

func parseOperationCapture(value model.JSON) ([]captureRoot, error) {
	var envelope struct {
		Roots json.RawMessage `json:"roots"`
	}
	decoder := json.NewDecoder(bytes.NewReader(value.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil || len(envelope.Roots) == 0 || bytes.Equal(envelope.Roots, []byte("null")) {
		return nil, fmt.Errorf("capture checkpoint: closed roots array required: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("capture checkpoint: %w", err)
	}
	var encoded []struct {
		ManifestDigest string `json:"manifest_digest"`
		RootDigest     string `json:"root_digest"`
	}
	rootsDecoder := json.NewDecoder(bytes.NewReader(envelope.Roots))
	rootsDecoder.DisallowUnknownFields()
	if err := rootsDecoder.Decode(&encoded); err != nil || encoded == nil {
		return nil, fmt.Errorf("capture checkpoint roots: %w", err)
	}
	if len(encoded) > model.MaxArtifactRefs {
		return nil, fmt.Errorf("capture checkpoint roots: got %d, max %d", len(encoded), model.MaxArtifactRefs)
	}
	if err := requireJSONEOF(rootsDecoder); err != nil {
		return nil, fmt.Errorf("capture checkpoint roots: %w", err)
	}
	result := make([]captureRoot, len(encoded))
	for index, item := range encoded {
		root, err := model.ParseDigest(item.RootDigest)
		if err != nil {
			return nil, fmt.Errorf("capture root %d: %w", index, err)
		}
		manifest, err := model.ParseDigest(item.ManifestDigest)
		if err != nil {
			return nil, fmt.Errorf("capture manifest %d: %w", index, err)
		}
		if index > 0 && result[index-1].RootDigest.String() >= root.String() {
			return nil, errors.New("capture roots must be uniquely sorted by root_digest")
		}
		result[index] = captureRoot{root, manifest}
	}
	return result, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON value")
	}
	return nil
}

func buildAcceptanceReceipt(ctx context.Context, q rowQuerier, operation model.OperationID,
	events []model.Event, items []LocalAcceptanceItem, capture []captureRoot,
) (model.JSON, error) {
	type eventReceipt struct {
		ArtifactRoots []model.ArtifactRef `json:"artifact_roots"`
		EventDigest   model.Digest        `json:"event_digest"`
		EventID       model.EventID       `json:"event_id"`
		EventType     model.EventType     `json:"event_type"`
		Work          struct {
			Ref     model.WorkRef   `json:"ref"`
			State   model.WorkState `json:"state"`
			Version uint64          `json:"version"`
		} `json:"work"`
	}
	type captureReceipt struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}
	if ctx == nil || q == nil || len(events) == 0 || len(events) != len(items) {
		return model.JSON{}, errors.New("acceptance receipt requires matching Events and items")
	}
	eventRows := make([]eventReceipt, len(events))
	for index, event := range events {
		work := model.ReviewWork{}
		if items[index].Work != nil {
			work = items[index].Work.Work
		} else {
			var err error
			work, err = readReviewWork(ctx, q, event.Scope().WorkRef())
			if err != nil {
				return model.JSON{}, fmt.Errorf("acceptance receipt Work %d: %w", index, err)
			}
		}
		row := eventReceipt{ArtifactRoots: event.Artifacts(), EventDigest: event.Digest(),
			EventID: event.ID(), EventType: event.Type()}
		row.Work.Ref, row.Work.Version, row.Work.State = work.Ref(), work.Version(), work.State()
		eventRows[index] = row
	}
	captureRows := make([]captureReceipt, len(capture))
	for index, root := range capture {
		captureRows[index] = captureReceipt{root.ManifestDigest, root.RootDigest}
	}
	// Event order is the committed sequence order; capture order is frozen by
	// parseOperationCapture. Do not accept caller-provided result projections.
	return model.JSONFrom(struct {
		CaptureRoots []captureReceipt `json:"capture_roots"`
		Events       []eventReceipt   `json:"events"`
		OperationID  string           `json:"operation_id"`
		Status       string           `json:"status"`
	}{captureRows, eventRows, operation.String(), "committed"})
}

func captureRootSet(roots []captureRoot) map[model.Digest]model.Digest {
	result := make(map[model.Digest]model.Digest, len(roots))
	for _, root := range roots {
		result[root.RootDigest] = root.ManifestDigest
	}
	return result
}

func sortedDigests(values map[model.Digest]struct{}) []model.Digest {
	result := make([]model.Digest, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result
}
