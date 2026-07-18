package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	// ErrArtifactSourceInput denotes a malformed local call. It is never a
	// protocol-level authorization response.
	ErrArtifactSourceInput = errors.New("invalid Artifact source input")
	// ErrArtifactSourceUnavailable deliberately closes unknown roots, missing
	// Event pins, cross-Channel requests, and every membership/semantic denial
	// behind one non-oracular result.
	ErrArtifactSourceUnavailable = errors.New("Artifact source is unavailable")
	// ErrArtifactSourceInvariant is reserved for corrupt durable authority. A
	// caller must fail closed and must not downgrade it to a not-found result.
	ErrArtifactSourceInvariant = errors.New("Artifact source durable invariant violated")
)

type ReadArtifactSourceManifestSpec struct {
	AuthenticatedPeerID model.PeerID
	ChannelID           model.ChannelID
	RootDigest          model.Digest
}

// ArtifactSourceManifest is the immutable Store-side capability returned to
// /mnemon/artifacts/1 after membership and semantic closure authorization. It
// deliberately contains no filesystem path, reader, descriptor, or handle.
type ArtifactSourceManifest struct {
	rootDigest     model.Digest
	manifestDigest model.Digest
	manifest       model.JSON
	totalBytes     uint64
}

func (manifest ArtifactSourceManifest) RootDigest() model.Digest     { return manifest.rootDigest }
func (manifest ArtifactSourceManifest) ManifestDigest() model.Digest { return manifest.manifestDigest }
func (manifest ArtifactSourceManifest) Manifest() model.JSON         { return manifest.manifest }
func (manifest ArtifactSourceManifest) ManifestBytes() []byte        { return manifest.manifest.Bytes() }
func (manifest ArtifactSourceManifest) TotalBytes() uint64           { return manifest.totalBytes }

type ReadArtifactSourceBlockSpec struct {
	AuthenticatedPeerID model.PeerID
	ChannelID           model.ChannelID
	RootDigest          model.Digest
	BlockDigest         model.Digest
}

// ArtifactSourceBlock is bounded metadata for one block proven reachable
// from the exact authorized, sealed root manifest. CAS bytes are read and
// rehashed outside the SQLite snapshot by the Artifact layer.
type ArtifactSourceBlock struct {
	rootDigest  model.Digest
	blockDigest model.Digest
	sizeBytes   uint64
}

func (block ArtifactSourceBlock) RootDigest() model.Digest  { return block.rootDigest }
func (block ArtifactSourceBlock) BlockDigest() model.Digest { return block.blockDigest }
func (block ArtifactSourceBlock) SizeBytes() uint64         { return block.sizeBytes }

// ReadArtifactSourceManifest proves transport identity, current Channel
// authority, immutable Event/Work visibility, and the complete sealed root in
// one read-only SQLite snapshot.
func (s *Store) ReadArtifactSourceManifest(ctx context.Context,
	spec ReadArtifactSourceManifestSpec,
) (ArtifactSourceManifest, error) {
	if err := validateArtifactSourceInput(s, ctx, spec.AuthenticatedPeerID,
		spec.ChannelID, spec.RootDigest); err != nil {
		return ArtifactSourceManifest{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ArtifactSourceManifest{}, fmt.Errorf("read Artifact source manifest: begin: %w", err)
	}
	defer tx.Rollback()
	if err := authorizeArtifactSourceRoot(ctx, tx, spec.AuthenticatedPeerID,
		spec.ChannelID, spec.RootDigest); err != nil {
		return ArtifactSourceManifest{}, err
	}
	closure, err := readSealedArtifactSourceClosure(ctx, tx, spec.RootDigest)
	if err != nil {
		return ArtifactSourceManifest{}, err
	}
	if err := tx.Commit(); err != nil {
		return ArtifactSourceManifest{}, fmt.Errorf("read Artifact source manifest: commit: %w", err)
	}
	return ArtifactSourceManifest{rootDigest: closure.root.RootDigest,
		manifestDigest: closure.root.ManifestDigest, manifest: closure.root.Manifest,
		totalBytes: closure.root.TotalBytes}, nil
}

// ReadArtifactSourceBlock uses the same closed authority proof as manifest
// reads and additionally requires the block to occur in that exact root's
// canonical manifest and immutable relational block map.
func (s *Store) ReadArtifactSourceBlock(ctx context.Context,
	spec ReadArtifactSourceBlockSpec,
) (ArtifactSourceBlock, error) {
	if err := validateArtifactSourceInput(s, ctx, spec.AuthenticatedPeerID,
		spec.ChannelID, spec.RootDigest); err != nil || spec.BlockDigest.IsZero() {
		if err != nil {
			return ArtifactSourceBlock{}, err
		}
		return ArtifactSourceBlock{}, ErrArtifactSourceInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ArtifactSourceBlock{}, fmt.Errorf("read Artifact source block: begin: %w", err)
	}
	defer tx.Rollback()
	if err := authorizeArtifactSourceRoot(ctx, tx, spec.AuthenticatedPeerID,
		spec.ChannelID, spec.RootDigest); err != nil {
		return ArtifactSourceBlock{}, err
	}
	closure, err := readSealedArtifactSourceClosure(ctx, tx, spec.RootDigest)
	if err != nil {
		return ArtifactSourceBlock{}, err
	}
	size, reachable := closure.blockSizes[spec.BlockDigest]
	if !reachable {
		return ArtifactSourceBlock{}, ErrArtifactSourceUnavailable
	}
	if err := tx.Commit(); err != nil {
		return ArtifactSourceBlock{}, fmt.Errorf("read Artifact source block: commit: %w", err)
	}
	return ArtifactSourceBlock{rootDigest: spec.RootDigest,
		blockDigest: spec.BlockDigest, sizeBytes: size}, nil
}

func validateArtifactSourceInput(s *Store, ctx context.Context, requester model.PeerID,
	channelID model.ChannelID, root model.Digest,
) error {
	if s == nil || s.db == nil || ctx == nil || requester.IsZero() ||
		channelID.IsZero() || root.IsZero() {
		return ErrArtifactSourceInput
	}
	return nil
}

func authorizeArtifactSourceRoot(ctx context.Context, tx *sql.Tx, requester model.PeerID,
	channelID model.ChannelID, root model.Digest,
) error {
	node, err := readNode(ctx, tx)
	if err != nil {
		return fmt.Errorf("%w: Node: %v", ErrArtifactSourceInvariant, err)
	}
	var channelPresent int
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM channels WHERE channel_id=?)`, channelID.String()).Scan(&channelPresent)
	if err != nil {
		return fmt.Errorf("%w: locate Channel: %v", ErrArtifactSourceInvariant, err)
	}
	if channelPresent != 1 {
		return ErrArtifactSourceUnavailable
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return fmt.Errorf("%w: Channel authority: %v", ErrArtifactSourceInvariant, err)
	}
	if authority.channel.Status() != model.ChannelActive {
		return ErrArtifactSourceUnavailable
	}
	member, known := authority.roster.CurrentMember(requester)
	if !known || member.Status() != model.MemberActive {
		return ErrArtifactSourceUnavailable
	}
	var binding model.PeerBinding
	for _, candidate := range authority.bindings {
		if candidate.PeerID() == requester {
			binding = candidate
			break
		}
	}
	if binding.PeerID().IsZero() || binding.State() != model.BindingActive ||
		binding.OriginEpoch() != member.OriginEpoch() || binding.MemberHead() != member.Head() {
		return ErrArtifactSourceUnavailable
	}

	publication, audienceRaw, artifactsRaw, participantCandidate, pinCreated, err :=
		readArtifactSourceAuthorizingEvent(ctx, tx, requester, channelID, root)
	if err != nil {
		return err
	}
	event := publication.Event()
	if err := validateArtifactSourceEventAuthority(authority, publication); err != nil {
		return err
	}
	expectedAudience, encodeErr := model.JSONFrom(event.Audience())
	expectedArtifacts, artifactsErr := model.JSONFrom(event.Artifacts())
	if encodeErr != nil || artifactsErr != nil ||
		!bytes.Equal(expectedAudience.Bytes(), audienceRaw) ||
		!bytes.Equal(expectedArtifacts.Bytes(), artifactsRaw) ||
		event.Scope().ChannelID() != channelID {
		return fmt.Errorf("%w: Event authority projection mismatch", ErrArtifactSourceInvariant)
	}
	if _, err := parseCanonicalStoreTime(pinCreated); err != nil {
		return fmt.Errorf("%w: Event pin creation time: %v", ErrArtifactSourceInvariant, err)
	}
	rootReferenced := false
	for _, ref := range event.Artifacts() {
		if ref.RootDigest() == root {
			rootReferenced = true
			break
		}
	}
	if !rootReferenced {
		return fmt.Errorf("%w: Event pin is not present in immutable Event closure",
			ErrArtifactSourceInvariant)
	}
	if event.Audience().Contains(requester) {
		return nil
	}
	if !participantCandidate {
		return fmt.Errorf("%w: SQL/Event semantic authority projection mismatch",
			ErrArtifactSourceInvariant)
	}
	work, err := readReviewWork(ctx, tx, event.Scope().WorkRef())
	if err != nil || work.ChannelID() != channelID || work.Ref() != event.Scope().WorkRef() {
		return fmt.Errorf("%w: frozen Work participant snapshot: %v",
			ErrArtifactSourceInvariant, err)
	}
	participants := work.Participants()
	if participants.InitiatorPeerID() != requester && participants.ReviewerPeerID() != requester {
		return fmt.Errorf("%w: SQL/Work participant authority projection mismatch",
			ErrArtifactSourceInvariant)
	}
	return nil
}

func readArtifactSourceAuthorizingEvent(ctx context.Context, tx *sql.Tx,
	requester model.PeerID, channelID model.ChannelID, root model.Digest,
) (model.SignedPublication, []byte, []byte, bool, string, error) {
	var eventText, channelText, homeText, workText, pinCreated string
	var audienceRaw, artifactsRaw, eventRaw, eventDigestRaw, bodyRaw, digestRaw, signature []byte
	var participantCandidate int
	err := tx.QueryRowContext(ctx, `SELECT e.event_id,e.channel_id,e.work_home_peer_id,e.work_id,
		e.audience_json,e.artifact_roots_json,e.canonical_event_json,e.event_digest,
		e.canonical_publication_json,e.publication_digest,e.origin_signature,
		EXISTS(SELECT 1 FROM work_members member
			WHERE member.channel_id=e.channel_id
			AND member.home_peer_id=e.work_home_peer_id
			AND member.work_id=e.work_id AND member.peer_id=?),p.created_at
		FROM artifact_pins p JOIN events e ON e.event_id=p.owner_id
		WHERE p.root_digest=? AND p.owner_kind='event' AND e.channel_id=? AND (
			EXISTS(SELECT 1 FROM json_each(e.audience_json) audience
				WHERE audience.type='text' AND audience.value=?)
			OR EXISTS(SELECT 1 FROM work_members member
				WHERE member.channel_id=e.channel_id
				AND member.home_peer_id=e.work_home_peer_id
				AND member.work_id=e.work_id AND member.peer_id=?))
		ORDER BY e.event_id LIMIT 1`, requester.String(), root.String(), channelID.String(),
		requester.String(), requester.String()).Scan(&eventText, &channelText, &homeText, &workText,
		&audienceRaw, &artifactsRaw, &eventRaw, &eventDigestRaw, &bodyRaw, &digestRaw, &signature,
		&participantCandidate, &pinCreated)
	if errors.Is(err, sql.ErrNoRows) {
		return model.SignedPublication{}, nil, nil, false, "", ErrArtifactSourceUnavailable
	}
	if err != nil {
		return model.SignedPublication{}, nil, nil, false, "",
			fmt.Errorf("%w: locate authorizing Event: %v", ErrArtifactSourceInvariant, err)
	}
	eventID, err := model.ParseEventID(eventText)
	if err != nil {
		return model.SignedPublication{}, nil, nil, false, "",
			fmt.Errorf("%w: Event ID: %v", ErrArtifactSourceInvariant, err)
	}
	body, bodyErr := model.NewJSON(bodyRaw)
	digest, digestErr := model.DigestFromBytes(digestRaw)
	eventDigest, eventDigestErr := model.DigestFromBytes(eventDigestRaw)
	wire, wireErr := model.JSONFrom(struct {
		Publication       model.JSON   `json:"publication"`
		PublicationDigest model.Digest `json:"publication_digest"`
		OriginSignature   []byte       `json:"origin_signature"`
	}{body, digest, signature})
	if bodyErr != nil || digestErr != nil || eventDigestErr != nil || wireErr != nil ||
		!bytes.Equal(body.Bytes(), bodyRaw) {
		return model.SignedPublication{}, nil, nil, false, "",
			fmt.Errorf("%w: reconstruct immutable publication: %v / %v / %v / %v",
				ErrArtifactSourceInvariant, bodyErr, digestErr, eventDigestErr, wireErr)
	}
	publication, err := model.ParseSignedPublication(wire.Bytes())
	if err != nil || publication.Event().ID() != eventID || publication.Digest() != digest ||
		publication.Event().Digest() != eventDigest ||
		!bytes.Equal(publication.Event().CanonicalJSON().Bytes(), eventRaw) ||
		!bytes.Equal(publication.CanonicalJSON().Bytes(), bodyRaw) ||
		!bytes.Equal(publication.OriginSignature(), signature) {
		return model.SignedPublication{}, nil, nil, false, "",
			fmt.Errorf("%w: immutable signed publication projection: %v",
				ErrArtifactSourceInvariant, err)
	}
	event := publication.Event()
	scope := event.Scope()
	if channelText != channelID.String() || scope.ChannelID() != channelID ||
		homeText != scope.WorkRef().HomePeerID().String() ||
		workText != scope.WorkRef().WorkID().String() {
		return model.SignedPublication{}, nil, nil, false, "",
			fmt.Errorf("%w: Event Channel/Work projection mismatch", ErrArtifactSourceInvariant)
	}
	return publication, append([]byte(nil), audienceRaw...), append([]byte(nil), artifactsRaw...),
		participantCandidate == 1, pinCreated, nil
}

func validateArtifactSourceEventAuthority(authority verifiedChannelAuthority,
	publication model.SignedPublication,
) error {
	event := publication.Event()
	scope := event.Scope()
	members := authority.roster.Members()
	if scope.ChannelID() != authority.channel.ID() || scope.OriginMember().Revision() == 0 ||
		scope.PublicationRoster().Revision() == 0 ||
		scope.OriginMember().Revision() > scope.PublicationRoster().Revision() ||
		scope.PublicationRoster().Revision() > uint64(len(members)) {
		return fmt.Errorf("%w: Event roster authority is out of range", ErrArtifactSourceInvariant)
	}
	origin := members[scope.OriginMember().Revision()-1]
	publicationHead := members[scope.PublicationRoster().Revision()-1]
	if origin.Head() != scope.OriginMember() || origin.PeerID() != scope.OriginPeerID() ||
		origin.OriginEpoch() != scope.OriginEpoch() || origin.Status() != model.MemberActive ||
		publicationHead.Head() != scope.PublicationRoster() {
		return fmt.Errorf("%w: Event roster authority projection mismatch", ErrArtifactSourceInvariant)
	}
	if err := model.VerifyPublication(origin.PublicKey(), publication); err != nil {
		return fmt.Errorf("%w: Event origin signature: %v", ErrArtifactSourceInvariant, err)
	}
	return nil
}

type sealedArtifactSourceClosure struct {
	root       VerifiedArtifactRoot
	blockSizes map[model.Digest]uint64
}

func readSealedArtifactSourceClosure(ctx context.Context, tx *sql.Tx,
	rootDigest model.Digest,
) (sealedArtifactSourceClosure, error) {
	root, state, err := readArtifactRoot(ctx, tx, rootDigest)
	if err != nil {
		return sealedArtifactSourceClosure{}, fmt.Errorf("%w: root metadata: %v",
			ErrArtifactSourceInvariant, err)
	}
	if state != "verified" || root.VerifiedAt.IsZero() {
		return sealedArtifactSourceClosure{}, fmt.Errorf("%w: Event-pinned root is not verified",
			ErrArtifactSourceInvariant)
	}
	manifest, err := artifactdomain.ParseManifest(root.Manifest.Bytes())
	if err != nil || manifest.RootDigest() != root.RootDigest ||
		manifest.ManifestDigest() != root.ManifestDigest ||
		manifest.TotalBytes() != root.TotalBytes ||
		!bytes.Equal(manifest.CanonicalJSON().Bytes(), root.Manifest.Bytes()) {
		return sealedArtifactSourceClosure{}, fmt.Errorf("%w: sealed manifest projection: %v",
			ErrArtifactSourceInvariant, err)
	}
	expected := artifactSourceRootMap(manifest)
	actual, err := readArtifactRootBlockMap(ctx, tx, rootDigest)
	if err != nil || !equalArtifactRootBlocks(actual, expected) {
		return sealedArtifactSourceClosure{}, fmt.Errorf("%w: sealed root block map: %v",
			ErrArtifactSourceInvariant, err)
	}
	blockSizes := make(map[model.Digest]uint64)
	for _, row := range expected {
		if previous, present := blockSizes[row.BlockDigest]; present {
			if previous != row.LengthBytes {
				return sealedArtifactSourceClosure{}, fmt.Errorf("%w: inconsistent manifest block size",
					ErrArtifactSourceInvariant)
			}
			continue
		}
		var size uint64
		var createdText string
		err := tx.QueryRowContext(ctx, `SELECT size_bytes,created_at FROM artifact_blocks
			WHERE block_digest=?`, row.BlockDigest.String()).Scan(&size, &createdText)
		if err != nil || size != row.LengthBytes || size == 0 ||
			size > artifactdomain.BlockSize {
			return sealedArtifactSourceClosure{}, fmt.Errorf("%w: reachable block metadata: %v",
				ErrArtifactSourceInvariant, err)
		}
		if _, err := parseCanonicalStoreTime(createdText); err != nil {
			return sealedArtifactSourceClosure{}, fmt.Errorf("%w: reachable block creation time: %v",
				ErrArtifactSourceInvariant, err)
		}
		blockSizes[row.BlockDigest] = size
	}
	return sealedArtifactSourceClosure{root: root, blockSizes: blockSizes}, nil
}

func artifactSourceRootMap(manifest artifactdomain.Manifest) []VerifiedArtifactRootBlock {
	entries := manifest.Entries()
	rows := make([]VerifiedArtifactRootBlock, 0)
	for _, entry := range entries {
		for _, block := range entry.Blocks {
			rows = append(rows, VerifiedArtifactRootBlock{RootDigest: manifest.RootDigest(),
				Ordinal: uint64(len(rows)), LogicalPath: entry.LogicalPath,
				OffsetBytes: block.OffsetBytes, LengthBytes: block.LengthBytes,
				BlockDigest: block.Digest, Mode: entry.Mode})
		}
	}
	return rows
}
