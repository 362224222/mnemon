package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestCreateChannelCommitsReplaysRestartsAndNeverStoresSecret(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	fixture := testkit.NewSignedChannel(t, "create-transaction")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-create-transaction")
	secret := model.Sum([]byte("plaintext-invite-secret-that-must-never-reach-store")).Bytes()
	token := storeTestEnrollmentTokenWithSecret(t, fixture.Descriptor(), fixture.Owner(), grantID,
		secret, fixture.Channel().CreatedAt(), 7)
	spec := CreateChannelSpec{Channel: fixture.Channel(), Genesis: fixture.OwnerMember().Member(), Token: token}
	created, err := st.CreateChannel(context.Background(), spec)
	if err != nil || !created.Created || created.Channel.ID() != fixture.Channel().ID() || created.GrantID != grantID {
		t.Fatalf("CreateChannel() = (%#v, %v)", created, err)
	}
	replayed, err := st.CreateChannel(context.Background(), spec)
	if err != nil || replayed.Created || replayed.Channel.ID() != fixture.Channel().ID() {
		t.Fatalf("CreateChannel(replay) = (%#v, %v)", replayed, err)
	}
	for table, want := range map[string]int{"channels": 1, "channel_members": 1,
		"publication_epochs": 1, "enrollment_grants": 1, "peer_bindings": 0} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count = %d, err=%v, want %d", table, got, err, want)
		}
	}
	wantVerifier, err := model.VerifierForEnrollment(secret, fixture.Channel().ID(), grantID)
	if err != nil {
		t.Fatal(err)
	}
	var storedVerifier []byte
	if err := st.db.QueryRow(`SELECT verifier FROM enrollment_grants WHERE grant_id=?`,
		grantID.String()).Scan(&storedVerifier); err != nil ||
		!bytes.Equal(storedVerifier, wantVerifier.Bytes()) {
		t.Fatalf("stored verifier = %x, %v, want exact derived verifier", storedVerifier, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	assertEnrollmentCredentialAbsent(t, path, token)
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	tx, err := restarted.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := readVerifiedChannelAuthority(context.Background(), tx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if err != nil || authority.roster.Head() != fixture.Roster().Head() {
		t.Fatalf("authority after restart = (%#v, %v)", authority, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestChannelInvitePersistenceRequiresSignedTokenAuthorityProjection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "token-authority-projection")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-token-authority-projection")
	wrongAddresses := []string{"/ip4/127.0.0.1/tcp/4999"}
	wrongToken := storeTestEnrollmentTokenWithAddresses(t, fixture.Descriptor(), fixture.Owner(),
		grantID, model.Sum([]byte("wrong-create-address-secret")).Bytes(), wrongAddresses,
		fixture.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: wrongToken}); !errors.Is(err, ErrChannelCreateInput) {
		t.Fatalf("create with uncommitted owner addresses error = %v", err)
	}
	var channels int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channels`).Scan(&channels); err != nil || channels != 0 {
		t.Fatalf("partial Channel count = %d, %v", channels, err)
	}

	initialID, _ := model.ParseGrantID("grant-token-authority-initial")
	initial := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), initialID,
		"authority-initial", fixture.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: initial}); err != nil {
		t.Fatal(err)
	}
	rotationAt := fixture.Channel().CreatedAt().Add(time.Minute)
	rotationID, _ := model.ParseGrantID("grant-token-authority-rotation")
	wrongRotation := storeTestEnrollmentTokenWithAddresses(t, fixture.Descriptor(), fixture.Owner(),
		rotationID, model.Sum([]byte("wrong-rotation-address-secret")).Bytes(), wrongAddresses,
		rotationAt, 1)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{ChannelID: fixture.Channel().ID(),
		Token: wrongRotation, At: rotationAt, ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant: inviteTestOpenGrantFence(initialID)}); !errors.Is(err, ErrChannelInviteInput) {
		t.Fatalf("rotation with uncommitted owner addresses error = %v", err)
	}
	assertInviteGrantState(t, st, initialID, "open", 0)
	var rotations int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM enrollment_grants WHERE grant_id=?`,
		rotationID.String()).Scan(&rotations); err != nil || rotations != 0 {
		t.Fatalf("partial rotation count = %d, %v", rotations, err)
	}
}

func TestCreateChannelReplaySurvivesConsumedClosedAndRotatedGrant(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "create-replay-after-rotation")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-create-replay-after-rotation")
	token := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), grantID,
		"create-replay", fixture.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
	spec := CreateChannelSpec{Channel: fixture.Channel(), Genesis: fixture.OwnerMember().Member(), Token: token}
	if _, err := st.CreateChannel(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	joined := fixture.AppendActive(t, "create-replay-joined-peer")
	joinedAt := joined.Member().CreatedAt()
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertChannelMember(context.Background(), tx, joined.Member()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, fixture.Roster().Head().Revision(), fixture.Roster().Head().Digest().Bytes(),
		storeTime(fixture.Channel().UpdatedAt()), fixture.Channel().ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES(?,?,?,?,?,?,?,?)`, "use-create-replay", grantID.String(), fixture.Channel().ID().String(),
		joined.Identity().PeerID().String(),
		inviteTestJoinIdentity(t, fixture.Channel().ID(), grantID, joined).Bytes(),
		joined.Member().Head().Revision(), joined.Member().Head().Digest().Bytes(), storeTime(joinedAt)); err != nil {
		t.Fatal(err)
	}
	receiptAt := joinedAt.Add(time.Second)
	receipt := inviteTestReceipt(t, fixture, grantID, joined, "create-replay", receiptAt)
	if _, err := tx.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, receipt.ReceiptID().String(), "use-create-replay",
		fixture.Channel().ID().String(), joined.Identity().PeerID().String(), joined.Member().Head().Revision(),
		joined.Member().Head().Digest().Bytes(), receipt.ReceiptJSON().Bytes(), receipt.OwnerSignature(),
		storeTime(receipt.AcceptedAt())); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE enrollment_grants SET status='closed',closed_at=? WHERE grant_id=?`,
		storeTime(receiptAt), grantID.String()); err != nil {
		t.Fatal(err)
	}
	rotatedID, _ := model.ParseGrantID("grant-create-replay-rotated")
	rotatedToken := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), rotatedID,
		"rotated", receiptAt, model.MaxMembersPerChannel-2)
	rotatedGrant := storeTestEnrollmentGrant(t, rotatedToken, receiptAt)
	if _, err := tx.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,expires_at,
		max_uses,used_uses,status,created_at,closed_at) VALUES(?,?,?,?,?,0,'open',?,NULL)`,
		rotatedID.String(), fixture.Channel().ID().String(),
		rotatedGrant.Verifier().Bytes(), storeTime(rotatedGrant.ExpiresAt()), rotatedGrant.MaxUses(),
		storeTime(rotatedGrant.CreatedAt())); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	replayed, err := st.CreateChannel(context.Background(), spec)
	if err != nil || replayed.Created || replayed.Channel.RosterHead() != fixture.Roster().Head() ||
		replayed.GrantID != grantID {
		t.Fatalf("CreateChannel(replay after rotation) = (%#v, %v)", replayed, err)
	}
	for table, want := range map[string]int{"enrollment_grants": 2, "enrollment_grant_uses": 1,
		"enrollment_receipts": 1} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count = %d, %v, want %d", table, got, err, want)
		}
	}
}

func TestCreateChannelRejectsNinthChannelWithoutPartialState(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "capacity-owner")
	createdAt := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	insertChannelTestNode(t, st.db, owner, createdAt)
	for index := 0; index <= model.MaxChannelsPerNode; index++ {
		fixture := testkit.NewSignedChannelForOwnerAt(t, "capacity-"+string(rune('a'+index)), owner,
			createdAt.Add(time.Duration(index)*time.Minute))
		grantID, _ := model.ParseGrantID("grant-capacity-" + string(rune('a'+index)))
		token := storeTestEnrollmentToken(t, fixture.Descriptor(), owner, grantID,
			"capacity", fixture.Channel().CreatedAt(), 7)
		_, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: fixture.Channel(),
			Genesis: fixture.OwnerMember().Member(), Token: token})
		if index < model.MaxChannelsPerNode && err != nil {
			t.Fatalf("create Channel %d: %v", index+1, err)
		}
		if index == model.MaxChannelsPerNode && !errors.Is(err, ErrNodeChannelLimit) {
			t.Fatalf("ninth Channel error = %v", err)
		}
	}
	for table, want := range map[string]int{"channels": model.MaxChannelsPerNode,
		"channel_members": model.MaxChannelsPerNode, "enrollment_grants": model.MaxChannelsPerNode,
		"publication_epochs": model.MaxChannelsPerNode} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s after ninth rejection = %d, err=%v, want %d", table, got, err, want)
		}
	}
}

func TestCreateChannelRejectsInitialGrantThatDoesNotCoverAllRemainingSeats(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "create-short-initial-grant")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-create-short-initial")
	token := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), grantID,
		"short-initial", fixture.Channel().CreatedAt(), 1)
	_, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: token})
	if !errors.Is(err, ErrChannelCreateInput) {
		t.Fatalf("short initial grant error = %v", err)
	}
	for _, table := range []string{"channels", "channel_members", "publication_epochs", "enrollment_grants"} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != 0 {
			t.Fatalf("partial %s rows after input rejection = %d, %v", table, got, err)
		}
	}
}

func TestCreateChannelMapsFinalGrantConflictAndRollsBackAllNewState(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "create-final-grant-conflict-owner")
	createdAt := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	insertChannelTestNode(t, st.db, owner, createdAt)
	grantID, _ := model.ParseGrantID("grant-create-final-step-conflict")
	first := testkit.NewSignedChannelForOwnerAt(t, "create-final-grant-first", owner, createdAt)
	firstToken := storeTestEnrollmentToken(t, first.Descriptor(), owner, grantID, "first", createdAt, 7)
	if _, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: first.Channel(),
		Genesis: first.OwnerMember().Member(), Token: firstToken}); err != nil {
		t.Fatal(err)
	}
	secondCreatedAt := createdAt.Add(time.Minute)
	second := testkit.NewSignedChannelForOwnerAt(t, "create-final-grant-second", owner, secondCreatedAt)
	secondToken := storeTestEnrollmentToken(t, second.Descriptor(), owner, grantID,
		"second", secondCreatedAt, 7)
	_, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: second.Channel(),
		Genesis: second.OwnerMember().Member(), Token: secondToken})
	if !errors.Is(err, ErrChannelCreateConflict) {
		t.Fatalf("final grant conflict error = %v", err)
	}
	for table, predicate := range map[string]string{
		"channels":           "channel_id",
		"channel_members":    "channel_id",
		"publication_epochs": "channel_id",
	} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE "+predicate+"=?",
			second.Channel().ID().String()).Scan(&got); err != nil || got != 0 {
			t.Fatalf("partial %s rows for rejected Channel = %d, %v", table, got, err)
		}
	}
	for table, want := range map[string]int{"channels": 1, "channel_members": 1,
		"publication_epochs": 1, "enrollment_grants": 1} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count after final-step rollback = %d, %v, want %d", table, got, err, want)
		}
	}
}

func storeTestEnrollmentToken(t testing.TB, descriptor model.SignedChannelDescriptor,
	owner testkit.Identity, grantID model.GrantID, seed string, createdAt time.Time, maxUses uint8,
) model.EnrollmentToken {
	t.Helper()
	secret := model.Sum([]byte("store-test-enrollment-secret:" + seed + ":" + grantID.String())).Bytes()
	return storeTestEnrollmentTokenWithSecret(t, descriptor, owner, grantID, secret, createdAt, maxUses)
}

func storeTestEnrollmentTokenWithSecret(t testing.TB, descriptor model.SignedChannelDescriptor,
	owner testkit.Identity, grantID model.GrantID, secret []byte, createdAt time.Time, maxUses uint8,
) model.EnrollmentToken {
	t.Helper()
	return storeTestEnrollmentTokenWithAddresses(t, descriptor, owner, grantID, secret,
		owner.Multiaddrs(), createdAt, maxUses)
}

func storeTestEnrollmentTokenWithAddresses(t testing.TB, descriptor model.SignedChannelDescriptor,
	owner testkit.Identity, grantID model.GrantID, secret []byte, ownerMultiaddrs []string,
	createdAt time.Time, maxUses uint8,
) model.EnrollmentToken {
	t.Helper()
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: ownerMultiaddrs, GrantID: grantID, BearerSecret: secret,
		ExpiresAt: createdAt.Add(time.Hour), MaxUses: maxUses,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentTokenSigningMessage(descriptor.Descriptor().ID(), payload.Digest())
	if err != nil {
		t.Fatal(err)
	}
	token, err := model.AttachEnrollmentTokenSignature(payload,
		ed25519.Sign(ed25519Private(owner), message))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func storeTestEnrollmentGrant(t testing.TB, token model.EnrollmentToken,
	createdAt time.Time,
) model.OpenEnrollmentGrant {
	t.Helper()
	grant, err := model.NewOpenEnrollmentGrantForToken(token, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func assertEnrollmentCredentialAbsent(t testing.TB, path string, token model.EnrollmentToken) {
	t.Helper()
	secret := token.Payload().BearerSecret()
	credentialForms := [][]byte{secret, []byte(base64.StdEncoding.EncodeToString(secret)),
		token.Payload().RevealCanonicalJSON().Bytes(), []byte(token.Reveal())}
	for _, candidate := range []string{path, path + "-wal"} {
		raw, err := os.ReadFile(candidate)
		if err == nil {
			for _, credential := range credentialForms {
				if bytes.Contains(raw, credential) {
					t.Fatalf("bearer invite credential leaked into %s", candidate)
				}
			}
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

func insertChannelTestNode(t testing.TB, db *sql.DB, identity testkit.Identity, at time.Time) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO node(singleton,peer_id,origin_epoch,next_origin_seq,active_asset_rev,
		created_at,updated_at) VALUES(1,?,?,1,'asset-r5',?,?)`, identity.PeerID().String(),
		identity.OriginEpoch().String(), storeTime(at), storeTime(at))
	if err != nil {
		t.Fatalf("insert signed Channel test Node: %v", err)
	}
}

func signedBootstrapValues(t testing.TB, identity testkit.Identity, principal, workspace string,
	at time.Time,
) (model.Node, model.Profile) {
	t.Helper()
	node, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(), OriginEpoch: identity.OriginEpoch(),
		NextOriginSequence: 1, ActiveAssetRevision: "asset-r5", CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(), Principal: principal,
		WorkspaceRoot: workspace, Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash:      model.Sum([]byte("credential-" + identity.PeerID().String())),
		ActiveAssetRevision: "asset-r5", HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: false,
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return node, profile
}

func insertSignedChannelFixture(t testing.TB, db *sql.DB, fixture *testkit.SignedChannel,
	topicState model.TopicState,
) {
	t.Helper()
	projection := fixture.Projection()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,owner_public_key,
		descriptor_json,descriptor_digest,descriptor_signature,member_limit,roster_head_revision,
		roster_head_hash,status,topic_state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		projection.ChannelID, projection.Name, projection.LocalAlias, projection.OwnerPeerID,
		projection.OwnerPublicKey, projection.DescriptorJSON, projection.DescriptorDigest,
		projection.DescriptorSignature, projection.MemberLimit, projection.RosterHeadRevision,
		projection.RosterHeadHash, projection.Status, string(topicState), storeTime(fixture.Channel().CreatedAt()),
		storeTime(fixture.Channel().UpdatedAt())); err != nil {
		t.Fatalf("insert signed Channel projection: %v", err)
	}
	memberFixtures := fixture.Members()
	for index, member := range fixture.MemberProjections() {
		if _, err := tx.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
			member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,
			status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			member.ChannelID, member.Revision, member.RecordHash, nullableBytes(member.PreviousHash),
			member.MemberPeerID, member.OriginEpoch, member.DisplayLabel, member.PublicKey,
			member.MultiaddrsJSON, member.ProtocolsJSON, member.LimitsJSON, member.Status,
			member.SignedRecordJSON, member.OwnerSignature,
			storeTime(memberFixtures[index].Member().CreatedAt())); err != nil {
			t.Fatalf("insert signed MemberRecord projection: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit signed Channel fixture: %v", err)
	}
}

func insertSignedPeerBinding(t testing.TB, db *sql.DB, channelID model.ChannelID,
	member testkit.MemberFixture, alias string, state model.BindingState, reachability model.Reachability,
	joinedAt time.Time,
) {
	t.Helper()
	projection := member.Projection()
	_, err := db.Exec(`INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,effective_alias,public_key,
		multiaddrs_json,protocols_json,limits_json,member_revision,member_record_hash,state,reachability,
		joined_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, channelID.String(), projection.MemberPeerID,
		projection.OriginEpoch, alias, projection.PublicKey, projection.MultiaddrsJSON,
		projection.ProtocolsJSON, projection.LimitsJSON, projection.Revision, projection.RecordHash,
		string(state), string(reachability), storeTime(joinedAt))
	if err != nil {
		t.Fatalf("insert signed PeerBinding projection: %v", err)
	}
}

func nullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func ed25519Private(identity testkit.Identity) ed25519.PrivateKey {
	key, err := identity.Libp2pPrivateKey()
	if err != nil {
		panic(err)
	}
	raw, err := key.Raw()
	if err != nil {
		panic(err)
	}
	return ed25519.PrivateKey(append([]byte(nil), raw...))
}
