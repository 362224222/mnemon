package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshPristineAuthorityReturnsInitializedExactSnapshot(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	if proof, err := st.ReadMeshPristineAuthority(context.Background()); proof != (MeshPristineAuthority{}) || !errors.Is(err, ErrMeshPristineAuthority) ||
		!errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("missing authority proof = (%#v,%v)", proof, err)
	}

	node, profile := bootstrapValues(t, "peer-mesh-pristine", "principal-mesh-pristine", t.TempDir())
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	proof, err := st.ReadMeshPristineAuthority(context.Background())
	if err != nil || proof.Node().PeerID() != node.PeerID() ||
		proof.Node().OriginEpoch() != node.OriginEpoch() ||
		proof.Profile().ID() != profile.ID() ||
		proof.Profile().CredentialHash() != profile.CredentialHash() {
		t.Fatalf("pristine authority proof = (%#v,%v)", proof, err)
	}
	if _, err := st.ReadMeshPristineAuthority(nil); !errors.Is(err, ErrMeshPristineAuthority) {
		t.Fatalf("nil context error = %v", err)
	}
}

func TestMeshPristineAuthorityPreservesCanceledReadWithoutInvariant(t *testing.T) {
	t.Parallel()
	st := initializedMeshPristineTestStore(t, "canceled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proof, err := st.ReadMeshPristineAuthority(ctx)
	if proof != (MeshPristineAuthority{}) || !errors.Is(err, context.Canceled) ||
		!errors.Is(err, ErrMeshPristineAuthority) || errors.Is(err, ErrChannelAuthorityInvariant) ||
		errors.Is(err, ErrMeshNotPristine) {
		t.Fatalf("canceled pristine proof = (%#v,%v)", proof, err)
	}
}

func TestMeshPristineAuthorityRejectsInstalledReservedAndUnknownMeshState(t *testing.T) {
	t.Parallel()
	t.Run("installed Channel authority", func(t *testing.T) {
		t.Parallel()
		st := openTestStore(t)
		fixture := testkit.NewSignedChannel(t, "mesh-pristine-installed")
		node, profile := signedBootstrapValues(t, fixture.Owner(),
			"principal-mesh-pristine-installed", t.TempDir(), fixture.Channel().CreatedAt())
		if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
			t.Fatal(err)
		}
		grantID, _ := model.ParseGrantID("grant-mesh-pristine-installed")
		token := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), grantID,
			"mesh-pristine-installed", fixture.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
		if _, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: fixture.Channel(),
			Genesis: fixture.OwnerMember().Member(), Token: token}); err != nil {
			t.Fatal(err)
		}
		assertMeshNotPristine(t, st)
	})

	for _, state := range []string{"reserved", "commit_unknown"} {
		state := state
		t.Run("join "+state, func(t *testing.T) {
			t.Parallel()
			fixture := newChannelEnrollmentFixture(t, "mesh-pristine-"+state)
			st := openTestStore(t)
			node, profile := signedBootstrapValues(t, fixture.joiner,
				"principal-mesh-pristine-"+state, t.TempDir(), fixture.channel.Channel().CreatedAt())
			if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
				t.Fatal(err)
			}
			reservation := reserveMeshPristineJoin(t, st, fixture, "mesh-pristine-"+state)
			if state == "commit_unknown" {
				if err := st.MarkJoinedChannelCommitUnknown(context.Background(), reservation.RequestID,
					fixture.joiner.PeerID(), reservation.Attempt, fixture.acceptedAt.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			assertMeshNotPristine(t, st)
		})
	}
}

func TestMeshPristineAuthorityHandlesRichValidMeshAsNonPristine(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "mesh-pristine-rich")
	remote := channel.AppendActive(t, "mesh-pristine-rich-remote")
	node, profile := signedBootstrapValues(t, channel.Owner(), "principal-mesh-pristine-rich",
		t.TempDir(), channel.Channel().CreatedAt())
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoined)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote, "rich-remote",
		model.BindingPending, model.ReachabilityUnknown, channel.Channel().CreatedAt())
	mustExec(t, st, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.Channel().ID().String(), channel.Owner().PeerID().String(),
		channel.Owner().OriginEpoch().String(), storeTime(channel.Channel().UpdatedAt()))
	fixture := peerInboxFixture{channelBaselineFixture: channelBaselineFixture{store: st,
		channel: channel, remote: remote, at: channel.Channel().UpdatedAt().Add(time.Second)}}
	if _, err := st.InstallInboundChannelBaseline(context.Background(), InstallInboundChannelBaselineSpec{
		AuthenticatedPeerID: remote.Identity().PeerID(), Baseline: fixture.remoteBaseline(0),
		At: fixture.at}); err != nil {
		t.Fatal(err)
	}
	fixture.at = fixture.at.Add(time.Second)
	publication := fixture.publication(t, 1, 1, "mesh-pristine-rich", true)
	if result := fixture.put(t, publication, fixture.at); result.Disposition != PeerInboxStored {
		t.Fatalf("rich Inbox result = %#v", result)
	}
	assertMeshNotPristine(t, st)
}

func TestMeshPristineAuthorityTreatsStandaloneHandlingPinAsMeshEvidence(t *testing.T) {
	t.Parallel()
	st := initializedMeshPristineTestStore(t, "handling-pin")
	root := verifiedRoot(t, "mesh-pristine-handling-pin",
		`{"entries":[],"kind":"report","total_bytes":0}`, 0)
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReadMeshPristineAuthority(context.Background()); err != nil {
		t.Fatalf("standalone Artifact root changed mesh proof: %v", err)
	}
	mustExec(t, st, `INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,created_at)
		VALUES(?,'handling','handling-standalone',?)`, root.RootDigest.String(), storeTime(root.VerifiedAt))
	assertMeshNotPristine(t, st)
}

func TestMeshPristineTableRegistryClassifiesExactSQLiteSchema(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	rows, err := st.db.Query(`SELECT name FROM sqlite_schema
		WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	schemaTables := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		schemaTables[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	classified := make(map[string]struct{}, len(meshPristineTables))
	for _, table := range meshPristineTables {
		if _, duplicate := classified[table.name]; duplicate || table.name == "" {
			t.Fatalf("duplicate or empty table classification %q", table.name)
		}
		if table.class < meshTableBootstrap || table.class > meshTableNonMesh ||
			table.meshPredicate != "" && table.class != meshTableEvidence {
			t.Fatalf("invalid table classification %#v", table)
		}
		classified[table.name] = struct{}{}
	}
	for name := range schemaTables {
		if _, ok := classified[name]; !ok {
			t.Errorf("schema table %q is unclassified", name)
		}
	}
	for name := range classified {
		if _, ok := schemaTables[name]; !ok {
			t.Errorf("classification names absent table %q", name)
		}
	}
}

func TestMeshPristineAuthorityPreservesCorruptionCategory(t *testing.T) {
	t.Parallel()
	t.Run("Channel projection", func(t *testing.T) {
		t.Parallel()
		st := openTestStore(t)
		fixture := testkit.NewSignedChannel(t, "mesh-pristine-corrupt-channel")
		node, profile := signedBootstrapValues(t, fixture.Owner(),
			"principal-mesh-pristine-corrupt-channel", t.TempDir(), fixture.Channel().CreatedAt())
		if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
			t.Fatal(err)
		}
		insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
		mustExec(t, st, `DROP TRIGGER channels_descriptor_immutable`)
		mustExec(t, st, `UPDATE channels SET name='forged' WHERE channel_id=?`,
			fixture.Channel().ID().String())
		assertMeshPristineInvariant(t, st)
	})

	t.Run("join reservation identity", func(t *testing.T) {
		t.Parallel()
		fixture := newChannelEnrollmentFixture(t, "mesh-pristine-corrupt-reservation")
		st := openTestStore(t)
		node, profile := signedBootstrapValues(t, fixture.joiner,
			"principal-mesh-pristine-corrupt-reservation", t.TempDir(),
			fixture.channel.Channel().CreatedAt())
		if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
			t.Fatal(err)
		}
		reserveMeshPristineJoin(t, st, fixture, "mesh-pristine-corrupt-reservation")
		mustExec(t, st, `DROP TRIGGER channel_join_reservations_identity_immutable`)
		mustExec(t, st, `UPDATE channel_join_reservations SET local_peer_id='peer-forged'`)
		assertMeshPristineInvariant(t, st)
	})

	t.Run("Inbox pressure", func(t *testing.T) {
		t.Parallel()
		st := initializedMeshPristineTestStore(t, "pressure")
		mustExec(t, st, `UPDATE peer_inbox_node_pressure SET pending_bytes=1 WHERE singleton_id=1`)
		assertMeshPristineInvariant(t, st)
	})

	t.Run("publication sequence", func(t *testing.T) {
		t.Parallel()
		st := initializedMeshPristineTestStore(t, "sequence")
		mustExec(t, st, `UPDATE node SET next_origin_seq=2 WHERE singleton=1`)
		assertMeshPristineInvariant(t, st)
	})
}

func reserveMeshPristineJoin(t *testing.T, st *Store, fixture channelEnrollmentFixture,
	localAlias string,
) PrepareJoinedChannelResult {
	t.Helper()
	result, err := st.PrepareJoinedChannel(context.Background(), PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: fixture.joiner.PeerID(), LocalPublicKey: fixture.joiner.PublicKey(),
		Descriptor: fixture.channel.Descriptor(), GrantID: fixture.grantID, LocalAlias: localAlias,
		At: fixture.acceptedAt,
	})
	if err != nil || !result.Reserved || result.Attempt != 1 {
		t.Fatalf("PrepareJoinedChannel() = (%#v,%v)", result, err)
	}
	return result
}

func initializedMeshPristineTestStore(t *testing.T, seed string) *Store {
	t.Helper()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-mesh-pristine-"+seed,
		"principal-mesh-pristine-"+seed, t.TempDir())
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	return st
}

func assertMeshNotPristine(t *testing.T, st *Store) {
	t.Helper()
	proof, err := st.ReadMeshPristineAuthority(context.Background())
	if proof != (MeshPristineAuthority{}) || !errors.Is(err, ErrMeshPristineAuthority) ||
		!errors.Is(err, ErrMeshNotPristine) || errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("non-pristine proof = (%#v,%v)", proof, err)
	}
}

func assertMeshPristineInvariant(t *testing.T, st *Store) {
	t.Helper()
	proof, err := st.ReadMeshPristineAuthority(context.Background())
	if proof != (MeshPristineAuthority{}) || !errors.Is(err, ErrMeshPristineAuthority) ||
		!errors.Is(err, ErrChannelAuthorityInvariant) || errors.Is(err, ErrMeshNotPristine) {
		t.Fatalf("corrupt mesh authority proof = (%#v,%v)", proof, err)
	}
}
