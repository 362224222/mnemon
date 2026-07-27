package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestVerifiedArtifactClosureCheckpointSealsMapAndReplaysAfterRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	closure := artifactClosureFixture(t, "one")
	first, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure)
	if err != nil || first.Replayed || len(first.Closure.Roots) != 2 || len(first.Closure.RootBlocks) != 3 {
		t.Fatalf("first checkpoint = (%#v, %v)", first, err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
	if _, err := st.db.Exec(`INSERT INTO artifact_root_blocks(root_digest,ordinal,logical_path,
		offset_bytes,length_bytes,block_digest,mode) VALUES(?,9,'later',0,3,?,384)`,
		closure.Roots[0].RootDigest.String(), closure.Blocks[0].Digest.String()); err == nil {
		t.Fatal("verified root block map accepted an append")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	replayInput := cloneVerifiedClosure(closure)
	for index := range replayInput.Roots {
		replayInput.Roots[index].CreatedAt = replayInput.Roots[index].CreatedAt.Add(time.Hour)
		replayInput.Roots[index].VerifiedAt = replayInput.Roots[index].VerifiedAt.Add(time.Hour)
	}
	for index := range replayInput.Blocks {
		replayInput.Blocks[index].CreatedAt = replayInput.Blocks[index].CreatedAt.Add(time.Hour)
	}
	replayed, err := restarted.CheckpointVerifiedArtifactClosure(context.Background(), replayInput)
	if err != nil || !replayed.Replayed ||
		!replayed.Closure.Roots[0].CreatedAt.Equal(closure.Roots[0].CreatedAt) ||
		!replayed.Closure.Blocks[0].CreatedAt.Equal(closure.Blocks[0].CreatedAt) {
		t.Fatalf("restart replay = (%#v, %v)", replayed, err)
	}
	assertArtifactClosureCounts(t, restarted, 2, 2, 3)
}

func TestVerifiedArtifactClosureSharesBlocksAndPromotesExistingStage(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	closure := artifactClosureFixture(t, "shared")
	root := closure.Roots[0]
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest,manifest_json,manifest_digest,
		total_bytes,state,created_at) VALUES(?,?,?,?,'staged',?)`, root.RootDigest.String(),
		root.Manifest.Bytes(), root.ManifestDigest.Bytes(), root.TotalBytes, storeTime(root.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	result, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure)
	if err != nil || result.Replayed {
		t.Fatalf("staged checkpoint = (%#v, %v)", result, err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
	if _, err := st.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); err != nil {
		t.Fatal(err)
	}
}

func TestVerifiedArtifactClosureRaisesNewRootVerificationToSharedBlockTime(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	closure := artifactClosureFixture(t, "shared-block-time")
	blockUses := make(map[model.Digest]int)
	for _, row := range closure.RootBlocks {
		blockUses[row.BlockDigest]++
	}
	var sharedDigest model.Digest
	for digest, uses := range blockUses {
		if uses == 1 {
			sharedDigest = digest
			break
		}
	}
	if sharedDigest.IsZero() {
		t.Fatal("fixture has no block isolated to one new root")
	}
	var shared VerifiedArtifactBlock
	for _, block := range closure.Blocks {
		if block.Digest == sharedDigest {
			shared = block
			break
		}
	}
	if shared.Digest.IsZero() {
		t.Fatal("fixture shared block is missing")
	}
	laterAt := shared.CreatedAt.Add(time.Hour)
	if _, err := st.db.Exec(`INSERT INTO artifact_blocks(block_digest,size_bytes,created_at)
		VALUES(?,?,?)`, shared.Digest.String(), shared.SizeBytes, storeTime(laterAt)); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := st.CheckpointVerifiedArtifactClosure(context.Background(), closure)
	if err != nil || checkpoint.Replayed {
		t.Fatalf("shared-block time checkpoint = (%#v,%v)", checkpoint, err)
	}
	for _, root := range checkpoint.Closure.Roots {
		usesShared := false
		for _, row := range closure.RootBlocks {
			usesShared = usesShared || row.RootDigest == root.RootDigest && row.BlockDigest == shared.Digest
		}
		want := closure.Roots[0].VerifiedAt
		for _, requested := range closure.Roots {
			if requested.RootDigest == root.RootDigest {
				want = requested.VerifiedAt
				break
			}
		}
		if usesShared {
			want = laterAt
		}
		if !root.VerifiedAt.Equal(want) {
			t.Fatalf("root %s verified_at = %s, want %s", root.RootDigest,
				root.VerifiedAt, want)
		}
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
}

func TestVerifiedArtifactClosureConflictRollsBackEveryNewRow(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := artifactClosureFixture(t, "base")
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	conflict := artifactClosureFixture(t, "conflict")
	changedDigest := conflict.Blocks[0].Digest
	conflict.Blocks[0].SizeBytes++
	for index := range conflict.RootBlocks {
		if conflict.RootBlocks[index].BlockDigest != changedDigest {
			continue
		}
		conflict.RootBlocks[index].LengthBytes++
		for rootIndex := range conflict.Roots {
			if conflict.Roots[rootIndex].RootDigest == conflict.RootBlocks[index].RootDigest {
				conflict.Roots[rootIndex].TotalBytes++
			}
		}
	}
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), conflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting block error = %v", err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)

	mapConflict := base
	mapConflict.RootBlocks[0].Mode = 0o644
	if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), mapConflict); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("conflicting root map error = %v", err)
	}
	assertArtifactClosureCounts(t, st, 2, 2, 3)
}

func TestVerifiedArtifactClosureRejectsMalformedRelationalShape(t *testing.T) {
	t.Parallel()
	base := artifactClosureFixture(t, "malformed")
	tests := []struct {
		name   string
		mutate func(*VerifiedArtifactClosure)
	}{
		{"root order", func(value *VerifiedArtifactClosure) {
			value.Roots[0], value.Roots[1] = value.Roots[1], value.Roots[0]
		}},
		{"block order", func(value *VerifiedArtifactClosure) {
			value.Blocks[0], value.Blocks[1] = value.Blocks[1], value.Blocks[0]
		}},
		{"ordinal gap", func(value *VerifiedArtifactClosure) { value.RootBlocks[1].Ordinal = 2 }},
		{"offset gap", func(value *VerifiedArtifactClosure) { value.RootBlocks[1].OffsetBytes++ }},
		{"unknown root", func(value *VerifiedArtifactClosure) {
			value.RootBlocks[0].RootDigest = model.Sum([]byte("unknown-root"))
		}},
		{"unknown block", func(value *VerifiedArtifactClosure) {
			value.RootBlocks[0].BlockDigest = model.Sum([]byte("unknown-block"))
		}},
		{"unsafe path", func(value *VerifiedArtifactClosure) { value.RootBlocks[0].LogicalPath = "../secret" }},
		{"root total drift", func(value *VerifiedArtifactClosure) { value.Roots[0].TotalBytes++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st := openTestStore(t)
			value := cloneVerifiedClosure(base)
			test.mutate(&value)
			if _, err := st.CheckpointVerifiedArtifactClosure(context.Background(), value); err == nil {
				t.Fatal("malformed closure was accepted")
			}
			assertArtifactClosureCounts(t, st, 0, 0, 0)
		})
	}
}

func artifactClosureFixture(t *testing.T, suffix string) VerifiedArtifactClosure {
	t.Helper()
	at := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	contentA := []byte("abc")
	contentB := []byte("defgh")
	blockA, blockB := model.Sum(contentA), model.Sum(contentB)
	manifestA, _ := model.NewJSON([]byte(`{"entries":[{"logical_path":"a.txt"}],"kind":"file"}`))
	manifestB, _ := model.NewJSON([]byte(`{"entries":[{"logical_path":"b.txt"},{"logical_path":"c.txt"}],"kind":"directory"}`))
	rootA := VerifiedArtifactRoot{RootDigest: model.Sum([]byte("root-a-" + suffix)), Manifest: manifestA,
		ManifestDigest: model.Sum(manifestA.Bytes()), TotalBytes: 3, CreatedAt: at, VerifiedAt: at}
	rootB := VerifiedArtifactRoot{RootDigest: model.Sum([]byte("root-b-" + suffix)), Manifest: manifestB,
		ManifestDigest: model.Sum(manifestB.Bytes()), TotalBytes: 8, CreatedAt: at, VerifiedAt: at}
	roots := []VerifiedArtifactRoot{rootA, rootB}
	if roots[0].RootDigest.String() > roots[1].RootDigest.String() {
		roots[0], roots[1] = roots[1], roots[0]
	}
	blocks := []VerifiedArtifactBlock{{Digest: blockA, SizeBytes: 3, CreatedAt: at},
		{Digest: blockB, SizeBytes: 5, CreatedAt: at}}
	if blocks[0].Digest.String() > blocks[1].Digest.String() {
		blocks[0], blocks[1] = blocks[1], blocks[0]
	}
	rowsByRoot := map[model.Digest][]VerifiedArtifactRootBlock{
		rootA.RootDigest: {{RootDigest: rootA.RootDigest, LogicalPath: "a.txt", LengthBytes: 3,
			BlockDigest: blockA, Mode: 0o600}},
		rootB.RootDigest: {{RootDigest: rootB.RootDigest, LogicalPath: "b.txt", LengthBytes: 3,
			BlockDigest: blockA, Mode: 0o600}, {RootDigest: rootB.RootDigest, Ordinal: 1,
			LogicalPath: "c.txt", LengthBytes: 5, BlockDigest: blockB, Mode: 0o600}},
	}
	rows := append([]VerifiedArtifactRootBlock{}, rowsByRoot[roots[0].RootDigest]...)
	rows = append(rows, rowsByRoot[roots[1].RootDigest]...)
	return VerifiedArtifactClosure{Roots: roots, Blocks: blocks, RootBlocks: rows}
}

func cloneVerifiedClosure(value VerifiedArtifactClosure) VerifiedArtifactClosure {
	return VerifiedArtifactClosure{Roots: append([]VerifiedArtifactRoot{}, value.Roots...),
		Blocks:     append([]VerifiedArtifactBlock{}, value.Blocks...),
		RootBlocks: append([]VerifiedArtifactRootBlock{}, value.RootBlocks...)}
}

func assertArtifactClosureCounts(t *testing.T, st *Store, roots, blocks, maps int) {
	t.Helper()
	for _, check := range []struct {
		table string
		want  int
	}{{"artifact_roots", roots}, {"artifact_blocks", blocks}, {"artifact_root_blocks", maps}} {
		var got int
		if err := st.db.QueryRow("SELECT COUNT(*) FROM " + check.table).Scan(&got); err != nil || got != check.want {
			t.Fatalf("%s count = (%d, %v), want %d", check.table, got, err, check.want)
		}
	}
}
