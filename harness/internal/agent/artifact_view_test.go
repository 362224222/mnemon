package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReadonlyArtifactViewValidatorChecksExactReceiptPathOwnerAndModeWithoutReadingAuthority(t *testing.T) {
	operation := artifactResolverOperation(t, "view-filesystem", model.OperationTeamworkDeliver, nil)
	current, ref := artifactResolverCurrent(t, operation)
	workspace := t.TempDir()
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	path, _ := ref.ViewPath()
	absolute := filepath.Join(workspace, filepath.FromSlash(path))
	ordinal := filepath.Dir(absolute)
	t.Cleanup(func() { _ = os.Chmod(ordinal, artifactViewControlMode) })
	if err := os.MkdirAll(filepath.Dir(absolute), artifactViewControlMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(absolute, []byte("model-visible copy"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []struct {
		path string
		mode os.FileMode
	}{
		{nodeState, artifactViewControlMode},
		{filepath.Join(nodeState, "views"), artifactViewControlMode},
		{filepath.Join(nodeState, "views", operation.AgentRunID().String()), artifactViewControlMode},
		{filepath.Join(nodeState, "views", operation.AgentRunID().String(), "0"), artifactViewDirMode},
	} {
		if err := os.Chmod(directory.path, directory.mode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(absolute, artifactViewFileMode); err != nil {
		t.Fatal(err)
	}
	validator, err := NewReadonlyArtifactViewValidator(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), current, ref); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	// The view is not byte authority. A same-owner byte change with the exact
	// readonly mode still maps to the immutable CAS root in the receipt; the
	// acceptance transaction never hashes these bytes as a new producer.
	if err := os.Chmod(absolute, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(absolute, []byte("changed local copy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(absolute, artifactViewFileMode); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), current, ref); err != nil {
		t.Fatalf("Validate() treated view bytes as authority: %v", err)
	}

	if err := os.Chmod(absolute, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), current, ref); !errors.Is(err, ErrArtifactViewValidation) {
		t.Fatalf("mode drift error = %v", err)
	}
	if err := os.Chmod(ordinal, artifactViewControlMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(absolute); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "external")
	if err := os.WriteFile(external, []byte("outside"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, absolute); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ordinal, artifactViewDirMode); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), current, ref); !errors.Is(err, ErrArtifactViewValidation) {
		t.Fatalf("symlink view error = %v", err)
	}
	if got, err := os.ReadFile(external); err != nil || string(got) != "outside" {
		t.Fatalf("validator touched symlink target = %q, %v", got, err)
	}
}

func TestReadonlyArtifactViewValidatorRejectsWrongReceiptAndNodeReplacement(t *testing.T) {
	operation := artifactResolverOperation(t, "view-binding", model.OperationTeamworkDeliver, nil)
	current, ref := artifactResolverCurrent(t, operation)
	nodeState := t.TempDir()
	if err := os.Chmod(nodeState, artifactViewControlMode); err != nil {
		t.Fatal(err)
	}
	validator, err := NewReadonlyArtifactViewValidator(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := modelCurrentArtifactViewForTest(t, ref, "run-view-other")
	if err := validator.Validate(context.Background(), current, other); !errors.Is(err, ErrArtifactViewValidation) {
		t.Fatalf("foreign receipt path error = %v", err)
	}
	if err := os.Chmod(nodeState, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), current, ref); !errors.Is(err, ErrArtifactViewValidation) {
		t.Fatalf("Node mode replacement error = %v", err)
	}
}

func modelCurrentArtifactViewForTest(t *testing.T, source interface {
	RootDigest() model.Digest
}, run string) (model.CurrentArtifactRef, error) {
	t.Helper()
	return model.NewCurrentArtifactView(source.RootDigest(),
		".mnemon/harness/node/views/"+run+"/0/input.txt")
}
