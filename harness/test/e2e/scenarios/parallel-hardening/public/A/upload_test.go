package upload

import (
	"os"
	"strings"
	"testing"
)

func TestSaveSmallUpload(t *testing.T) {
	root := t.TempDir()
	path, err := Save(root, "avatar.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello" {
		t.Fatalf("got %q", content)
	}
}
