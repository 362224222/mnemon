package architecture_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestReleaseDistributionPublishesWindowsArchives(t *testing.T) {
	root := repositoryRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".goreleaser.yml"))
	if err != nil {
		t.Fatal(err)
	}

	var config struct {
		Builds []struct {
			ID     string   `yaml:"id"`
			GOOS   []string `yaml:"goos"`
			GOARCH []string `yaml:"goarch"`
		} `yaml:"builds"`
		Archives []struct {
			IDs             []string `yaml:"ids"`
			Formats         []string `yaml:"formats"`
			FormatOverrides []struct {
				GOOS    string   `yaml:"goos"`
				Formats []string `yaml:"formats"`
			} `yaml:"format_overrides"`
		} `yaml:"archives"`
	}
	if err := yaml.Unmarshal(raw, &config); err != nil {
		t.Fatalf("parse .goreleaser.yml: %v", err)
	}

	if len(config.Builds) != 1 {
		t.Fatalf("release builds = %d, want one product build", len(config.Builds))
	}
	build := config.Builds[0]
	if build.ID != "mnemon" {
		t.Fatalf("release build ID = %q, want mnemon", build.ID)
	}
	if want := []string{"linux", "darwin", "windows"}; !slices.Equal(build.GOOS, want) {
		t.Fatalf("mnemon release operating systems = %v, want %v", build.GOOS, want)
	}
	if want := []string{"amd64", "arm64"}; !slices.Equal(build.GOARCH, want) {
		t.Fatalf("mnemon release architectures = %v, want %v", build.GOARCH, want)
	}

	if len(config.Archives) != 1 {
		t.Fatalf("release archives = %d, want one product archive", len(config.Archives))
	}
	archive := config.Archives[0]
	if !slices.Equal(archive.IDs, []string{"mnemon"}) {
		t.Fatalf("archive build IDs = %v, want [mnemon]", archive.IDs)
	}
	if !slices.Equal(archive.Formats, []string{"tar.gz"}) {
		t.Fatalf("default archive formats = %v, want [tar.gz]", archive.Formats)
	}
	if len(archive.FormatOverrides) != 1 {
		t.Fatalf("archive format overrides = %d, want one Windows override", len(archive.FormatOverrides))
	}
	windows := archive.FormatOverrides[0]
	if windows.GOOS != "windows" || !slices.Equal(windows.Formats, []string{"zip"}) {
		t.Fatalf("archive format override = (%q, %v), want (windows, [zip])",
			windows.GOOS, windows.Formats)
	}
}
