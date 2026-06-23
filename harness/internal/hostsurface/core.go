package hostsurface

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"
)

// corePaths is the host config dir plus the project-local Mnemon state dir.
type corePaths struct {
	configDir string
	mnemonDir string
}

// projectorCore is the small host-io core shared by the R1 static host shim.
type projectorCore struct {
	host        string
	projectRoot string
	paths       corePaths
	dryRun      bool
	stdout      io.Writer
	stderr      io.Writer
	managed     *managedState
}

type hostProjectionManifest struct {
	SchemaVersion int                         `json:"schema_version"`
	Host          string                      `json:"host"`
	UpdatedAt     string                      `json:"updated_at,omitempty"`
	ProjectRoot   string                      `json:"project_root,omitempty"`
	MnemonDir     string                      `json:"mnemon_dir,omitempty"`
	Store         string                      `json:"store,omitempty"`
	Loops         map[string]hostManifestLoop `json:"loops,omitempty"`
}

type hostManifestLoop struct {
	LoopPath         string              `json:"loop_path"`
	LoopVersion      string              `json:"loop_version,omitempty"`
	StatePath        string              `json:"state_path"`
	IntentPolicy     string              `json:"intent_policy"`
	StatusPath       string              `json:"status_path"`
	Projection       map[string]any      `json:"projection"`
	Reality          map[string]any      `json:"reality"`
	Reconcile        map[string]any      `json:"reconcile"`
	LifecycleMapping map[string]string   `json:"lifecycle_mapping"`
	Surfaces         map[string]string   `json:"surfaces"`
	Ownership        projectionOwnership `json:"ownership"`
}

type projectionOwnership struct {
	Files         []string          `json:"files,omitempty"`
	Dirs          []string          `json:"dirs,omitempty"`
	Hashes        map[string]string `json:"hashes,omitempty"`
	Preserved     []string          `json:"preserved,omitempty"`
	MarkerVersion int               `json:"marker_version,omitempty"`
}

// pathJoin is the package's display-path primitive: forward-slash joins for host surfaces
// regardless of OS, so projected refs read identically on every platform.
func pathJoin(base string, elems ...string) string {
	parts := append([]string{base}, elems...)
	return path.Join(parts...)
}

func (c projectorCore) resolve(displayPath string) string {
	if filepath.IsAbs(displayPath) {
		return filepath.Clean(displayPath)
	}
	return filepath.Join(c.projectRoot, filepath.FromSlash(displayPath))
}

func (c projectorCore) writeFile(dstDisplay string, data []byte, mode os.FileMode) error {
	if c.dryRun {
		c.printf("would write %s\n", dstDisplay)
		return nil
	}
	dst := c.resolve(dstDisplay)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", dstDisplay, err)
	}
	if err := os.Chmod(dst, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", dstDisplay, err)
	}
	return nil
}

func (c projectorCore) writeJSON(dstDisplay string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", dstDisplay, err)
	}
	data = append(data, '\n')
	return c.writeFile(dstDisplay, data, mode)
}

func (c projectorCore) printf(format string, args ...any) {
	fmt.Fprintf(c.stdout, format, args...)
}

func (c projectorCore) hostManifestPath() string {
	return pathJoin(c.paths.mnemonDir, "hosts", c.host, "manifest.json")
}

func (c projectorCore) removeHostManifestLoop(loopName string) error {
	manifestPath := c.resolve(c.hostManifestPath())
	data, err := os.ReadFile(manifestPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read host manifest %s: %w", c.hostManifestPath(), err)
	}
	var manifest hostProjectionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parse host manifest %s: %w", c.hostManifestPath(), err)
	}
	delete(manifest.Loops, loopName)
	if len(manifest.Loops) == 0 {
		if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove host manifest: %w", err)
		}
		return nil
	}
	manifest.UpdatedAt = nowUTC()
	return c.writeJSON(c.hostManifestPath(), manifest, 0o644)
}

func nowUTC() string {
	return time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
}
