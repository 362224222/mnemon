package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// File is the 4-layer config that drives select-only capability assembly: where the store/endpoint
// live (local), how the channel authenticates (channel), which built-in capabilities are enabled and
// bound (capabilities), and which background workers run (background). It enables/binds/limits
// already-compiled capabilities; it can never define new behavior (the assembler is fail-closed).
type File struct {
	Local        localConfig                 `json:"local"`
	Channel      ChannelConfig               `json:"channel"`
	Capabilities map[string]CapabilityConfig `json:"capabilities"`
	Background   BackgroundConfig            `json:"background"`
}

type localConfig struct {
	StorePath string `json:"store_path,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
}

type ChannelConfig struct {
	BindingFile    string `json:"binding_file,omitempty"`
	CredentialsDir string `json:"credentials_dir,omitempty"`
}

// CapabilityConfig enables and bounds one built-in policy. RuleRef ("native:<id>") selects the
// compiled rule kind; the assembler resolves it select-only and fails closed on an unknown id.
type CapabilityConfig struct {
	Enabled         bool   `json:"enabled"`
	ResourceRef     string `json:"resource_ref,omitempty"`
	MaxPayloadBytes int    `json:"max_payload_bytes,omitempty"`
	RuleRef         string `json:"rule_ref,omitempty"` // "native:<id>"
}

type BackgroundConfig struct {
	Sync                    string `json:"sync,omitempty"`                      // "disabled" | "manual"
	PresentationViewRefresh string `json:"presentation_view_refresh,omitempty"` // "manual"
}

// Load reads and validates a config File. It is fail-closed: an unknown field anywhere in the document
// is rejected (DisallowUnknownFields), and an enabled capability must carry a native rule_ref.
func Load(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("read config %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var f File
	if err := dec.Decode(&f); err != nil {
		return File{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := f.validate(); err != nil {
		return File{}, fmt.Errorf("config %s: %w", path, err)
	}
	return f, nil
}

func (f File) validate() error {
	for name, c := range f.Capabilities {
		if !c.Enabled {
			continue
		}
		if !strings.HasPrefix(c.RuleRef, "native:") {
			return fmt.Errorf("capability %q: rule_ref must be \"native:<id>\", got %q", name, c.RuleRef)
		}
	}
	switch f.Background.Sync {
	case "", "disabled", "manual":
	default:
		return fmt.Errorf("background.sync must be disabled or manual, got %q", f.Background.Sync)
	}
	return nil
}
