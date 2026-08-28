package embed

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadEmbedConfigFromPath verifies the embed.yml file is parsed into the
// expected struct fields, including the api_key mapping.
func TestLoadEmbedConfigFromPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "embed.yml")
	content := `provider: openai
model: BAAI/bge-m3
endpoint: https://api.siliconflow.cn/v1
api_key: sk-from-file
dimensions: 1024
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg := loadEmbedConfigFromPath(path)
	if cfg.Provider != "openai" {
		t.Errorf("Provider = %q, want openai", cfg.Provider)
	}
	if cfg.Model != "BAAI/bge-m3" {
		t.Errorf("Model = %q, want BAAI/bge-m3", cfg.Model)
	}
	if cfg.Endpoint != "https://api.siliconflow.cn/v1" {
		t.Errorf("Endpoint = %q", cfg.Endpoint)
	}
	if cfg.APIKey != "sk-from-file" {
		t.Errorf("APIKey = %q, want sk-from-file", cfg.APIKey)
	}
	if cfg.Dimensions != 1024 {
		t.Errorf("Dimensions = %d, want 1024", cfg.Dimensions)
	}
}

// TestLoadEmbedConfigFromPathMissingFile returns a zero value without error.
func TestLoadEmbedConfigFromPathMissingFile(t *testing.T) {
	cfg := loadEmbedConfigFromPath(filepath.Join(t.TempDir(), "nope.yml"))
	if cfg != (EmbedConfigFile{}) {
		t.Errorf("expected zero EmbedConfigFile, got %+v", cfg)
	}
}

// TestNewClientWithModelFileFallback confirms embed.yml supplies all values
// when no environment variable overrides them (env vars are no longer read).
func TestNewClientWithModelFileFallback(t *testing.T) {
	file := EmbedConfigFile{
		Provider:   "openai",
		Endpoint:   "https://api.siliconflow.cn/v1",
		Model:      "BAAI/bge-m3",
		APIKey:     "sk-file",
		Dimensions: 1024,
	}
	c := newClientWithModel("", file)

	if c.Endpoint() != "https://api.siliconflow.cn/v1" {
		t.Errorf("Endpoint = %q, want file value", c.Endpoint())
	}
	if c.Model() != "BAAI/bge-m3" {
		t.Errorf("Model = %q, want file value", c.Model())
	}
	if c.apiKey != "sk-file" {
		t.Errorf("apiKey = %q, want file value", c.apiKey)
	}
	if c.Protocol() != ProtocolOpenAI {
		t.Errorf("Protocol = %q, want openai", c.Protocol())
	}
	if c.dims != 1024 {
		t.Errorf("dims = %d, want 1024 (file)", c.dims)
	}
}

// TestNewClientWithModelFileProviderDefaultsToSiliconFlow checks that a file
// with provider: openai and no endpoint falls back to the built-in SiliconFlow
// default; other endpoints are selected via the file's endpoint field.
func TestNewClientWithModelFileProviderDefaultsToSiliconFlow(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{Provider: "openai"})
	if c.Protocol() != ProtocolOpenAI {
		t.Errorf("Protocol = %q, want openai", c.Protocol())
	}
	if c.Endpoint() != DefaultOpenAIEndpoint {
		t.Errorf("Endpoint = %q, want %q", c.Endpoint(), DefaultOpenAIEndpoint)
	}
}
