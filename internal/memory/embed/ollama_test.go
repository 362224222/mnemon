package embed

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClientWithModel_DefaultModel(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{})
	if c.Model() != DefaultModel {
		t.Errorf("default model: want %q, got %q", DefaultModel, c.Model())
	}
}

func TestNewClientWithModel_FileModel(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{Model: "file-model"})
	if c.Model() != "file-model" {
		t.Errorf("file model: want %q, got %q", "file-model", c.Model())
	}
}

func TestNewClientWithModel_Explicit(t *testing.T) {
	c := newClientWithModel("explicit-model:v1", EmbedConfigFile{})
	if c.Model() != "explicit-model:v1" {
		t.Errorf("explicit model: want %q, got %q", "explicit-model:v1", c.Model())
	}
}

func TestNewClientWithModel_ExplicitWinsOverFile(t *testing.T) {
	c := newClientWithModel("explicit-model", EmbedConfigFile{Model: "file-model"})
	if c.Model() != "explicit-model" {
		t.Errorf("explicit-over-file: want %q, got %q", "explicit-model", c.Model())
	}
}

func TestNewClientWithModel_EmptyFallsBackToFile(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{Model: "file-model"})
	if c.Model() != "file-model" {
		t.Errorf("empty-falls-to-file: want %q, got %q", "file-model", c.Model())
	}
}

func TestNewClientWithModel_EmptyAndNoFileFallsBackToDefault(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{})
	if c.Model() != DefaultModel {
		t.Errorf("empty-and-no-file: want %q, got %q", DefaultModel, c.Model())
	}
}

func TestNewClientWithModel_DefaultEndpoint(t *testing.T) {
	c := newClientWithModel("any-model", EmbedConfigFile{})
	if c.Endpoint() != DefaultEndpoint {
		t.Errorf("default endpoint: want %q, got %q", DefaultEndpoint, c.Endpoint())
	}
}

// TestNewClientWithModel_ExplicitEmptyTreatedAsUnset documents the deliberate
// choice that --embed-model "" falls through to the file value (or built-in
// default) rather than being rejected. This matches how the existing
// --data-dir flag handles empty strings.
func TestNewClientWithModel_ExplicitEmptyTreatedAsUnset(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{Model: "file-model"})
	if c.Model() != "file-model" {
		t.Errorf("explicit empty should fall through to file: want %q, got %q", "file-model", c.Model())
	}

	c = newClientWithModel("", EmbedConfigFile{})
	if c.Model() != DefaultModel {
		t.Errorf("explicit empty + no file should fall through to default: want %q, got %q", DefaultModel, c.Model())
	}
}

func TestOllamaEndpointWithTrailingSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("expected Ollama request without Authorization header, got %q", got)
		}
		switch r.URL.Path {
		case "/api/tags":
			w.WriteHeader(http.StatusOK)
		case "/api/embed":
			if r.Method != http.MethodPost {
				t.Errorf("expected POST, got %s", r.Method)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("expected application/json, got %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"embeddings":[[0.1,0.2,0.3]]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newClientWithModel("", EmbedConfigFile{Provider: "ollama", Endpoint: srv.URL + "/", APIKey: "must-not-be-sent"})
	if !c.Available() {
		t.Fatal("expected Available() true for trailing-slash Ollama endpoint")
	}
	vec, err := c.Embed("hello")
	if err != nil {
		t.Fatalf("Embed with trailing-slash Ollama endpoint: %v", err)
	}
	if len(vec) != 3 {
		t.Fatalf("expected 3 dims, got %d", len(vec))
	}
}
