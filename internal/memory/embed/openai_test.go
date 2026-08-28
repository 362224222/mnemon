package embed

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProtocolAutoDetect(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{Endpoint: "http://127.0.0.1:18000/v1"})
	if c.Protocol() != ProtocolOpenAI {
		t.Fatalf("expected openai protocol for /v1 endpoint, got %q", c.Protocol())
	}

	c = newClientWithModel("", EmbedConfigFile{Endpoint: "http://localhost:11434"})
	if c.Protocol() != ProtocolOllama {
		t.Fatalf("expected ollama protocol for default endpoint, got %q", c.Protocol())
	}

	// Explicit protocol override wins over auto-detection.
	c = newClientWithModel("", EmbedConfigFile{Endpoint: "http://127.0.0.1:18000/v1", Provider: "ollama"})
	if c.Protocol() != ProtocolOllama {
		t.Fatalf("expected explicit protocol override to win, got %q", c.Protocol())
	}

	c = newClientWithModel("", EmbedConfigFile{Endpoint: "http://localhost:11434", Provider: "openai"})
	if c.Protocol() != ProtocolOpenAI {
		t.Fatalf("expected explicit openai protocol, got %q", c.Protocol())
	}
}

func TestOpenAIAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("expected /v1/models, got %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-test" {
			t.Errorf("expected Bearer sk-test, got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newClientWithModel("", EmbedConfigFile{Endpoint: srv.URL + "/v1", Provider: "openai", APIKey: "sk-test"})
	if !c.Available() {
		t.Fatal("expected Available() true for 200 /v1/models")
	}
}

func TestOpenAIEndpointWithTrailingSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.WriteHeader(http.StatusOK)
		case "/v1/embeddings":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"embedding":[1.0,2.0]}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := newClientWithModel("", EmbedConfigFile{Endpoint: srv.URL + "/v1/", Provider: "openai"})
	if c.Protocol() != ProtocolOpenAI {
		t.Fatalf("expected openai protocol for /v1/ endpoint, got %q", c.Protocol())
	}
	if !c.Available() {
		t.Fatal("expected Available() true for trailing-slash endpoint")
	}
	vec, err := c.Embed("hello")
	if err != nil {
		t.Fatalf("Embed with trailing-slash endpoint: %v", err)
	}
	if len(vec) != 2 {
		t.Fatalf("expected 2 dims, got %d", len(vec))
	}
}

func TestOpenAIEmbed(t *testing.T) {
	var gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Errorf("expected /v1/embeddings, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"object":"list","data":[{"object":"embedding","embedding":[0.1,0.2,0.3],"index":0}]}`))
	}))
	defer srv.Close()

	c := newClientWithModel("", EmbedConfigFile{Endpoint: srv.URL + "/v1", Provider: "openai", APIKey: "sk-test", Model: "bge-m3-mlx-8bit"})
	vec, err := c.Embed("跨会话记忆测试")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 3 {
		t.Fatalf("expected 3 dims, got %d", len(vec))
	}
	if gotAuth != "Bearer sk-test" {
		t.Errorf("expected Bearer sk-test, got %q", gotAuth)
	}
	if gotBody["model"] != "bge-m3-mlx-8bit" {
		t.Errorf("expected model in body, got %v", gotBody["model"])
	}
	if input, _ := gotBody["input"].(string); input != "跨会话记忆测试" {
		t.Errorf("expected input text, got %v", gotBody["input"])
	}
}

func TestOpenAIEmbedWithoutKey(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":[{"embedding":[1.0]}]}`))
	}))
	defer srv.Close()

	c := newClientWithModel("", EmbedConfigFile{Endpoint: srv.URL + "/v1", Provider: "openai"})
	vec, err := c.Embed("hello")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 1 {
		t.Fatalf("expected 1 dim, got %d", len(vec))
	}
	if gotAuth != "" {
		t.Errorf("expected no Authorization header without API key, got %q", gotAuth)
	}
}

func TestOpenAIEmbedEmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	c := newClientWithModel("", EmbedConfigFile{Endpoint: srv.URL + "/v1", Provider: "openai"})
	if _, err := c.Embed("hello"); err == nil {
		t.Fatal("expected error for empty embedding response")
	}
}

// TestOpenAIProtocolDefaultsToSiliconFlow verifies this fork's behavior: when
// embed.yml sets provider: openai without an explicit endpoint, the client
// defaults to SiliconFlow so cloud embedding works out of the box.
func TestOpenAIProtocolDefaultsToSiliconFlow(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{Provider: "openai"})
	if c.endpoint != DefaultOpenAIEndpoint {
		t.Fatalf("expected default SiliconFlow endpoint %q, got %q", DefaultOpenAIEndpoint, c.endpoint)
	}
	if c.Protocol() != ProtocolOpenAI {
		t.Fatalf("expected openai protocol, got %q", c.Protocol())
	}
}

// TestOllamaProtocolKeepsLocalDefault confirms the default does not change
// Ollama behavior: with no provider and no endpoint, the local Ollama endpoint
// remains the default.
func TestOllamaProtocolKeepsLocalDefault(t *testing.T) {
	c := newClientWithModel("", EmbedConfigFile{})
	if c.endpoint != DefaultEndpoint {
		t.Fatalf("expected default Ollama endpoint %q, got %q", DefaultEndpoint, c.endpoint)
	}
	if c.Protocol() != ProtocolOllama {
		t.Fatalf("expected ollama protocol, got %q", c.Protocol())
	}
}
