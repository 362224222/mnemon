package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveAllowsGatewayHistoryWithoutBroadeningControlSurface(t *testing.T) {
	t.Parallel()

	target, err := resolve("http://gateway:8080", "/history?prefix=incident-")
	if err != nil {
		t.Fatalf("resolve gateway history: %v", err)
	}
	if target != "http://gateway:8080/history?prefix=incident-" {
		t.Fatalf("history target = %q", target)
	}

	if _, err := resolve("http://gateway:8080", "/requests"); err == nil {
		t.Fatal("unreviewed read surface was accepted")
	}
}

func TestProbeHasNoCallerControlledShape(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method != http.MethodPost || request.URL.Path != "/probe" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}
		if string(body) != "{}" {
			t.Errorf("probe body = %q", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]bool{"observed": true})
	}))
	t.Cleanup(server.Close)

	result, err := execute(context.Background(), configuration{
		role: "lead", endpoint: server.URL, args: []string{"probe"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "{\"observed\":true}\n" {
		t.Fatalf("probe result = %q", result)
	}
}
