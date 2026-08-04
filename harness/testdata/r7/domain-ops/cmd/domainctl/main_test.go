package main

import "testing"

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
