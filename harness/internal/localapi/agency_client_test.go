package localapi

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestAgencyClientRoundTripsWithoutR5ProfileState(t *testing.T) {
	fixture := newAgencyHTTPFixture(t)
	content := []byte("client Artifact")
	fixture.service.capture = node.AgencyArtifactCapture{Handle: "artifact:client",
		Digest: node.AgencyContentDigest(content), ByteSize: int64(len(content))}
	server := newTestAgencyServer(t, fixture.service)
	nodeState := newClientNodeState(t)
	stop := serveRawClientControl(t, nodeState, server)
	defer stop()

	client, err := NewAgencyClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(nodeState, "profiles")); !os.IsNotExist(err) {
		t.Fatalf("Agency client unexpectedly required R5 profiles: %v", err)
	}
	attachment, apiErr := client.Attach(context.Background())
	if apiErr != nil || attachment.ID != fixture.attachment.ID ||
		!bytes.Equal(attachment.Credential, fixture.attachment.Credential) {
		t.Fatalf("Attach() = (%#v, %#v)", attachment, apiErr)
	}
	view, apiErr := client.Current(context.Background(), attachment, fixture.current)
	if apiErr != nil || !bytes.Equal(view, fixture.view.CanonicalJSON()) {
		t.Fatalf("Current() = (%s, %#v)", view, apiErr)
	}
	receipt, apiErr := client.Submit(context.Background(), attachment, fixture.current,
		fixture.operation, fixture.intent.CanonicalJSON(), nil)
	if apiErr != nil || !bytes.Equal(receipt, fixture.receipt.CanonicalJSON()) {
		t.Fatalf("Submit() = (%s, %#v)", receipt, apiErr)
	}
	capture, apiErr := client.Capture(context.Background(), content)
	if apiErr != nil || capture.Handle != fixture.service.capture.Handle ||
		capture.Digest != node.AgencyContentDigest(content) || capture.ByteSize != int64(len(content)) {
		t.Fatalf("Capture() = (%#v, %#v)", capture, apiErr)
	}
	status, apiErr := client.Status(context.Background())
	if apiErr != nil || !status.Ready {
		t.Fatalf("Status() = (%#v, %#v)", status, apiErr)
	}
}

func TestAgencyClientRejectsNoncanonicalOrWrongSchemaProjection(t *testing.T) {
	fixture := newAgencyHTTPFixture(t)
	tests := map[string]string{
		"whitespace":   `{ "schema":"mnemon.agent.view","version":1}`,
		"wrong schema": `{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":false}`,
		"duplicate":    `{"schema":"mnemon.agent.view","schema":"mnemon.agent.view","version":1}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			nodeState := newClientNodeState(t)
			stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
				request *http.Request,
			) {
				if request.URL.Path != RouteAgencyCurrent {
					t.Fatalf("route = %s", request.URL.Path)
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.Header().Set("Cache-Control", "no-store")
				_, _ = writer.Write([]byte(body + "\n"))
			}))
			defer stop()
			client, err := NewAgencyClient(nodeState)
			if err != nil {
				t.Fatal(err)
			}
			if _, apiErr := client.Current(context.Background(), fixture.attachment,
				fixture.current); apiErr == nil || apiErr.Code != CodeInternal {
				t.Fatalf("Current() error = %#v", apiErr)
			}
		})
	}
}

func TestAgencyClientKeepsArtifactAndRequestBoundsLocal(t *testing.T) {
	nodeState := newClientNodeState(t)
	client, err := NewAgencyClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := client.Capture(context.Background(), make([]byte, node.MaxAgencyArtifactBytes+1)); apiErr == nil || apiErr.Code != CodeArtifactTooLarge {
		t.Fatalf("oversized Capture() error = %#v", apiErr)
	}
	zero := node.AgencyAttachment{}
	if _, apiErr := client.Current(context.Background(), zero,
		"operation:client-current"); apiErr == nil ||
		apiErr.Code != CodeAuthenticationFailed || strings.Contains(apiErr.Message, "operation:client-current") {
		t.Fatalf("authority-free Current() error = %#v", apiErr)
	}
}
