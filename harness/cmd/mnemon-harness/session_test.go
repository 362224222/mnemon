package main

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	sessionstore "github.com/mnemon-dev/mnemon/harness/internal/session"
)

func TestSessionStartUsesProductConfigDefaults(t *testing.T) {
	root := t.TempDir()
	cfg := productconfig.Default()
	cfg.Connections.Multica = productconfig.MulticaConnection{Enabled: true, Workspace: "ws-multica", RuntimeBinary: "mnemon-multica-runtime"}
	cfg.Sessions.PrimaryActivationCarrier = productconfig.ConnectionMultica
	if err := productconfig.Save(productconfig.DefaultPath(root, ""), cfg); err != nil {
		t.Fatal(err)
	}
	restoreSessionFlags(t)
	sessionRoot = root
	sessionStartID = "release-readiness"
	sessionStartTitle = "Release readiness"

	cmd, out := testCommand()
	if err := runSessionStart(cmd, nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"Session: release-readiness",
		"Primary activation carrier: multica",
		"Duplicate activation policy: suppress",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("session start missing %q:\n%s", want, got)
		}
	}
	record, err := sessionstore.NewFileStore(sessionstore.DefaultDir(root, ""), nil).Load("release-readiness")
	if err != nil {
		t.Fatal(err)
	}
	if record.Title != "Release readiness" || record.PrimaryActivationCarrier != productconfig.ConnectionMultica {
		t.Fatalf("session record = %+v", record)
	}
}

func TestSessionAttachRecordsExternalReference(t *testing.T) {
	root := t.TempDir()
	store := sessionstore.NewFileStore(sessionstore.DefaultDir(root, ""), nil)
	if _, err := store.Start(sessionstore.Record{ID: "release-readiness", PrimaryActivationCarrier: "local"}); err != nil {
		t.Fatal(err)
	}
	restoreSessionFlags(t)
	sessionRoot = root
	sessionAttachID = "release-readiness"
	sessionAttachSurface = productconfig.ConnectionMultica
	sessionAttachExternal = "issue/root-1"
	sessionAttachSetPrimary = true

	cmd, out := testCommand()
	if err := runSessionAttach(cmd, nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"Session: release-readiness",
		"Attachments: 1",
		"Primary activation carrier: multica",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("session attach missing %q:\n%s", want, got)
		}
	}
	record, err := store.Load("release-readiness")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Attachments) != 1 || record.Attachments[0].ExternalRef != "issue/root-1" || record.PrimaryActivationCarrier != productconfig.ConnectionMultica {
		t.Fatalf("attached session record = %+v", record)
	}
}

func TestSessionAttachRequiresID(t *testing.T) {
	restoreSessionFlags(t)
	sessionRoot = t.TempDir()
	sessionAttachSurface = productconfig.ConnectionMultica
	sessionAttachExternal = "issue/root-1"
	if err := runSessionAttach(mustTestCommand(t), nil); err == nil {
		t.Fatal("expected attach to require --id")
	}
}

func restoreSessionFlags(t *testing.T) {
	t.Helper()
	oldRoot := sessionRoot
	oldStartID := sessionStartID
	oldStartTitle := sessionStartTitle
	oldStartCarrier := sessionStartCarrier
	oldStartDupPolicy := sessionStartDupPolicy
	oldAttachID := sessionAttachID
	oldAttachSurface := sessionAttachSurface
	oldAttachExternal := sessionAttachExternal
	oldAttachPrimary := sessionAttachSetPrimary
	sessionRoot = "."
	sessionStartID = ""
	sessionStartTitle = ""
	sessionStartCarrier = ""
	sessionStartDupPolicy = ""
	sessionAttachID = ""
	sessionAttachSurface = ""
	sessionAttachExternal = ""
	sessionAttachSetPrimary = false
	t.Cleanup(func() {
		sessionRoot = oldRoot
		sessionStartID = oldStartID
		sessionStartTitle = oldStartTitle
		sessionStartCarrier = oldStartCarrier
		sessionStartDupPolicy = oldStartDupPolicy
		sessionAttachID = oldAttachID
		sessionAttachSurface = oldAttachSurface
		sessionAttachExternal = oldAttachExternal
		sessionAttachSetPrimary = oldAttachPrimary
	})
}
