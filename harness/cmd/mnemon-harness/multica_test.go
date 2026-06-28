package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func TestMulticaImportIssueWritesObservedDraftToLocalIngest(t *testing.T) {
	restoreMulticaFlags(t)

	issuePath := filepath.Join(t.TempDir(), "issue.json")
	if err := os.WriteFile(issuePath, []byte(`{
		"id": "iss-7",
		"identifier": "MUL-7",
		"title": "Coordinate adapter validation",
		"description": "Validate that a Multica issue can be handed to local teamwork without leaking rule ids into narrative."
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var got contract.ObservationEnvelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.Header.Get("X-Mnemon-Principal") != "codex@project" {
			t.Fatalf("principal header = %q", r.Header.Get("X-Mnemon-Principal"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"seq": 7, "dup": false, "ticked": true})
	}))
	defer srv.Close()

	multicaIssueJSON = issuePath
	multicaScope = "multica/poc"
	multicaTTL = "45m"
	multicaWhyTeamwork = "Adapter validation needs multiple local agents."
	multicaContextRefs = []string{"multica:workspace:test"}
	multicaEvidenceRefs = []string{"multica:issue:iss-7"}
	multicaLocalAddr = srv.URL
	multicaLocalPrincipal = "codex@project"
	multicaJSON = true

	var out bytes.Buffer
	multicaImportIssueCmd.SetOut(&out)
	t.Cleanup(func() {
		multicaImportIssueCmd.SetOut(os.Stdout)
	})
	if err := runMulticaImportIssue(multicaImportIssueCmd, nil); err != nil {
		t.Fatalf("import issue: %v", err)
	}

	if got.ExternalID != "multica-issue-iss-7" {
		t.Fatalf("external id = %q", got.ExternalID)
	}
	if got.Event.Type != "teamwork_signal.write_candidate.observed" {
		t.Fatalf("event type = %q", got.Event.Type)
	}
	rule, _ := got.Event.Payload["rule"].(map[string]any)
	if rule["external_source"] != "multica" || rule["external_issue_id"] != "iss-7" || rule["ttl"] != "45m" {
		t.Fatalf("rule payload mismatch: %+v", rule)
	}
	narrative, _ := got.Event.Payload["narrative"].(map[string]any)
	if narrative["title"] != "Coordinate adapter validation" {
		t.Fatalf("narrative title mismatch: %+v", narrative)
	}
	if _, ok := narrative["external_issue_id"]; ok {
		t.Fatalf("narrative must not carry rule ids: %+v", narrative)
	}
	refs, _ := got.Event.Payload["refs"].(map[string]any)
	if refs == nil || len(refs) == 0 {
		t.Fatalf("refs payload missing: %+v", got.Event.Payload)
	}
}

func restoreMulticaFlags(t *testing.T) {
	t.Helper()
	oldBin := multicaBin
	oldProfile := multicaProfile
	oldServerURL := multicaServerURL
	oldWorkspaceID := multicaWorkspaceID
	oldTimeout := multicaTimeout
	oldJSON := multicaJSON
	oldIssueID := multicaIssueID
	oldIssueJSON := multicaIssueJSON
	oldScope := multicaScope
	oldTTL := multicaTTL
	oldWhy := multicaWhyTeamwork
	oldEvidence := multicaEvidenceRefs
	oldContext := multicaContextRefs
	oldDryRun := multicaDryRun
	oldAddr := multicaLocalAddr
	oldPrincipal := multicaLocalPrincipal
	oldToken := multicaLocalToken
	oldTokenFile := multicaLocalTokenFile
	oldContent := multicaCommentContent
	oldFile := multicaCommentFile
	oldStdin := multicaCommentStdin
	oldTitle := multicaCommentTitle
	oldEvents := multicaCommentEvents
	t.Cleanup(func() {
		multicaBin = oldBin
		multicaProfile = oldProfile
		multicaServerURL = oldServerURL
		multicaWorkspaceID = oldWorkspaceID
		multicaTimeout = oldTimeout
		multicaJSON = oldJSON
		multicaIssueID = oldIssueID
		multicaIssueJSON = oldIssueJSON
		multicaScope = oldScope
		multicaTTL = oldTTL
		multicaWhyTeamwork = oldWhy
		multicaEvidenceRefs = oldEvidence
		multicaContextRefs = oldContext
		multicaDryRun = oldDryRun
		multicaLocalAddr = oldAddr
		multicaLocalPrincipal = oldPrincipal
		multicaLocalToken = oldToken
		multicaLocalTokenFile = oldTokenFile
		multicaCommentContent = oldContent
		multicaCommentFile = oldFile
		multicaCommentStdin = oldStdin
		multicaCommentTitle = oldTitle
		multicaCommentEvents = oldEvents
	})
	multicaBin = ""
	multicaProfile = ""
	multicaServerURL = ""
	multicaWorkspaceID = ""
	multicaJSON = false
	multicaIssueID = ""
	multicaIssueJSON = ""
	multicaScope = "multica/teamwork"
	multicaTTL = "30m"
	multicaWhyTeamwork = ""
	multicaEvidenceRefs = nil
	multicaContextRefs = nil
	multicaDryRun = false
	multicaLocalAddr = "http://127.0.0.1:8787"
	multicaLocalPrincipal = ""
	multicaLocalToken = ""
	multicaLocalTokenFile = ""
	multicaCommentContent = ""
	multicaCommentFile = ""
	multicaCommentStdin = false
	multicaCommentTitle = ""
	multicaCommentEvents = nil
}
