package localapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestServerRejectsUnknownTeamworkActionBeforeManagedService(t *testing.T) {
	t.Parallel()
	credential := make([]byte, opaqueSecretBytes)
	operation := make([]byte, opaqueSecretBytes)
	service := &fakeService{}
	server, err := NewServer(&fakeAuthenticator{want: modelDigest(credential)}, service)
	if err != nil {
		t.Fatal(err)
	}
	request := authenticatedRequest(t, http.MethodPost, RouteTeamworkAction,
		`{"action":"future-action"}`, credential)
	request.Header.Set(operationKeyHeader, encodeSecret(operation))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || service.called != "" ||
		!strings.Contains(recorder.Body.String(), `"code":"unknown_action"`) {
		t.Fatalf("semantic rejection = %d %s service=%#v", recorder.Code,
			recorder.Body.String(), service)
	}
}

func TestClientDoesNotOwnTeamworkActionCatalogOrReceiptPolicy(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x92)
	installClientCredential(t, nodeState, credential)
	var requestBody string
	response := OperationResponse{SchemaVersion: SchemaVersion, Status: "accepted",
		Action: "teamwork.future-action", OperationID: "operation-client-future",
		Results: []OperationResult{
			{EventID: "event-client-future-one", EventType: "review.offered",
				Work: WorkReceipt{Ref: "peer/work-one", Version: 1, State: "OFFERED"}},
			{EventID: "event-client-future-two", EventType: "review.offered",
				Work: WorkReceipt{Ref: "peer/work-two", Version: 1, State: "OFFERED"}},
		}, Receipt: "receipt-future"}
	encoded, err := model.CanonicalMarshal(response)
	if err != nil {
		t.Fatal(err)
	}
	stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
		request *http.Request,
	) {
		raw, _ := io.ReadAll(request.Body)
		requestBody = string(raw)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(append(encoded, '\n'))
	}))
	defer stop()
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-client-future-action")
	contextFile, err := WriteContextFile(nodeState, runID, testOpaqueValue(0x93))
	if err != nil {
		t.Fatal(err)
	}
	action := TeamworkActionRequest{Action: "future-action", Content: "opaque"}
	actionRaw, err := model.CanonicalMarshal(action)
	if err != nil {
		t.Fatal(err)
	}
	journalStore, err := NewPendingJournalStore(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	journal, _, err := journalStore.FindOrCreate(model.Sum(actionRaw),
		digestPointer(contextFile.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	got, apiErr := client.TeamworkAction(context.Background(), action, &contextFile, journal)
	if apiErr != nil || got.Action != response.Action || got.Handling != nil || len(got.Results) != 2 ||
		!strings.Contains(requestBody, `"action":"future-action"`) {
		t.Fatalf("opaque Action transport = (%#v, %#v, %q)", got, apiErr, requestBody)
	}
}
