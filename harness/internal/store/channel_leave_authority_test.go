package store

import (
	"bytes"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func assertChannelLeaveRow(t *testing.T, st *Store, request model.SignedChannelLeaveRequest,
	wantStatus string, wantAttempts uint64, wantReceipt []byte,
) {
	t.Helper()
	var status string
	var attempts uint64
	var requestJSON, signature, receipt []byte
	if err := st.db.QueryRow(`SELECT status,attempts,request_json,member_signature,receipt_json
		FROM channel_leave_requests WHERE request_id=?`, request.RequestID().String()).Scan(
		&status, &attempts, &requestJSON, &signature, &receipt); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attempts != wantAttempts ||
		!bytes.Equal(requestJSON, request.Record().CanonicalJSON().Bytes()) ||
		!bytes.Equal(signature, request.MemberSignature()) || !bytes.Equal(receipt, wantReceipt) {
		t.Fatalf("leave row = status %q attempts %d request=%d signature=%d receipt=%d",
			status, attempts, len(requestJSON), len(signature), len(receipt))
	}
}

func assertChannelLeaveProjection(t *testing.T, st *Store, channelID model.ChannelID,
	wantStatus string, wantRequests int,
) {
	t.Helper()
	var status string
	var requests int
	if err := st.db.QueryRow(`SELECT status FROM channels WHERE channel_id=?`,
		channelID.String()).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_leave_requests WHERE channel_id=?`,
		channelID.String()).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || requests != wantRequests {
		t.Fatalf("leave projection = status %q requests %d, want %q/%d",
			status, requests, wantStatus, wantRequests)
	}
}
