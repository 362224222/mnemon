package presentation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

type Status string

const (
	StatusOK       Status = "ok"
	StatusFallback Status = "fallback"
	StatusEmpty    Status = "empty"
	StatusDenied   Status = "denied"
)

const (
	IntentSkillBootstrap  = "skill.bootstrap"
	IntentContextPacket   = "context.packet"
	IntentProfileEvents   = "profile.events"
	IntentTeamworkEvents  = "teamwork.events"
	IntentPayloadContract = "payload.contract"
)

type Request struct {
	SchemaVersion int
	Principal     contract.ActorID
	Host          string
	Lifecycle     string
	Surface       string
	RenderIntent  string
	SessionID     string
	InputDigest   string
	Budget        Budget
	Client        ClientInfo
}

type Budget struct {
	MaxChars      int
	EventViewTier contract.BudgetTier
}

type ClientInfo struct {
	ShimVersion string
	Supports    []string
}

type Response struct {
	SchemaVersion   int
	Status          Status
	Body            string
	BodyFormat      string
	BodyDigest      string
	EventViewDigest string
	Provenance      Provenance
	AuditID         string
	TTLSeconds      int
}

type Provenance struct {
	Source        string
	CatalogDigest string
	DecisionHead  string
	RenderedAt    string
}

type Renderer struct {
	Now       func() time.Time
	AuditSink AuditSink
}

func (r Renderer) RenderPresentation(ctx context.Context, req Request, proj eventview.EventView) (Response, error) {
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	if req.Principal == "" {
		return Response{SchemaVersion: 1, Status: StatusDenied, EventViewDigest: proj.Digest}, nil
	}
	body, events := BuildBodyAndEventEnvelopes(req, proj, now)
	if req.Budget.MaxChars > 0 && len(body) > req.Budget.MaxChars {
		body = body[:req.Budget.MaxChars]
	}
	resp := Response{
		SchemaVersion:   1,
		Status:          StatusOK,
		Body:            body,
		BodyFormat:      "plain_text",
		BodyDigest:      digest(body),
		EventViewDigest: proj.Digest,
		Provenance: Provenance{
			Source:        "local-mnemon",
			CatalogDigest: digest("render-intents:v1"),
			DecisionHead:  proj.Ref,
			RenderedAt:    now.Format(time.RFC3339),
		},
		TTLSeconds: 300,
	}
	if strings.TrimSpace(body) == "" {
		resp.Status = StatusEmpty
		resp.BodyDigest = ""
		return resp, nil
	}
	resp.AuditID = auditID(req, resp)
	if r.AuditSink != nil {
		if err := r.AuditSink.WriteRenderAudit(ctx, AuditRecordFrom(req, resp, presentationSectionCounts(body), eventEnvelopeCounts(events))); err != nil {
			return Response{}, err
		}
	}
	return resp, nil
}

func digest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func auditID(req Request, resp Response) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%s", req.Principal, req.Lifecycle, req.RenderIntent, resp.EventViewDigest, resp.BodyDigest)))
	return "render_" + hex.EncodeToString(sum[:8])
}
