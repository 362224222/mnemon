package render

import "time"

func MinimalFallback(req Request, now time.Time) Response {
	body := "mnemon is temporarily unavailable; continue only with local context, or retry mnemon status."
	return Response{
		SchemaVersion: 1,
		Status:        StatusFallback,
		Body:          body,
		BodyFormat:    "plain_text",
		BodyDigest:    digest(body),
		Provenance: Provenance{
			Source:     "embedded-fallback",
			RenderedAt: now.UTC().Format(time.RFC3339),
		},
		TTLSeconds: 60,
	}
}
