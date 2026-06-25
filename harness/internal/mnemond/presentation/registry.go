package presentation

import (
	"time"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

// PresentationBody is the presenter-owned body plus any derived event envelopes that explain it.
// Renderer keeps the response envelope, budget, digest, provenance, and audit responsibilities.
type PresentationBody struct {
	Body   string
	Events []eventmodel.EventEnvelope
}

// Presenter owns one render intent. Domain-specific dataflow derivation belongs in presenters,
// not in the Renderer or presentation routing core.
type Presenter interface {
	Intent() string
	Present(Request, view.View, time.Time) (PresentationBody, error)
}

type PresenterRegistry struct {
	byIntent map[string]Presenter
}

func NewPresenterRegistry(presenters ...Presenter) PresenterRegistry {
	r := PresenterRegistry{byIntent: map[string]Presenter{}}
	for _, presenter := range presenters {
		if presenter == nil || presenter.Intent() == "" {
			continue
		}
		r.byIntent[presenter.Intent()] = presenter
	}
	return r
}

func DefaultPresenterRegistry() PresenterRegistry {
	return NewPresenterRegistry(
		teamworkEventsPresenter{},
		profileEventsPresenter{},
		contextPacketPresenter{},
		payloadContractPresenter{},
		skillBootstrapPresenter{},
	)
}

func (r PresenterRegistry) Present(req Request, proj view.View, now time.Time) (PresentationBody, error) {
	presenter, ok := r.byIntent[req.RenderIntent]
	if !ok {
		return PresentationBody{}, nil
	}
	return presenter.Present(req, proj, now)
}

func BuildPresentationBody(req Request, proj view.View, now time.Time) (PresentationBody, error) {
	return DefaultPresenterRegistry().Present(req, proj, now)
}

func BuildBodyAndEventEnvelopes(req Request, proj view.View, now time.Time) (string, []eventmodel.EventEnvelope) {
	body, err := BuildPresentationBody(req, proj, now)
	if err != nil {
		return "", nil
	}
	return body.Body, body.Events
}
