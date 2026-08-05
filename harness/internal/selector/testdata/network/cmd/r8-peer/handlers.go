package main

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

func (service *peerService) networkMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(networkPath, service.handleSample)
	return mux
}

func (service *peerService) controlMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(controlReadyPath, service.handleReady)
	mux.HandleFunc(controlStatusPath, service.handleStatus)
	mux.HandleFunc(controlRoundPath, service.handleRound)
	return mux
}

func (service *peerService) handleSample(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != networkPath {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	raw, err := readBounded(request.Body, maxFrameBytes)
	if err != nil {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	kind, source, payload, err := verifyFrame(raw, service.config)
	if err != nil || kind != kindQuery {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	query, err := selector.ParseSampleQueryCanonical(payload)
	if err != nil {
		http.Error(writer, "invalid query", http.StatusBadRequest)
		return
	}
	fresh, err := service.attempts.claim("in:"+source.String(), sampleQueryKey(query))
	if err != nil {
		http.Error(writer, "query budget exhausted", http.StatusTooManyRequests)
		return
	}
	if !fresh {
		service.writeSampleReply(writer, selector.SampleResponse{})
		return
	}
	response, err := service.store.RespondSampleQuery(request.Context(), source, query)
	if err != nil {
		http.Error(writer, "query unavailable", http.StatusServiceUnavailable)
		return
	}
	service.writeSampleReply(writer, response)
}

func (service *peerService) writeSampleReply(writer http.ResponseWriter,
	response selector.SampleResponse,
) {
	responseKind, responsePayload := kindNoVote, []byte(nil)
	if vote, present := response.Vote(); present {
		responseKind = kindVote
		var err error
		responsePayload, err = vote.CanonicalBytes()
		if err != nil {
			http.Error(writer, "query unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	encoded, err := signFrame(responseKind, service.self.id, responsePayload, service.private)
	if err != nil {
		http.Error(writer, "query unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (service *peerService) handleReady(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != controlReadyPath {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"ready":true}`))
}

func (service *peerService) handleStatus(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet || request.URL.Path != controlStatusPath {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	snapshot, err := service.store.Selection(request.Context(), service.config.descriptor.ID())
	if err != nil {
		http.Error(writer, "status unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := writeJSON(writer, projectSnapshot(snapshot)); err != nil {
		http.Error(writer, "status unavailable", http.StatusServiceUnavailable)
	}
}

func (service *peerService) handleRound(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != controlRoundPath {
		http.Error(writer, "not found", http.StatusNotFound)
		return
	}
	execution, err := service.executeRound(request.Context())
	if err != nil {
		http.Error(writer, "round failed: "+boundedError(err), http.StatusConflict)
		return
	}
	output, err := projectRoundExecution(execution)
	if err != nil {
		http.Error(writer, "round result unavailable", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := writeJSON(writer, output); err != nil {
		http.Error(writer, "round result unavailable", http.StatusServiceUnavailable)
	}
}

func boundedError(err error) string {
	if err == nil {
		return "unknown"
	}
	value := err.Error()
	if len(value) > 160 {
		return value[:160]
	}
	return value
}

type roundExecution struct {
	before  selector.SelectionSnapshot
	pending selector.PendingRound
	votes   []selector.AuthenticatedVote
	after   selector.SelectionSnapshot
}

func (service *peerService) executeRound(ctx context.Context) (roundExecution, error) {
	pending, err := service.store.FreezeRound(ctx, service.config.descriptor.ID())
	if err != nil {
		return roundExecution{}, err
	}
	before, err := service.store.Selection(ctx, service.config.descriptor.ID())
	if err != nil {
		return roundExecution{}, err
	}
	stored, present := before.PendingRound()
	if !present || stored.Query().SelectionID() != pending.Query().SelectionID() ||
		stored.Query().Round() != pending.Query().Round() ||
		stored.Query().Nonce() != pending.Query().Nonce() {
		return roundExecution{}, errors.New("frozen round changed before network sampling")
	}
	roundContext, cancel := context.WithDeadline(ctx, pending.Deadline())
	defer cancel()
	votes := querySample(roundContext, pending.Sample(), pending.Query(), service.queryPeer)
	after, err := service.store.ApplyObservations(ctx, pending, votes)
	if err != nil {
		return roundExecution{}, err
	}
	return roundExecution{before: before, pending: pending, votes: votes, after: after}, nil
}

type sampleQueryFunc func(context.Context, selector.ParticipantID,
	selector.SampleQuery) (selector.AuthenticatedVote, bool, error)

// querySample gives every member of the frozen sample the same round deadline.
// query must observe ctx; all bounded workers are joined before this returns.
func querySample(ctx context.Context, sample []selector.ParticipantID, query selector.SampleQuery,
	queryPeer sampleQueryFunc,
) []selector.AuthenticatedVote {
	type result struct {
		vote    selector.AuthenticatedVote
		present bool
	}
	results := make([]result, len(sample))
	var workers sync.WaitGroup
	workers.Add(len(sample))
	for index, sampled := range sample {
		go func() {
			defer workers.Done()
			vote, present, err := queryPeer(ctx, sampled, query)
			if err == nil && present {
				results[index] = result{vote: vote, present: true}
			}
		}()
	}
	workers.Wait()
	votes := make([]selector.AuthenticatedVote, 0, len(results))
	for _, result := range results {
		if result.present {
			votes = append(votes, result.vote)
		}
	}
	return votes
}
