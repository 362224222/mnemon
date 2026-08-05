package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

func (service *peerService) queryPeer(ctx context.Context, target selector.ParticipantID,
	query selector.SampleQuery,
) (selector.AuthenticatedVote, bool, error) {
	fresh, err := service.attempts.claim("out:"+target.String(), sampleQueryKey(query))
	if err != nil {
		return selector.AuthenticatedVote{}, false, err
	}
	if !fresh {
		return selector.AuthenticatedVote{}, false, nil
	}
	payload, err := query.CanonicalBytes()
	if err != nil {
		return selector.AuthenticatedVote{}, false, err
	}
	frame, err := signFrame(kindQuery, service.self.id, payload, service.private)
	if err != nil {
		return selector.AuthenticatedVote{}, false, err
	}
	status, response, err := postFrame(ctx, service.config, target, frame)
	if err != nil || status != http.StatusOK {
		return selector.AuthenticatedVote{}, false, errors.New("sample peer did not return an authenticated response")
	}
	kind, source, reply, err := verifyFrame(response, service.config)
	if err != nil || source != target {
		return selector.AuthenticatedVote{}, false, errors.New("sample response identity mismatch")
	}
	if kind == kindNoVote {
		return selector.AuthenticatedVote{}, false, nil
	}
	if kind != kindVote {
		return selector.AuthenticatedVote{}, false, errors.New("sample response kind is invalid")
	}
	vote, err := selector.ParseSampleVoteCanonical(reply)
	if err != nil {
		return selector.AuthenticatedVote{}, false, err
	}
	authenticated, err := selector.AuthenticateSampleVote(source, vote)
	if err != nil {
		return selector.AuthenticatedVote{}, false, err
	}
	return authenticated, true, nil
}

func sampleQueryKey(query selector.SampleQuery) string {
	return fmt.Sprintf("%s/%d/%s", query.SelectionID(), query.Round(), query.Nonce())
}

func postFrame(ctx context.Context, config runtimeConfig, target selector.ParticipantID,
	frame []byte,
) (int, []byte, error) {
	peer, err := config.peer(target.String())
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+peer.address+networkPath, bytes.NewReader(frame))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: requestBudget, Transport: &http.Transport{
		DialContext:       (&net.Dialer{Timeout: requestBudget, KeepAlive: -1}).DialContext,
		DisableKeepAlives: true, MaxConnsPerHost: 1,
		ResponseHeaderTimeout: requestBudget,
	}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := readBounded(response.Body, maxFrameBytes)
	if err != nil {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, body, nil
}

type probeOutput struct {
	Mode          string `json:"mode"`
	HTTPStatus    int    `json:"http_status"`
	Authenticated bool   `json:"authenticated"`
	NoVote        bool   `json:"no_vote"`
}

func runProbe(ctx context.Context, args []string) error {
	options, target, mode, err := parseProbeOptions(args)
	if err != nil {
		return err
	}
	config, err := loadConfig(options.config)
	if err != nil {
		return err
	}
	self, err := requireSelfIdentity(config, options.self, options.stateDir)
	if err != nil {
		return err
	}
	targetPeer, err := config.peer(target)
	if err != nil {
		return err
	}
	private, err := loadPrivateKey(options.stateDir)
	if err != nil {
		return err
	}
	query, err := unknownSampleQuery()
	if err != nil {
		return err
	}
	payload, _ := query.CanonicalBytes()
	claim := self.id
	if mode == "identity-mismatch" {
		claim = targetPeer.id
	}
	frame, err := signFrame(kindQuery, claim, payload, private)
	if err != nil {
		return err
	}
	status, raw, requestErr := postFrame(ctx, config, targetPeer.id, frame)
	if mode == "identity-mismatch" {
		if requestErr == nil && status == http.StatusUnauthorized {
			return writeJSON(os.Stdout, probeOutput{Mode: mode, HTTPStatus: status})
		}
		return errors.New("forged claimed source was not rejected")
	}
	if requestErr != nil || status != http.StatusOK {
		return fmt.Errorf("no-vote probe failed: %w", requestErr)
	}
	kind, source, responsePayload, err := verifyFrame(raw, config)
	if err != nil || source != targetPeer.id || kind != kindNoVote || len(responsePayload) != 0 {
		return errors.New("unknown selection did not return an authenticated no-vote")
	}
	return writeJSON(os.Stdout, probeOutput{Mode: mode, HTTPStatus: status,
		Authenticated: true, NoVote: true})
}

func unknownSampleQuery() (selector.SampleQuery, error) {
	id, err := selector.ParseSelectionID(agency.Sum([]byte("r8-network-unknown-selection")).String())
	if err != nil {
		return selector.SampleQuery{}, err
	}
	return selector.NewSampleQuery(id, 1, agency.Sum([]byte("r8-network-probe-nonce")))
}
