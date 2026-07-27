package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var (
	ErrAgentSelectionInput                  = errors.New("invalid Agent offer selection input")
	ErrAgentSelectionChannelUnavailable     = errors.New("Agent offer Channel is unavailable")
	ErrAgentSelectionChannelAmbiguous       = errors.New("Agent offer Channel is ambiguous")
	ErrAgentSelectionParticipantUnavailable = errors.New("Agent offer participant is unavailable")
	ErrAgentSelectionInvariant              = errors.New("Agent offer selection invariant violated")
)

type AgentSelectionCandidatesError struct {
	kind       error
	candidates []string
}

func (e *AgentSelectionCandidatesError) Error() string {
	if e == nil {
		return "Agent offer selection is ambiguous"
	}
	return fmt.Sprintf("%v: candidates=%q", e.kind, e.candidates)
}

func (e *AgentSelectionCandidatesError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.kind
}

func (e *AgentSelectionCandidatesError) Candidates() []string {
	if e == nil {
		return nil
	}
	return append([]string(nil), e.candidates...)
}

type AgentOfferCandidateReader interface {
	ReadAgentOfferCandidates(context.Context, model.Profile, time.Time) (store.AgentOfferCandidates, error)
}

type AgentOfferSelectionSpec struct {
	Profile             model.Profile
	ChannelAlias        string
	ParticipantSelector string
	At                  time.Time
}

type AgentOfferReviewer struct {
	peerID         model.PeerID
	effectiveAlias string
	reachability   model.Reachability
}

func (r AgentOfferReviewer) PeerID() model.PeerID             { return r.peerID }
func (r AgentOfferReviewer) EffectiveAlias() string           { return r.effectiveAlias }
func (r AgentOfferReviewer) Reachability() model.Reachability { return r.reachability }
func (r AgentOfferReviewer) Reachable() bool {
	return r.reachability == model.ReachabilityReachable
}

type AgentOfferSelection struct {
	channelID    model.ChannelID
	channelAlias string
	rosterHead   model.RecordHead
	reviewer     AgentOfferReviewer
}

func (s AgentOfferSelection) ChannelID() model.ChannelID   { return s.channelID }
func (s AgentOfferSelection) ChannelAlias() string         { return s.channelAlias }
func (s AgentOfferSelection) RosterHead() model.RecordHead { return s.rosterHead }
func (s AgentOfferSelection) RosterRevision() uint64       { return s.rosterHead.Revision() }
func (s AgentOfferSelection) Reviewer() AgentOfferReviewer { return s.reviewer }

type OfferSelector struct {
	reader AgentOfferCandidateReader
}

func NewOfferSelector(reader AgentOfferCandidateReader) (*OfferSelector, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: candidate reader is required", ErrAgentSelectionInput)
	}
	return &OfferSelector{reader: reader}, nil
}

// Resolve reads one trusted Store snapshot and resolves one explicit participant
// alias without a second authority read.
func (s *OfferSelector) Resolve(ctx context.Context,
	spec AgentOfferSelectionSpec,
) (AgentOfferSelection, error) {
	if s == nil || s.reader == nil || ctx == nil || spec.Profile.ID().IsZero() || spec.At.IsZero() {
		return AgentOfferSelection{}, fmt.Errorf("%w: selector, context, Profile and trusted time are required",
			ErrAgentSelectionInput)
	}
	if err := validateAgentSelectionAlias("Channel", spec.ChannelAlias, true); err != nil {
		return AgentOfferSelection{}, err
	}
	if err := validateAgentSelectionAlias("participant", spec.ParticipantSelector, false); err != nil {
		return AgentOfferSelection{}, err
	}

	snapshot, err := s.reader.ReadAgentOfferCandidates(ctx, spec.Profile, spec.At)
	if err != nil {
		return AgentOfferSelection{}, err
	}
	channels, err := decodeAgentOfferCandidates(snapshot)
	if err != nil {
		return AgentOfferSelection{}, err
	}
	channel, reviewer, err := selectAgentOfferReviewer(
		channels, spec.ChannelAlias, spec.ParticipantSelector)
	if err != nil {
		return AgentOfferSelection{}, err
	}
	return AgentOfferSelection{channelID: channel.id, channelAlias: channel.alias,
		rosterHead: channel.rosterHead, reviewer: reviewer}, nil
}

type decodedAgentOfferReviewer struct {
	projection AgentOfferReviewer
	eligible   bool
}

type decodedAgentOfferChannel struct {
	id         model.ChannelID
	alias      string
	rosterHead model.RecordHead
	reviewers  []decodedAgentOfferReviewer
}

func decodeAgentOfferCandidates(snapshot store.AgentOfferCandidates) ([]decodedAgentOfferChannel, error) {
	channels := snapshot.Channels()
	if len(channels) > model.MaxChannelsPerNode {
		return nil, fmt.Errorf("%w: more than %d candidate Channels",
			ErrAgentSelectionInvariant, model.MaxChannelsPerNode)
	}
	result := make([]decodedAgentOfferChannel, len(channels))
	seenAliases := make(map[string]struct{}, len(channels))
	for index, channel := range channels {
		if channel.ChannelID().IsZero() || channel.RosterHead().IsZero() {
			return nil, fmt.Errorf("%w: candidate Channel identity is incomplete", ErrAgentSelectionInvariant)
		}
		if err := validateAgentSelectionAlias("candidate Channel", channel.LocalAlias(), false); err != nil {
			return nil, fmt.Errorf("%w: invalid candidate Channel alias", ErrAgentSelectionInvariant)
		}
		if _, exists := seenAliases[channel.LocalAlias()]; exists {
			return nil, fmt.Errorf("%w: duplicate candidate Channel alias", ErrAgentSelectionInvariant)
		}
		seenAliases[channel.LocalAlias()] = struct{}{}
		reviewers, err := decodeAgentOfferReviewers(channel.Reviewers())
		if err != nil {
			return nil, fmt.Errorf("Channel %q: %w", channel.LocalAlias(), err)
		}
		result[index] = decodedAgentOfferChannel{id: channel.ChannelID(), alias: channel.LocalAlias(),
			rosterHead: channel.RosterHead(), reviewers: reviewers}
	}
	return result, nil
}

func decodeAgentOfferReviewers(candidates []store.AgentOfferCandidateReviewer) ([]decodedAgentOfferReviewer, error) {
	if len(candidates) > model.MaxChildWorks {
		return nil, fmt.Errorf("%w: more than %d candidate reviewers",
			ErrAgentSelectionInvariant, model.MaxChildWorks)
	}
	result := make([]decodedAgentOfferReviewer, len(candidates))
	seenPeers := make(map[string]struct{}, len(candidates))
	seenAliases := make(map[string]struct{}, len(candidates))
	for index, candidate := range candidates {
		if candidate.PeerID().IsZero() || !candidate.Reachability().Valid() {
			return nil, fmt.Errorf("%w: candidate reviewer identity or reachability is invalid",
				ErrAgentSelectionInvariant)
		}
		alias := candidate.EffectiveAlias()
		if err := validateAgentCandidateAlias(alias); err != nil {
			return nil, fmt.Errorf("%w: candidate participant alias %q is invalid",
				ErrAgentSelectionInvariant, alias)
		}
		if _, exists := seenAliases[alias]; exists {
			return nil, fmt.Errorf("%w: duplicate participant alias %q", ErrAgentSelectionInvariant, alias)
		}
		seenAliases[alias] = struct{}{}
		canonicalPeer, err := canonicalAgentPeerBytes(candidate.PeerID())
		if err != nil {
			return nil, fmt.Errorf("%w: PeerID %q: %v", ErrAgentSelectionInvariant,
				candidate.PeerID().String(), err)
		}
		key := string(canonicalPeer)
		if _, exists := seenPeers[key]; exists {
			return nil, fmt.Errorf("%w: duplicate canonical PeerID", ErrAgentSelectionInvariant)
		}
		seenPeers[key] = struct{}{}
		result[index] = decodedAgentOfferReviewer{
			projection: AgentOfferReviewer{peerID: candidate.PeerID(), effectiveAlias: alias,
				reachability: candidate.Reachability()},
			eligible: candidate.Eligible(),
		}
	}
	return result, nil
}

func canonicalAgentPeerBytes(id model.PeerID) ([]byte, error) {
	return model.CanonicalPeerIDBytes(id)
}

func validateAgentSelectionAlias(field, value string, emptyOK bool) error {
	if value == "" && emptyOK {
		return nil
	}
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxLabelBytes ||
		strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: %s alias is empty, oversized, or non-canonical",
			ErrAgentSelectionInput, field)
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return fmt.Errorf("%w: %s alias contains whitespace or a control character",
				ErrAgentSelectionInput, field)
		}
	}
	return nil
}

func validateAgentCandidateAlias(value string) error {
	return validateAgentSelectionAlias("candidate participant", value, false)
}

func selectAgentOfferReviewer(channels []decodedAgentOfferChannel,
	channelAlias, participantAlias string,
) (decodedAgentOfferChannel, AgentOfferReviewer, error) {
	if channelAlias != "" {
		return selectAgentOfferReviewerInChannel(channels, channelAlias, participantAlias)
	}
	return selectAgentOfferReviewerAcrossChannels(channels, participantAlias)
}

func selectAgentOfferReviewerInChannel(channels []decodedAgentOfferChannel,
	channelAlias, participantAlias string,
) (decodedAgentOfferChannel, AgentOfferReviewer, error) {
	for _, channel := range channels {
		if channel.alias != channelAlias {
			continue
		}
		reviewer, ok := eligibleAgentOfferReviewer(channel.reviewers, participantAlias)
		if ok {
			return channel, reviewer, nil
		}
		return decodedAgentOfferChannel{}, AgentOfferReviewer{},
			ErrAgentSelectionParticipantUnavailable
	}
	return decodedAgentOfferChannel{}, AgentOfferReviewer{},
		ErrAgentSelectionChannelUnavailable
}

type agentOfferReviewerMatch struct {
	channel  decodedAgentOfferChannel
	reviewer AgentOfferReviewer
}

func selectAgentOfferReviewerAcrossChannels(channels []decodedAgentOfferChannel,
	participantAlias string,
) (decodedAgentOfferChannel, AgentOfferReviewer, error) {
	matches := make([]agentOfferReviewerMatch, 0, len(channels))
	for _, channel := range channels {
		reviewer, ok := eligibleAgentOfferReviewer(channel.reviewers, participantAlias)
		if ok {
			matches = append(matches, agentOfferReviewerMatch{channel: channel, reviewer: reviewer})
		}
	}
	switch len(matches) {
	case 0:
		return decodedAgentOfferChannel{}, AgentOfferReviewer{},
			ErrAgentSelectionParticipantUnavailable
	case 1:
		return matches[0].channel, matches[0].reviewer, nil
	default:
		candidates := make([]string, len(matches))
		for index := range matches {
			candidates[index] = matches[index].channel.alias
		}
		sort.Strings(candidates)
		return decodedAgentOfferChannel{}, AgentOfferReviewer{},
			newAgentSelectionCandidatesError(
				ErrAgentSelectionChannelAmbiguous, candidates, model.MaxChannelsPerNode)
	}
}

func eligibleAgentOfferReviewer(reviewers []decodedAgentOfferReviewer,
	participantAlias string,
) (AgentOfferReviewer, bool) {
	for _, reviewer := range reviewers {
		if reviewer.eligible && reviewer.projection.EffectiveAlias() == participantAlias {
			return reviewer.projection, true
		}
	}
	return AgentOfferReviewer{}, false
}

func newAgentSelectionCandidatesError(kind error, candidates []string, limit int) error {
	if len(candidates) == 0 || len(candidates) > limit {
		return fmt.Errorf("%w: ambiguity candidate count is out of range", ErrAgentSelectionInvariant)
	}
	bounded := append([]string(nil), candidates...)
	for _, candidate := range bounded {
		if err := validateAgentSelectionAlias("ambiguity candidate", candidate, false); err != nil {
			return fmt.Errorf("%w: invalid ambiguity candidate", ErrAgentSelectionInvariant)
		}
	}
	return &AgentSelectionCandidatesError{kind: kind, candidates: bounded}
}
