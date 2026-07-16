package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	AgentParticipantAuto = "auto"
	AgentParticipantTeam = "team"
)

var (
	ErrAgentSelectionInput                  = errors.New("invalid Agent offer selection input")
	ErrAgentSelectionChannelUnavailable     = errors.New("Agent offer Channel is unavailable")
	ErrAgentSelectionChannelAmbiguous       = errors.New("Agent offer Channel is ambiguous")
	ErrAgentSelectionParticipantUnavailable = errors.New("Agent offer participant is unavailable")
	ErrAgentSelectionParticipantAmbiguous   = errors.New("Agent offer participant is ambiguous")
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
	reviewers    []AgentOfferReviewer
}

func (s AgentOfferSelection) ChannelID() model.ChannelID   { return s.channelID }
func (s AgentOfferSelection) ChannelAlias() string         { return s.channelAlias }
func (s AgentOfferSelection) RosterHead() model.RecordHead { return s.rosterHead }
func (s AgentOfferSelection) RosterRevision() uint64       { return s.rosterHead.Revision() }
func (s AgentOfferSelection) Reviewers() []AgentOfferReviewer {
	return append([]AgentOfferReviewer(nil), s.reviewers...)
}

type OfferSelector struct {
	reader AgentOfferCandidateReader
}

func NewOfferSelector(reader AgentOfferCandidateReader) (*OfferSelector, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: candidate reader is required", ErrAgentSelectionInput)
	}
	return &OfferSelector{reader: reader}, nil
}

// Resolve reads one trusted Store snapshot, decodes every PeerID, and applies
// the closed explicit/auto/team selector rules without a second authority read.
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
	if err := validateAgentSelectionAlias("participant", spec.ParticipantSelector, true); err != nil {
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
	channel, err := selectAgentOfferChannel(channels, spec.ChannelAlias)
	if err != nil {
		return AgentOfferSelection{}, err
	}
	reviewers, err := selectAgentOfferReviewers(channel.reviewers, spec.ParticipantSelector)
	if err != nil {
		return AgentOfferSelection{}, err
	}
	return AgentOfferSelection{channelID: channel.id, channelAlias: channel.alias,
		rosterHead: channel.rosterHead, reviewers: reviewers}, nil
}

type decodedAgentOfferReviewer struct {
	projection    AgentOfferReviewer
	canonicalPeer []byte
	eligible      bool
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
			return nil, fmt.Errorf("%w: candidate participant alias %q is invalid or reserved",
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
			canonicalPeer: canonicalPeer, eligible: candidate.Eligible(),
		}
	}
	sortAgentOfferReviewers(result)
	return result, nil
}

func sortAgentOfferReviewers(reviewers []decodedAgentOfferReviewer) {
	sort.Slice(reviewers, func(left, right int) bool {
		return bytes.Compare(reviewers[left].canonicalPeer, reviewers[right].canonicalPeer) < 0
	})
}

func canonicalAgentPeerBytes(id model.PeerID) ([]byte, error) {
	decoded, err := libp2ppeer.Decode(id.String())
	if err != nil {
		return nil, err
	}
	if decoded.String() != id.String() {
		return nil, errors.New("PeerID text is not canonical")
	}
	return append([]byte(nil), []byte(decoded)...), nil
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
	if err := validateAgentSelectionAlias("candidate participant", value, false); err != nil {
		return err
	}
	if value == AgentParticipantAuto || value == AgentParticipantTeam {
		return fmt.Errorf("%w: participant alias is reserved", ErrAgentSelectionInvariant)
	}
	return nil
}

func selectAgentOfferChannel(channels []decodedAgentOfferChannel,
	explicitAlias string,
) (decodedAgentOfferChannel, error) {
	if explicitAlias != "" {
		for _, channel := range channels {
			if channel.alias == explicitAlias {
				return channel, nil
			}
		}
		return decodedAgentOfferChannel{}, ErrAgentSelectionChannelUnavailable
	}
	eligible := make([]decodedAgentOfferChannel, 0, len(channels))
	for _, channel := range channels {
		for _, reviewer := range channel.reviewers {
			if reviewer.eligible {
				eligible = append(eligible, channel)
				break
			}
		}
	}
	switch len(eligible) {
	case 0:
		return decodedAgentOfferChannel{}, ErrAgentSelectionChannelUnavailable
	case 1:
		return eligible[0], nil
	default:
		candidates := make([]string, len(eligible))
		for index := range eligible {
			candidates[index] = eligible[index].alias
		}
		sort.Strings(candidates)
		return decodedAgentOfferChannel{}, newAgentSelectionCandidatesError(
			ErrAgentSelectionChannelAmbiguous, candidates, model.MaxChannelsPerNode)
	}
}

func selectAgentOfferReviewers(reviewers []decodedAgentOfferReviewer,
	selector string,
) ([]AgentOfferReviewer, error) {
	eligible := make([]decodedAgentOfferReviewer, 0, len(reviewers))
	for _, reviewer := range reviewers {
		if reviewer.eligible {
			eligible = append(eligible, reviewer)
		}
	}
	switch selector {
	case AgentParticipantTeam:
		if len(eligible) == 0 {
			return nil, ErrAgentSelectionParticipantUnavailable
		}
		return projectAgentOfferReviewers(eligible), nil
	case "", AgentParticipantAuto:
		switch len(eligible) {
		case 0:
			return nil, ErrAgentSelectionParticipantUnavailable
		case 1:
			return projectAgentOfferReviewers(eligible), nil
		default:
			return nil, ambiguousAgentReviewers(eligible)
		}
	default:
		for _, reviewer := range eligible {
			if reviewer.projection.EffectiveAlias() == selector {
				return []AgentOfferReviewer{reviewer.projection}, nil
			}
		}
		return nil, ErrAgentSelectionParticipantUnavailable
	}
}

func projectAgentOfferReviewers(reviewers []decodedAgentOfferReviewer) []AgentOfferReviewer {
	result := make([]AgentOfferReviewer, len(reviewers))
	for index := range reviewers {
		result[index] = reviewers[index].projection
	}
	return result
}

func ambiguousAgentReviewers(reviewers []decodedAgentOfferReviewer) error {
	candidates := make([]string, len(reviewers))
	for index := range reviewers {
		candidates[index] = reviewers[index].projection.EffectiveAlias()
	}
	sort.Strings(candidates)
	return newAgentSelectionCandidatesError(ErrAgentSelectionParticipantAmbiguous,
		candidates, model.MaxChildWorks)
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
