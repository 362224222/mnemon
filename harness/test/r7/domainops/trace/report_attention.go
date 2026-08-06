package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	openAttentionTurnLimit = 16
	maxAttentionHandlings  = 64
)

type openAttentionSettlement struct {
	Episode   string              `json:"episode"`
	Status    string              `json:"status"`
	TurnLimit int                 `json:"turn_limit"`
	TurnsUsed int                 `json:"turns_used"`
	Waves     []openAttentionWave `json:"waves"`
	Final     []openAttentionNode `json:"final_nodes"`
}

type openAttentionWave struct {
	Wave  int                 `json:"wave"`
	Nodes []openAttentionNode `json:"nodes"`
}

type openAttentionNode struct {
	Role           string `json:"role"`
	OpenUnclaimed  int    `json:"open_unclaimed"`
	OccupiedClaims int    `json:"occupied_claims"`
}

type openAttentionValidation struct {
	Turns    map[string]string
	Barriers map[string]struct{}
}

type attentionFailureKind string

const (
	attentionFailureBudgetExhausted attentionFailureKind = "budget_exhausted"
	attentionFailureClaimOccupied   attentionFailureKind = "claim_occupied"
)

func validateOpenAttention(values []openAttentionSettlement) (openAttentionValidation, error) {
	validated := openAttentionValidation{
		Turns: make(map[string]string), Barriers: make(map[string]struct{}),
	}
	if len(values) != 2 {
		return validated, errors.New("sanitized live report omits an attention settlement")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateOpenAttentionSettlement(value, seen, &validated); err != nil {
			return validated, err
		}
	}
	return validated, nil
}

func validateOpenAttentionSettlement(value openAttentionSettlement, seen map[string]struct{},
	validated *openAttentionValidation,
) error {
	if value.Episode != "episode-1" && value.Episode != "episode-2" {
		return errors.New("sanitized live report has an unknown attention episode")
	}
	if _, duplicate := seen[value.Episode]; duplicate {
		return errors.New("sanitized live report repeats an attention settlement")
	}
	seen[value.Episode] = struct{}{}
	if value.Status != "settled" || value.TurnLimit != openAttentionTurnLimit ||
		value.TurnsUsed < 0 || value.TurnsUsed > value.TurnLimit {
		return errors.New("sanitized live report has an invalid attention settlement")
	}
	used := 0
	for index, wave := range value.Waves {
		count, err := bindOpenAttentionWave(value.Episode, index, wave, validated)
		if err != nil {
			return err
		}
		used += count
	}
	final, err := validateOpenAttentionNodes(value.Final)
	if err != nil {
		return err
	}
	if used != value.TurnsUsed || used > value.TurnLimit ||
		positiveOpenUnclaimedNodes(final) != 0 || positiveOccupiedNodes(final) != 0 {
		return errors.New("sanitized live report attention turns are inconsistent")
	}
	return nil
}

func bindOpenAttentionWave(episode string, index int, wave openAttentionWave,
	validated *openAttentionValidation,
) (int, error) {
	if wave.Wave != index+1 {
		return 0, errors.New("sanitized live report has a non-contiguous attention wave")
	}
	nodes, err := validateOpenAttentionNodes(wave.Nodes)
	if err != nil {
		return 0, err
	}
	if positiveOpenUnclaimedNodes(nodes) == 0 || positiveOccupiedNodes(nodes) != 0 {
		return 0, errors.New("sanitized live report has an invalid attention wave")
	}
	barrier := fmt.Sprintf("%s-open-attention-%d", episode, wave.Wave)
	validated.Barriers[barrier] = struct{}{}
	used := 0
	for role, node := range nodes {
		if node.OpenUnclaimed == 0 {
			continue
		}
		turn := fmt.Sprintf("%s-open-attention-%d-%s", episode, wave.Wave, role)
		validated.Turns[turn] = role
		used++
	}
	return used, nil
}

func validateOpenAttentionNodes(values []openAttentionNode) (map[string]openAttentionNode, error) {
	if len(values) != len(domainRoles) {
		return nil, errors.New("sanitized live report has an incomplete attention snapshot")
	}
	nodes := make(map[string]openAttentionNode, len(values))
	for _, value := range values {
		if !slices.Contains(domainRoles, value.Role) || value.OpenUnclaimed < 0 ||
			value.OpenUnclaimed > maxAttentionHandlings || value.OccupiedClaims < 0 ||
			value.OccupiedClaims > maxAttentionHandlings {
			return nil, errors.New("sanitized live report has an invalid attention node")
		}
		if _, duplicate := nodes[value.Role]; duplicate {
			return nil, errors.New("sanitized live report repeats an attention node")
		}
		nodes[value.Role] = value
	}
	return nodes, nil
}

func validateFailedOpenAttention(code string, value *openAttentionSettlement,
	turns []turnSummary,
) error {
	kind, present, err := classifyAttentionFailure(code, value)
	if err != nil || !present {
		return err
	}
	completed := make(map[string]struct{}, len(turns))
	for _, turn := range turns {
		completed[turn.Turn] = struct{}{}
	}
	used, err := validateFailedAttentionWaves(*value, completed)
	if err != nil {
		return err
	}
	final, err := validateOpenAttentionNodes(value.Final)
	if err != nil {
		return err
	}
	if used != value.TurnsUsed {
		return errors.New("sanitized failure report has inconsistent attention turns")
	}
	if kind == attentionFailureClaimOccupied {
		if positiveOccupiedNodes(final) == 0 {
			return errors.New("sanitized failure report does not prove an occupied claim boundary")
		}
		return nil
	}
	if positiveOccupiedNodes(final) != 0 || positiveOpenUnclaimedNodes(final) == 0 ||
		used+positiveOpenUnclaimedNodes(final) <= value.TurnLimit {
		return errors.New("sanitized failure report does not prove attention budget exhaustion")
	}
	return nil
}

func classifyAttentionFailure(code string,
	value *openAttentionSettlement,
) (attentionFailureKind, bool, error) {
	kind := attentionFailureKind("")
	suffix := ""
	switch {
	case strings.HasSuffix(code, ".attention-budget-exhausted"):
		kind, suffix = attentionFailureBudgetExhausted, ".attention-budget-exhausted"
	case strings.HasSuffix(code, ".attention-claim-occupied"):
		kind, suffix = attentionFailureClaimOccupied, ".attention-claim-occupied"
	default:
		if value != nil {
			return "", false, errors.New("sanitized failure report has unexpected attention evidence")
		}
		return "", false, nil
	}
	if value == nil || (value.Episode != "episode-1" && value.Episode != "episode-2") ||
		value.TurnLimit != openAttentionTurnLimit || value.TurnsUsed < 0 ||
		value.TurnsUsed > value.TurnLimit {
		return "", false, errors.New("sanitized failure report omits its bounded attention evidence")
	}
	if value.Status != string(kind) || code != "scenario."+value.Episode+suffix {
		return "", false, errors.New("sanitized failure report mismatches its attention identity")
	}
	return kind, true, nil
}

func validateFailedAttentionWaves(value openAttentionSettlement,
	completed map[string]struct{},
) (int, error) {
	used := 0
	for index, wave := range value.Waves {
		if wave.Wave != index+1 {
			return 0, errors.New("sanitized failure report has a non-contiguous attention wave")
		}
		nodes, err := validateOpenAttentionNodes(wave.Nodes)
		if err != nil {
			return 0, err
		}
		if positiveOpenUnclaimedNodes(nodes) == 0 || positiveOccupiedNodes(nodes) != 0 {
			return 0, errors.New("sanitized failure report has an invalid attention wave")
		}
		for role, node := range nodes {
			if node.OpenUnclaimed == 0 {
				continue
			}
			turn := fmt.Sprintf("%s-open-attention-%d-%s", value.Episode, wave.Wave, role)
			if _, exists := completed[turn]; !exists {
				return 0, errors.New("sanitized failure report omits a completed attention turn")
			}
			used++
		}
	}
	return used, nil
}

func positiveOpenUnclaimedNodes(nodes map[string]openAttentionNode) int {
	positive := 0
	for _, node := range nodes {
		if node.OpenUnclaimed > 0 {
			positive++
		}
	}
	return positive
}

func positiveOccupiedNodes(nodes map[string]openAttentionNode) int {
	positive := 0
	for _, node := range nodes {
		if node.OccupiedClaims > 0 {
			positive++
		}
	}
	return positive
}
