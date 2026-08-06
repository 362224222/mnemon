package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	firstAttentionTurnLimit = 16
	maxUnseenOpenHandlings  = 64
)

type firstAttentionSettlement struct {
	Episode   string               `json:"episode"`
	Status    string               `json:"status"`
	TurnLimit int                  `json:"turn_limit"`
	TurnsUsed int                  `json:"turns_used"`
	Waves     []firstAttentionWave `json:"waves"`
	Final     []firstAttentionNode `json:"final_nodes"`
}

type firstAttentionWave struct {
	Wave  int                  `json:"wave"`
	Nodes []firstAttentionNode `json:"nodes"`
}

type firstAttentionNode struct {
	Role         string `json:"role"`
	UnseenOpen   int    `json:"unseen_open"`
	ActiveClaims int    `json:"active_claims"`
}

type firstAttentionValidation struct {
	Turns    map[string]string
	Barriers map[string]struct{}
}

func validateFirstAttention(values []firstAttentionSettlement) (firstAttentionValidation, error) {
	validated := firstAttentionValidation{
		Turns: make(map[string]string), Barriers: make(map[string]struct{}),
	}
	if len(values) != 2 {
		return validated, errors.New("sanitized live report omits an attention settlement")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateFirstAttentionSettlement(value, seen, &validated); err != nil {
			return validated, err
		}
	}
	return validated, nil
}

func validateFirstAttentionSettlement(value firstAttentionSettlement, seen map[string]struct{},
	validated *firstAttentionValidation,
) error {
	if value.Episode != "episode-1" && value.Episode != "episode-2" {
		return errors.New("sanitized live report has an unknown attention episode")
	}
	if _, duplicate := seen[value.Episode]; duplicate {
		return errors.New("sanitized live report repeats an attention settlement")
	}
	seen[value.Episode] = struct{}{}
	if value.Status != "settled" || value.TurnLimit != firstAttentionTurnLimit ||
		value.TurnsUsed < 0 || value.TurnsUsed > value.TurnLimit {
		return errors.New("sanitized live report has an invalid attention settlement")
	}
	used := 0
	for index, wave := range value.Waves {
		count, err := bindFirstAttentionWave(value.Episode, index, wave, validated)
		if err != nil {
			return err
		}
		used += count
	}
	if _, err := validateFirstAttentionNodes(value.Final, true); err != nil {
		return err
	}
	if used != value.TurnsUsed || used > value.TurnLimit {
		return errors.New("sanitized live report attention turns are inconsistent")
	}
	return nil
}

func bindFirstAttentionWave(episode string, index int, wave firstAttentionWave,
	validated *firstAttentionValidation,
) (int, error) {
	if wave.Wave != index+1 {
		return 0, errors.New("sanitized live report has a non-contiguous attention wave")
	}
	nodes, err := validateFirstAttentionNodes(wave.Nodes, false)
	if err != nil {
		return 0, err
	}
	barrier := fmt.Sprintf("%s-attention-debt-%d", episode, wave.Wave)
	validated.Barriers[barrier] = struct{}{}
	used := 0
	for role, node := range nodes {
		if node.UnseenOpen == 0 {
			continue
		}
		turn := fmt.Sprintf("%s-attention-debt-%d-%s", episode, wave.Wave, role)
		validated.Turns[turn] = role
		used++
	}
	return used, nil
}

func validateFirstAttentionNodes(values []firstAttentionNode,
	final bool,
) (map[string]firstAttentionNode, error) {
	if len(values) != len(domainRoles) {
		return nil, errors.New("sanitized live report has an incomplete attention snapshot")
	}
	nodes := make(map[string]firstAttentionNode, len(values))
	positive := 0
	for _, value := range values {
		if !slices.Contains(domainRoles, value.Role) || value.UnseenOpen < 0 ||
			value.UnseenOpen > maxUnseenOpenHandlings || value.ActiveClaims != 0 {
			return nil, errors.New("sanitized live report has an invalid attention node")
		}
		if _, duplicate := nodes[value.Role]; duplicate {
			return nil, errors.New("sanitized live report repeats an attention node")
		}
		nodes[value.Role] = value
		if value.UnseenOpen > 0 {
			positive++
		}
	}
	if final && positive != 0 {
		return nil, errors.New("sanitized live report ends with unpaid attention debt")
	}
	if !final && positive == 0 {
		return nil, errors.New("sanitized live report contains an empty attention wave")
	}
	return nodes, nil
}

func validateFailedFirstAttention(code string, value *firstAttentionSettlement,
	turns []turnSummary,
) error {
	budgetExhausted := strings.HasSuffix(code, ".attention-budget-exhausted")
	if !budgetExhausted {
		if value != nil {
			return errors.New("sanitized failure report has unexpected attention evidence")
		}
		return nil
	}
	if value == nil || (value.Episode != "episode-1" && value.Episode != "episode-2") ||
		value.Status != "budget_exhausted" || value.TurnLimit != firstAttentionTurnLimit ||
		value.TurnsUsed < 0 || value.TurnsUsed > value.TurnLimit {
		return errors.New("sanitized failure report omits its bounded attention evidence")
	}
	if code != "scenario."+value.Episode+".attention-budget-exhausted" {
		return errors.New("sanitized failure report mismatches its attention episode")
	}
	completed := make(map[string]struct{}, len(turns))
	for _, turn := range turns {
		completed[turn.Turn] = struct{}{}
	}
	used, err := validateFailedAttentionWaves(*value, completed)
	if err != nil {
		return err
	}
	final, err := validateFirstAttentionNodes(value.Final, false)
	if err != nil {
		return err
	}
	if used != value.TurnsUsed || used+positiveAttentionNodes(final) <= value.TurnLimit {
		return errors.New("sanitized failure report does not prove attention budget exhaustion")
	}
	return nil
}

func validateFailedAttentionWaves(value firstAttentionSettlement,
	completed map[string]struct{},
) (int, error) {
	used := 0
	for index, wave := range value.Waves {
		if wave.Wave != index+1 {
			return 0, errors.New("sanitized failure report has a non-contiguous attention wave")
		}
		nodes, err := validateFirstAttentionNodes(wave.Nodes, false)
		if err != nil {
			return 0, err
		}
		for role, node := range nodes {
			if node.UnseenOpen == 0 {
				continue
			}
			turn := fmt.Sprintf("%s-attention-debt-%d-%s", value.Episode, wave.Wave, role)
			if _, exists := completed[turn]; !exists {
				return 0, errors.New("sanitized failure report omits a completed attention turn")
			}
			used++
		}
	}
	return used, nil
}

func positiveAttentionNodes(nodes map[string]firstAttentionNode) int {
	positive := 0
	for _, node := range nodes {
		if node.UnseenOpen > 0 {
			positive++
		}
	}
	return positive
}
