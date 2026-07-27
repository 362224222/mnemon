package store

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func uniqueEffectiveAlias(label string, peerID model.PeerID,
	occupied map[string]model.PeerID,
) (string, error) {
	peerText := peerID.String()
	for width := 8; width <= len(peerText); width += 4 {
		if width > len(peerText) {
			width = len(peerText)
		}
		candidate := label + "~" + peerText[len(peerText)-width:]
		if _, exists := occupied[candidate]; !exists {
			return candidate, nil
		}
		if width == len(peerText) {
			break
		}
	}
	base := label + "~" + strings.TrimPrefix(model.Sum([]byte(peerText)).String(), "sha256:")
	for index := 0; index <= len(occupied); index++ {
		candidate := base
		if index != 0 {
			candidate = fmt.Sprintf("%s~%d", base, index)
		}
		if _, exists := occupied[candidate]; !exists {
			return candidate, nil
		}
	}
	return "", ErrChannelJoinConflict
}

func temporaryBindingAlias(channelID model.ChannelID, peerID model.PeerID,
	unavailable map[string]struct{},
) string {
	base := "mnemon-sync~" + strings.TrimPrefix(model.Sum([]byte(
		channelID.String()+"\x00"+peerID.String())).String(), "sha256:")
	for index := 0; ; index++ {
		candidate := base
		if index != 0 {
			candidate = fmt.Sprintf("%s~%d", base, index)
		}
		if _, exists := unavailable[candidate]; !exists {
			return candidate
		}
	}
}

func deriveEffectiveAliases(localPeer model.PeerID,
	roster model.VerifiedRoster,
) (map[model.PeerID]string, map[model.PeerID]model.Member, error) {
	active, labels, labelCounts := activeRosterAliasInputs(localPeer, roster)
	widths := initialEffectiveAliasWidths(active, labels, labelCounts)
	for attempts := 0; attempts <= model.MaxMembersPerChannel; attempts++ {
		aliases, byAlias := projectEffectiveAliases(active, labels, widths)
		if widenCollidingEffectiveAliases(byAlias, widths) {
			continue
		}
		if !validEffectiveAliases(aliases) {
			return nil, nil, ErrChannelJoinConflict
		}
		return aliases, active, nil
	}
	return nil, nil, ErrChannelJoinConflict
}

func activeRosterAliasInputs(localPeer model.PeerID,
	roster model.VerifiedRoster,
) (map[model.PeerID]model.Member, map[model.PeerID]string, map[string]int) {
	active := make(map[model.PeerID]model.Member)
	labels := make(map[model.PeerID]string)
	labelCounts := make(map[string]int)
	for peerID, member := range currentRosterMembers(roster) {
		if peerID == localPeer || member.Status() != model.MemberActive {
			continue
		}
		active[peerID] = member
		label := sanitizeEffectiveAliasLabel(member.DisplayLabel())
		labels[peerID] = label
		labelCounts[label]++
	}
	return active, labels, labelCounts
}

func initialEffectiveAliasWidths(active map[model.PeerID]model.Member,
	labels map[model.PeerID]string, labelCounts map[string]int,
) map[model.PeerID]int {
	widths := make(map[model.PeerID]int, len(active))
	for peerID := range active {
		if labelCounts[labels[peerID]] > 1 {
			widths[peerID] = 8
		}
	}
	return widths
}

func projectEffectiveAliases(active map[model.PeerID]model.Member,
	labels map[model.PeerID]string, widths map[model.PeerID]int,
) (map[model.PeerID]string, map[string][]model.PeerID) {
	aliases := make(map[model.PeerID]string, len(active))
	byAlias := make(map[string][]model.PeerID, len(active))
	for peerID := range active {
		alias := labels[peerID]
		if width := widths[peerID]; width != 0 {
			peerText := peerID.String()
			if width > len(peerText) {
				width = len(peerText)
			}
			alias += "~" + peerText[len(peerText)-width:]
		}
		aliases[peerID] = alias
		byAlias[alias] = append(byAlias[alias], peerID)
	}
	return aliases, byAlias
}

func widenCollidingEffectiveAliases(byAlias map[string][]model.PeerID,
	widths map[model.PeerID]int,
) bool {
	collision := false
	for _, peers := range byAlias {
		if len(peers) == 1 {
			continue
		}
		collision = true
		for _, peerID := range peers {
			if widths[peerID] == 0 {
				widths[peerID] = 8
			} else {
				widths[peerID] += 4
			}
		}
	}
	return collision
}

func validEffectiveAliases(aliases map[model.PeerID]string) bool {
	for _, alias := range aliases {
		if strings.TrimSpace(alias) != alias || alias == "" || len(alias) > model.MaxIdentifierBytes {
			return false
		}
	}
	return true
}

func sanitizeEffectiveAliasLabel(label string) string {
	var result strings.Builder
	result.Grow(len(label))
	separator := false
	for _, character := range label {
		if unicode.IsSpace(character) {
			separator = result.Len() != 0
			continue
		}
		if separator {
			result.WriteByte('-')
			separator = false
		}
		result.WriteRune(character)
	}
	return result.String()
}
