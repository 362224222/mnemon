package node

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func channelAlias(name string) string {
	var result strings.Builder
	separator := false
	for _, character := range strings.ToLower(name) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			result.WriteRune(character)
			separator = false
		} else if result.Len() != 0 && !separator {
			result.WriteByte('-')
			separator = true
		}
	}
	alias := strings.Trim(result.String(), "-")
	if alias == "" {
		return "channel"
	}
	if len(alias) > 48 {
		alias = strings.Trim(alias[:48], "-")
	}
	return alias
}

func uniqueChannelAlias(base string, authority store.ChannelControlAuthority) string {
	used := make(map[string]struct{}, len(authority.Channels()))
	for _, channel := range authority.Channels() {
		used[channel.Channel().LocalAlias()] = struct{}{}
	}
	if _, exists := used[base]; !exists {
		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := fmt.Sprintf("%s-%d", base, suffix)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func existingChannelAlias(channelID model.ChannelID, authority store.ChannelControlAuthority) string {
	for _, channel := range authority.Channels() {
		if channel.Channel().ID() == channelID {
			return channel.Channel().LocalAlias()
		}
	}
	return ""
}

func rebindChannelAlias(channel model.Channel, alias string) (model.Channel, error) {
	return model.NewChannel(model.ChannelSpec{Descriptor: channel.Descriptor(), LocalAlias: alias,
		RosterHead: channel.RosterHead(), Status: channel.Status(), TopicState: channel.TopicState(),
		UpdatedAt: channel.UpdatedAt()})
}
