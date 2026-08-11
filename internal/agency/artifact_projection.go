package agency

import (
	"unicode"
	"unicode/utf8"
)

// MaxAgentArtifactReadBytes is the fixed model-facing expansion bound. CAS
// may retain larger opaque objects, but one Agent terminal read cannot inject
// more than this amount into a Runtime context.
const MaxAgentArtifactReadBytes = 64 << 10

// ValidateAgentArtifactText accepts only bounded terminal-safe UTF-8. Newline,
// carriage return, and tab remain available to ordinary text; control and
// formatting code points that can alter terminal presentation fail closed.
func ValidateAgentArtifactText(content []byte) error {
	if len(content) > MaxAgentArtifactReadBytes {
		return limit("Agent Artifact read", len(content), MaxAgentArtifactReadBytes)
	}
	if !utf8.Valid(content) {
		return invalid("Agent Artifact read", "must be valid UTF-8")
	}
	for _, value := range string(content) {
		if value == '\n' || value == '\r' || value == '\t' {
			continue
		}
		if unicode.IsControl(value) || unicode.In(value, unicode.Cf) {
			return invalid("Agent Artifact read", "contains an unsafe control character")
		}
	}
	return nil
}
