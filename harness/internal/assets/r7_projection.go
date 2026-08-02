package assets

import (
	"embed"
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	r7GuidePath       = "r7/mnemond.md"
	r7HookCuePath     = "r7/hook-cue.txt"
	r7PiExtensionPath = "r7/pi/mnemond.ts"

	MaxR7GuideBytes       = 8 << 10
	MaxR7HookCueBytes     = 160
	MaxR7PiExtensionBytes = 8 << 10
)

//go:embed r7/mnemond.md r7/hook-cue.txt r7/pi/mnemond.ts
var r7ProjectionFS embed.FS

// R7Projection is the small, pattern-neutral surface installed for an Agent.
// Authority and dynamic work remain in mnemond; these bytes only teach the
// View -> Intent -> Receipt interaction.
type R7Projection struct {
	guide       []byte
	cue         string
	piExtension []byte
}

func LoadR7Projection() (R7Projection, error) {
	guide, err := r7ProjectionFS.ReadFile(r7GuidePath)
	if err != nil {
		return R7Projection{}, err
	}
	cueBytes, err := r7ProjectionFS.ReadFile(r7HookCuePath)
	if err != nil {
		return R7Projection{}, err
	}
	piExtension, err := r7ProjectionFS.ReadFile(r7PiExtensionPath)
	if err != nil {
		return R7Projection{}, err
	}
	if len(guide) == 0 || len(guide) > MaxR7GuideBytes || !utf8.Valid(guide) ||
		strings.IndexByte(string(guide), 0) >= 0 {
		return R7Projection{}, errors.New("R7 guide is not bounded UTF-8")
	}
	if len(cueBytes) == 0 || len(cueBytes) > MaxR7HookCueBytes || !utf8.Valid(cueBytes) ||
		strings.IndexByte(string(cueBytes), 0) >= 0 {
		return R7Projection{}, errors.New("R7 Hook cue is not bounded UTF-8")
	}
	cue := strings.TrimSuffix(string(cueBytes), "\n")
	if cue == "" || strings.ContainsAny(cue, "\r\n") {
		return R7Projection{}, errors.New("R7 Hook cue must be one nonempty line")
	}
	if len(piExtension) == 0 || len(piExtension) > MaxR7PiExtensionBytes ||
		!utf8.Valid(piExtension) || strings.IndexByte(string(piExtension), 0) >= 0 {
		return R7Projection{}, errors.New("R7 Pi extension is not bounded UTF-8")
	}
	return R7Projection{
		guide:       append([]byte(nil), guide...),
		cue:         cue,
		piExtension: append([]byte(nil), piExtension...),
	}, nil
}

func (projection R7Projection) Guide() []byte {
	return append([]byte(nil), projection.guide...)
}

func (projection R7Projection) HookCue() string { return projection.cue }

func (projection R7Projection) PiExtension() []byte {
	return append([]byte(nil), projection.piExtension...)
}
