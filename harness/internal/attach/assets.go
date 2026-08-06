package attach

import (
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	guideAsset     = "assets/mnemond.md"
	cueAsset       = "assets/hook-cue.txt"
	extensionAsset = "assets/pi/mnemond.ts"

	MaxGuideBytes     = 4 << 10
	MaxCueBytes       = 160
	MaxExtensionBytes = 8 << 10
)

//go:embed assets/mnemond.md assets/hook-cue.txt assets/pi/mnemond.ts
var projectionFS embed.FS

// Projection is the complete, immutable Pi-facing R7 surface. It contains no
// runtime state; callers receive copies of all mutable byte slices.
type Projection struct {
	guide     []byte
	cue       string
	extension []byte
}

// Load verifies and returns the three embedded, pattern-neutral R7 assets.
func Load() (Projection, error) {
	guide, err := projectionFS.ReadFile(guideAsset)
	if err != nil {
		return Projection{}, err
	}
	cueBytes, err := projectionFS.ReadFile(cueAsset)
	if err != nil {
		return Projection{}, err
	}
	extension, err := projectionFS.ReadFile(extensionAsset)
	if err != nil {
		return Projection{}, err
	}
	if err := validateText("guide", guide, MaxGuideBytes); err != nil {
		return Projection{}, err
	}
	if err := validateText("cue", cueBytes, MaxCueBytes); err != nil {
		return Projection{}, err
	}
	if err := validateText("Pi extension", extension, MaxExtensionBytes); err != nil {
		return Projection{}, err
	}
	cue := strings.TrimSuffix(string(cueBytes), "\n")
	if cue == "" || strings.ContainsAny(cue, "\r\n") {
		return Projection{}, errors.New("attach: cue must be one nonempty line")
	}
	if err := validateNeutralProjection(guide, cue, extension); err != nil {
		return Projection{}, err
	}
	return Projection{guide: clone(guide), cue: cue, extension: clone(extension)}, nil
}

func (projection Projection) Guide() []byte { return clone(projection.guide) }

func (projection Projection) HookCue() string { return projection.cue }

func (projection Projection) PiExtension() []byte { return clone(projection.extension) }

func validateText(name string, content []byte, maximum int) error {
	if len(content) == 0 || len(content) > maximum || !utf8.Valid(content) ||
		strings.IndexByte(string(content), 0) >= 0 {
		return fmt.Errorf("attach: %s is not bounded UTF-8", name)
	}
	return nil
}

func validateNeutralProjection(guide []byte, cue string, extension []byte) error {
	all := strings.ToLower(string(guide) + "\n" + cue + "\n" + string(extension))
	for _, forbidden := range []string{
		"--event-id", "--operation-id", "--principal", "--fence", "--peer-id",
		"deepseek", "api_key", "api-key", "authorization:", "bearer ", "sk-",
	} {
		if strings.Contains(all, forbidden) {
			return fmt.Errorf("attach: projection contains forbidden surface %q", forbidden)
		}
	}
	source := string(extension)
	if strings.Count(source, "content: HOOK_CUE") != 1 ||
		strings.Count(source, "text: receiptText") != 1 ||
		!strings.Contains(source, "const HOOK_CUE = "+strconv.Quote(cue)+";") ||
		!strings.Contains(source, `pi.on("before_agent_start"`) {
		return errors.New("attach: Pi extension does not have one fixed cue and one bounded Receipt surface")
	}
	for _, forbidden := range []string{
		"process.env", "stdout", "stderr", "json.parse(", "content: raw",
		"content: output", "content: result", "text: raw", "text: output",
		"text: result", "event_id", "eventid", "payload",
		"transcript", "credential", "console.", "--socket",
	} {
		if strings.Contains(strings.ToLower(source), forbidden) {
			return fmt.Errorf("attach: Pi extension carries runtime data %q", forbidden)
		}
	}
	return nil
}

func clone(content []byte) []byte { return append([]byte(nil), content...) }
