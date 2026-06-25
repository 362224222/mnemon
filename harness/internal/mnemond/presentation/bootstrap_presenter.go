package presentation

import (
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

type skillBootstrapPresenter struct{}

func (skillBootstrapPresenter) Intent() string { return IntentSkillBootstrap }

func (skillBootstrapPresenter) Present(_ Request, _ view.View, _ time.Time) (PresentationBody, error) {
	return PresentationBody{Body: BuildSkillBootstrap()}, nil
}

func BuildSkillBootstrap() string {
	return strings.Join([]string{
		"[mnemon:skill-bootstrap]",
		"Use mnemon observe for durable profile, teamwork signal, assignment, and progress_digest events.",
		"Read current work through context.packet or teamwork.events before emitting a governed event.",
	}, "\n")
}

func section(kind, body string) string {
	return "[mnemon:" + kind + "]\n" + body
}
