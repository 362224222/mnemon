package agency

// Public Agent View wire types remain private so only the checked projector
// and parsers can construct machine-owned facts.
type agentViewWire struct {
	Schema         string                     `json:"schema"`
	Version        int                        `json:"version"`
	View           string                     `json:"view"`
	Current        *agentViewCurrentWire      `json:"current,omitempty"`
	Related        []agentViewRelatedWire     `json:"related,omitempty"`
	Outstanding    agentViewOutstandingWire   `json:"outstanding"`
	References     []agentViewReferenceWire   `json:"references,omitempty"`
	Targets        []string                   `json:"targets,omitempty"`
	AllowedIntents []agentViewIntentShapeWire `json:"allowed_intents"`
	Provenance     []string                   `json:"provenance_handles,omitempty"`
}

type agentViewCurrentWire struct {
	Facts    agentViewCurrentFactsWire `json:"facts"`
	Semantic agentViewSemanticWire     `json:"semantic"`
}

type agentViewCurrentFactsWire struct {
	Handle                  string                  `json:"handle"`
	ReplyTo                 string                  `json:"reply_to"`
	ReplyRequired           bool                    `json:"reply_required"`
	ReplyTarget             string                  `json:"reply_target,omitempty"`
	ReplyObservationPending bool                    `json:"reply_observation_pending"`
	Artifacts               []agentViewArtifactWire `json:"artifacts,omitempty"`
}

type agentViewSemanticWire struct {
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

type agentViewReferenceWire struct {
	Facts agentViewReferenceFactsWire `json:"facts"`
}

type agentViewReferenceFactsWire struct {
	Key              string                         `json:"key"`
	Head             string                         `json:"head"`
	State            string                         `json:"state"`
	Artifact         *agentViewArtifactWire         `json:"artifact,omitempty"`
	TerminalOutcomes *agentViewTerminalOutcomesWire `json:"terminal_outcomes,omitempty"`
}

type agentViewTerminalOutcomesWire struct {
	Completed  int64 `json:"completed"`
	Declined   int64 `json:"declined"`
	Unresolved int64 `json:"unresolved"`
}

type agentViewArtifactWire struct {
	Handle string `json:"handle"`
	Digest string `json:"digest"`
}

type agentViewIntentShapeWire struct {
	Consequence string `json:"consequence"`
	Subject     string `json:"subject"`
	Successors  string `json:"successors,omitempty"`
	Reference   string `json:"reference,omitempty"`
	Artifacts   string `json:"artifacts"`
}
