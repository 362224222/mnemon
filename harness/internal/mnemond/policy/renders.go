package policy

import "strings"

// renderCatalog is the CLOSED render vocabulary of capability spec v1. Render members are
// CONCAT-ONLY by frozen contract: a member that evaluates user content as a template is forbidden
// vocabulary (render injection is structurally impossible — item values are joined, never executed).
var renderCatalog = map[string]paramSchema{
	"entry-list":  {},
	"bullet-list": {required: []string{"title", "field"}},
}

// compileHeader builds the Capability.Header closure from the render spec: a fresh map per call
// carrying the static literal fields plus, when a content member is selected, the rendered
// "content" key.
func compileHeader(spec CapabilitySpec) func(items []Item) map[string]any {
	static := map[string]string{}
	for k, v := range spec.Render.Static {
		static[k] = v
	}
	content := spec.Render.Content
	return func(items []Item) map[string]any {
		h := map[string]any{}
		for k, v := range static {
			h[k] = v
		}
		if content == nil {
			return h
		}
		switch content.Member {
		case "entry-list":
			h["content"] = renderEntryItems(items)
		case "bullet-list":
			lines := []string{content.Params["title"]}
			for _, it := range items {
				lines = append(lines, "- "+itemString(it, content.Params["field"]))
			}
			h["content"] = strings.Join(lines, "\n")
		}
		return h
	}
}
