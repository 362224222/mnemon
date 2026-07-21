package localapi

import "testing"

func TestNodeRuntimeRejectsMissingRunAttachmentState(t *testing.T) {
	filesystem, err := (NodeRuntime{}).NewRunAttachmentFilesystem("")
	if err == nil || filesystem != nil {
		t.Fatalf("missing node state filesystem = (%#v, %v)", filesystem, err)
	}
}
