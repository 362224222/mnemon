package hostagent

// knownLegacyManagedHashes are the sha256 fingerprints of managed-file content we shipped BEFORE
// the corresponding surface became generated (the 16 handwritten hook shells retired by the
// stage-3 hook generator; byte-identical cross-host pairs share one fingerprint). A workspace
// that predates ownership recording holds these exact bytes with no recorded prior;
// classifyManaged adopts a match as ours so the generated replacement (incl. the claude compact
// escape fix) reaches old workspaces — a REAL user edit never matches and is preserved as before.
//
// Scope notes (deliberate): adoption is CONTENT-keyed, not path-keyed — matching content is by
// definition bytes we shipped, so adopting it at any managed path is safe. The table pins only
// the LAST handwritten generation of each surface; earlier generations (and the codex projected
// SKILL.md variants, which append a binding-derived runtime note) upgrade via uninstall+reinstall.
var knownLegacyManagedHashes = map[string]bool{
	"0281afc8283922df9ee4b7a1fabf9910776079c66b107f3d2d8179337ce3eec1": true, // hosts/claude-code/memory/hooks/compact.sh
	"870336ca55cc85bb891f3abdfe6477bc851d4b7215476ffb4d70427c6be15c59": true, // hosts/claude-code/memory/hooks/nudge.sh
	"7d3f4fc0c0438371a5d4d5f1b9451f4b84bdb8adede836d2e52f5ceb5a1b9b3e": true, // hosts/claude-code/memory/hooks/prime.sh; hosts/codex/memory/hooks/prime.sh
	"6b755a8e8325abf9e52442402a04d6f3e9da77dfe40762ea187ff170be95b4d0": true, // hosts/claude-code/memory/hooks/remind.sh
	"dd0c690b9e8b12b0bce3ea35c2fd0e7396d6dc3752d2e74afcb46b7340914aa5": true, // hosts/claude-code/skill/hooks/compact.sh
	"4177fa0388447132f26e3bee2b33254272fd164c8f0e7a95b00ca9d35576b6fb": true, // hosts/claude-code/skill/hooks/nudge.sh
	"c7309e602979940c0d0aa62e75ceb0bf15ddd36d37e05f5d83e50caf4f818db1": true, // hosts/claude-code/skill/hooks/prime.sh
	"39508d6cc8ec74307b8f8b65719a4e48f36f87e741aeaaf8f87166484c466a9e": true, // hosts/claude-code/skill/hooks/remind.sh; hosts/codex/skill/hooks/remind.sh
	"d7021b8ce2bf4a00bab6946254a93e5fe4512ba0fb7be7b87955ef79aab76b7e": true, // hosts/codex/memory/hooks/compact.sh
	"6673d442815e416ebdebac471deefadfa5f5340c70c9326a866368030a4aa585": true, // hosts/codex/memory/hooks/nudge.sh
	"de3a08eef56f9406f96d7c961d43215702e83a7974ea0e652accc9e6d2a342c3": true, // hosts/codex/memory/hooks/remind.sh
	"466b4ccdfef70ec931db795d7885b8da98adc1ab10b63c6a6f44ebfe80fe470f": true, // hosts/codex/skill/hooks/compact.sh
	"ad914b0849a1dc2a69c5580f9434b78e7bff15167bc74bf393cacfdd12169844": true, // hosts/codex/skill/hooks/nudge.sh
	"93621a58110dcd1ee6ff97745dcac64e04436a7cb7a32f38c4d7b4df49270d64": true, // hosts/codex/skill/hooks/prime.sh
	"0a2abd4211b03d9f8e327927b230ffe0bae10ba9f8574f9c8d53d26c553057fb": true, // legacy handwritten memory-set/SKILL.md (claude projection = canonical bytes)
	"4f75d45525fa39f804e5d87e2b3038cc6afb77fc01cd18d8f69017648871d663": true, // legacy handwritten skill-manage/SKILL.md (claude projection = canonical bytes)
}
