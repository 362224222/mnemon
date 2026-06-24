package policy

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestBuiltinsLoadFromEmbeddedSpecs(t *testing.T) {
	for _, id := range []string{"agent_profile", "teamwork_signal", "project_intent", "assignment", "progress_digest"} {
		cap, ok := EmbeddedCatalog()[id]
		if !ok {
			t.Fatalf("builtin %q must load from assets/capabilities", id)
		}
		if cap.Decode == nil || cap.Header == nil {
			t.Fatalf("builtin %q must carry compiled decode/header", id)
		}
	}
	// Generic substrate fixtures live in testdata/capabilities and the e2e external-package leg,
	// never in the embedded product catalog.
	for _, id := range []string{"fixture_record", "fixture_declaration", "note", "decision"} {
		if _, ok := EmbeddedCatalog()[id]; ok {
			t.Fatalf("%q must NOT be embedded (demoted to a test/external-package fixture)", id)
		}
	}
	if len(EmbeddedCatalog()) != 5 {
		t.Fatalf("EmbeddedCatalog() must be {agent_profile, teamwork_signal, project_intent, assignment, progress_digest}, got %d entries", len(EmbeddedCatalog()))
	}
}

// loadBuiltins 的错误路径(嵌入物走 panic;外部目录——阶段五——走这些 error):
// 坏 JSON、FromSpec 失败、跨 spec 重名/重事件类型。
func TestLoadBuiltinsErrorPaths(t *testing.T) {
	good := `{"schema_version":1,"name":"note","observed_type":"note.write_candidate.observed",
"proposed_type":"note.write.proposed","resource_kind":"note","items_field":"items",
"fields":[{"name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# Notes","field":"text"}}}}`
	cases := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{"malformed json", map[string]string{"bad.json": `{nope`}, "parse capability spec"},
		{"fromspec failure", map[string]string{"bad.json": `{"schema_version":1,"name":"x"}`}, "compile capability spec"},
		{"duplicate name", map[string]string{"a.json": good, "b.json": good}, "duplicate capability name"},
		// type forgery (one spec claiming another family's events) is PRE-EMPTED by the frozen
		// type grammar — types derive from the name, so a cross-family claim cannot compile;
		// the registry's type axes remain as defense in depth (pinned via mergeExternal tests).
		{"type forgery pre-empted by grammar", map[string]string{"a.json": good,
			"b.json": strings.Replace(good, `"name":"note"`, `"name":"memo"`, 1)}, "frozen type grammar"},
	}
	for _, c := range cases {
		m := fstest.MapFS{}
		for f, body := range c.files {
			m["capabilities/"+f] = &fstest.MapFile{Data: []byte(body)}
		}
		if _, err := loadBuiltins(m); err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Fatalf("%s: want error containing %q, got %v", c.name, c.wantErr, err)
		}
	}
}

// 冻结协议面在语法层同样 fail-closed:任何层级的未知 JSON 键(顶层/字段对象/校验器对象/
// 渲染对象)都拒绝整个 spec——typo 永不静默降级为缺省行为。外部目录(阶段五)依赖同一解码器。
func TestSpecDecodeRejectsUnknownJSONFields(t *testing.T) {
	base := `{"schema_version":1,"name":"note","observed_type":"note.write_candidate.observed",
"proposed_type":"note.write.proposed","resource_kind":"note","items_field":"items",
"fields":[{"name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# Notes","field":"text"}}}}`
	cases := []struct{ name, body string }{
		{"top-level unknown", strings.Replace(base, `"items_field":"items",`, `"items_field":"items","typo_field":true,`, 1)},
		{"field-object unknown", strings.Replace(base, `{"name":"text",`, `{"name":"text","requierd":true,`, 1)},
		{"validator-object unknown", strings.Replace(base, `{"id":"required",`, `{"id":"required","prams":{},`, 1)},
		{"render-object unknown", strings.Replace(base, `"render":{"content"`, `"render":{"contnet":{},"content"`, 1)},
	}
	for _, c := range cases {
		m := fstest.MapFS{"capabilities/x.json": &fstest.MapFile{Data: []byte(c.body)}}
		if _, err := loadBuiltins(m); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("%s: want unknown-field rejection, got %v", c.name, err)
		}
	}
	// 尾随数据同属语法层 fail-closed:{spec}{...} 与 {spec} garbage 都拒绝整个 spec。
	for _, c := range []struct{ name, body string }{
		{"trailing object", base + ` {}`},
		{"trailing garbage", base + ` xx`},
	} {
		m := fstest.MapFS{"capabilities/x.json": &fstest.MapFile{Data: []byte(c.body)}}
		if _, err := loadBuiltins(m); err == nil || !strings.Contains(err.Error(), "trailing data") {
			t.Fatalf("%s: want trailing-data rejection, got %v", c.name, err)
		}
	}

	// 基线:未注入 typo 的 base 必须可解析(防本测试自身的假阳性)。
	m := fstest.MapFS{"capabilities/note.json": &fstest.MapFile{Data: []byte(base)}}
	if _, err := loadBuiltins(m); err != nil {
		t.Fatalf("baseline spec must load: %v", err)
	}
}
