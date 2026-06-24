package replay

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

const gateActor = contract.ActorID("codex@project")

func gateRuntime(t *testing.T) *runtime.Runtime {
	t.Helper()
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	cap := policy.EmbeddedCatalog()["progress_digest"]
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), runtime.RuntimeConfig{
		Rules:       admission.NewRuleSet(cap.Rule(gateActor, ref, policy.Limits{})),
		Authority:   state.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{gateActor: {"progress_digest"}}},
		Subs:        map[contract.ActorID]contract.Subscription{gateActor: {Actor: gateActor, Refs: []contract.ResourceRef{ref}}},
		SchemaGuard: state.SchemaGuardWith(map[contract.ResourceKind][]string{"progress_digest": {"content"}, "goal": {"statement"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { rt.Close() })
	return rt
}

// 每步 = 一批 ingest + 一次 Tick(冲突步刻意单 tick 双事件:两提案对同一派发时视图铸造,
// 第二个必 read-stale —— cutover parity 时确立的判例)。
type gateStep struct {
	name          string
	envs          []contract.ObservationEnvelope
	wantDecisions int
}

func progressEnv(extID, summary string) contract.ObservationEnvelope {
	return contract.ObservationEnvelope{
		ExternalID: extID,
		Event: contract.Event{Type: "progress_digest.write_candidate.observed",
			Payload: map[string]any{"summary": summary}},
	}
}

func deterministicScript() []gateStep {
	return []gateStep{
		{name: "accept create", envs: []contract.ObservationEnvelope{progressEnv("m1", "first fact")}, wantDecisions: 1},
		{name: "accept update", envs: []contract.ObservationEnvelope{progressEnv("m2", "second fact")}, wantDecisions: 1},
		// deny → 0 决策 + 1 条 *.diagnostic(rule deny 不产 kernel 决策;两侧对齐已经经验探针确认)
		{name: "deny secret", envs: []contract.ObservationEnvelope{progressEnv("bad", "password=hunter2")}, wantDecisions: 0},
		// 单 tick 双事件:第二个提案基于同一派发时视图 → read-stale 冲突,Reject 默认下 Rejected
		{name: "read-stale conflict", envs: []contract.ObservationEnvelope{
			progressEnv("c1", "third fact"), progressEnv("c2", "racing fact"),
		}, wantDecisions: 2},
	}
}

// I6 制度化(kernel 半边):真实场景(接受/deny/冲突)的 live 决策序列,与对同一事件日志的
// Replay 在掩码动态字段后逐字段一致。范围注记:脚本不含 kernel-authz 拒绝——replay 以
// permissiveAuthority 归约,authz 拒绝按设计不在复现范围(decision-contract-v1.md)。
func TestReplayReproducesLiveDecisions(t *testing.T) {
	rt := gateRuntime(t)
	var live []contract.Decision
	for _, step := range deterministicScript() {
		for _, env := range step.envs {
			if _, _, err := rt.API().Ingest(gateActor, env); err != nil {
				t.Fatalf("%s: ingest %s: %v", step.name, env.ExternalID, err)
			}
		}
		ds, err := rt.Tick()
		if err != nil {
			t.Fatalf("%s: tick: %v", step.name, err)
		}
		if len(ds) != step.wantDecisions {
			t.Fatalf("%s: want %d decisions, got %d (%#v)", step.name, step.wantDecisions, len(ds), ds)
		}
		live = append(live, ds...)
	}

	events, err := rt.PendingEvents(0)
	if err != nil {
		t.Fatal(err)
	}
	// 日志连续性:把静默错位变成具名失败(决策 IngestSeq 引用日志位置,对齐依赖于此)。
	for i, ev := range events {
		if ev.IngestSeq != int64(i+1) {
			t.Fatalf("event log must be contiguous: events[%d].IngestSeq=%d", i, ev.IngestSeq)
		}
	}
	// deny 类显式在场:日志含该步的持久 *.diagnostic。
	var sawDiagnostic bool
	for _, ev := range events {
		if strings.HasSuffix(ev.Type, ".diagnostic") {
			sawDiagnostic = true
		}
	}
	if !sawDiagnostic {
		t.Fatal("the deny step must leave a durable *.diagnostic in the log")
	}

	replayed := Replay(events, admission.RuleSet{})
	if len(replayed) != len(live) {
		t.Fatalf("decision count: live=%d replay=%d", len(live), len(replayed))
	}
	for i := range live {
		l, r := maskDynamic(live[i]), maskDynamic(replayed[i])
		if !reflect.DeepEqual(l, r) {
			t.Fatalf("decision %d diverged after masking:\nlive:   %#v\nreplay: %#v", i, l, r)
		}
	}
	// 场景覆盖自检:序列里必须真的含 accepted 与 rejected(conflict)两类。
	var accepted, rejected int
	for _, d := range live {
		switch d.Status {
		case contract.Accepted:
			accepted++
		case contract.Rejected:
			rejected++
		}
	}
	if accepted < 3 || rejected < 1 {
		t.Fatalf("scenario must cover accepts and a conflict reject; got accepted=%d rejected=%d", accepted, rejected)
	}
}

// 红可证:掩码比较器必须检出最小篡改(状态/版本/Reason/序号)。
func TestMaskedComparatorDetectsTampering(t *testing.T) {
	base := contract.Decision{Status: contract.Accepted, Reason: "", IngestSeq: 3,
		NewVersions: []contract.ResourceVersion{{Ref: contract.ResourceRef{Kind: "memory", ID: "p"}, Version: 2}}}
	for name, mutate := range map[string]func(*contract.Decision){
		"status flip":       func(d *contract.Decision) { d.Status = contract.Rejected },
		"version rewrite":   func(d *contract.Decision) { d.NewVersions[0].Version = 9 },
		"reason rewrite":    func(d *contract.Decision) { d.Reason = "looks fine" },
		"ingestseq rewrite": func(d *contract.Decision) { d.IngestSeq = 4 },
	} {
		tampered := base
		tampered.NewVersions = append([]contract.ResourceVersion(nil), base.NewVersions...)
		mutate(&tampered)
		if reflect.DeepEqual(maskDynamic(base), maskDynamic(tampered)) {
			t.Fatalf("%s: masked comparator failed to detect the tampering", name)
		}
	}
}

// 冻结契约:IngestSeq 是唯一 ordering key —— Replay/Shadow 不得依赖调用方 slice 顺序。
// 反转日志后,Replay 决策序列与有序输入逐字段一致;Shadow 报告亦不变(乱序曾可能让后续
// proposal 先应用,腐蚀派发时视图)。
func TestReplayAndShadowHonorIngestSeqOverSliceOrder(t *testing.T) {
	rt := gateRuntime(t)
	for _, step := range deterministicScript() {
		for _, env := range step.envs {
			if _, _, err := rt.API().Ingest(gateActor, env); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := rt.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	events, err := rt.PendingEvents(0)
	if err != nil {
		t.Fatal(err)
	}
	reversed := make([]contract.Event, len(events))
	for i, ev := range events {
		reversed[len(events)-1-i] = ev
	}

	want := Replay(events, admission.RuleSet{})
	got := Replay(reversed, admission.RuleSet{})
	if len(got) != len(want) {
		t.Fatalf("reversed-input replay count %d != %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(maskDynamic(want[i]), maskDynamic(got[i])) {
			t.Fatalf("decision %d differs under reversed input", i)
		}
	}

	subs := map[contract.ActorID]contract.Subscription{
		gateActor: {Actor: gateActor, Refs: []contract.ResourceRef{{Kind: "progress_digest", ID: "project"}}},
	}
	live := admission.NewRuleSet(policy.EmbeddedCatalog()["progress_digest"].Rule(gateActor, contract.ResourceRef{Kind: "progress_digest", ID: "project"}, policy.Limits{}))
	a := Shadow(events, subs, live, live)
	b := Shadow(reversed, subs, live, live)
	if a != b {
		t.Fatalf("Shadow must be slice-order independent: ordered=%+v reversed=%+v", a, b)
	}
	if !a.Clean {
		t.Fatalf("self-shadow over the gate log must be clean, got %+v", a)
	}
}
