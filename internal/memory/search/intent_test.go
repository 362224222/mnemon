package search

import (
	"testing"
)

func TestDetectIntent_Why(t *testing.T) {
	cases := []string{
		"why did we choose SQLite",
		"the reason we chose Go because of motivation",
		"为什么选择这个方案",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentWhy {
			t.Errorf("DetectIntent(%q) = %q, want WHY", q, got)
		}
	}
}

func TestDetectIntent_When(t *testing.T) {
	cases := []string{
		"when was the database migrated",
		"timeline of changes",
		"什么时候做的修改",
		"what happened before the release",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentWhen {
			t.Errorf("DetectIntent(%q) = %q, want WHEN", q, got)
		}
	}
}

func TestDetectIntent_Entity(t *testing.T) {
	cases := []string{
		"what is MAGMA",
		"who is responsible for the API",
		"tell me about the graph engine",
		"是什么原理",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentEntity {
			t.Errorf("DetectIntent(%q) = %q, want ENTITY", q, got)
		}
	}
}

func TestDetectIntent_General(t *testing.T) {
	cases := []string{
		"SQLite performance tuning",
		"graph traversal algorithm",
		"内存管理策略",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentGeneral {
			t.Errorf("DetectIntent(%q) = %q, want GENERAL", q, got)
		}
	}
}

func TestIntentFromString_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  Intent
	}{
		{"WHY", IntentWhy},
		{"why", IntentWhy},
		{" When ", IntentWhen},
		{"ENTITY", IntentEntity},
		{"general", IntentGeneral},
	}
	for _, tt := range tests {
		got, err := IntentFromString(tt.input)
		if err != nil {
			t.Errorf("IntentFromString(%q): unexpected error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("IntentFromString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIntentFromString_Invalid(t *testing.T) {
	_, err := IntentFromString("BOGUS")
	if err == nil {
		t.Error("IntentFromString(BOGUS): want error, got nil")
	}
}

func TestGetWeights_KnownIntents(t *testing.T) {
	for _, intent := range []Intent{IntentWhy, IntentWhen, IntentEntity, IntentGeneral} {
		w := GetWeights(intent)
		if len(w) == 0 {
			t.Errorf("GetWeights(%q): want non-empty weights", intent)
		}
		// All weights should sum to ~1.0
		var sum float64
		for _, v := range w {
			sum += v
		}
		if sum < 0.99 || sum > 1.01 {
			t.Errorf("GetWeights(%q): weights sum to %f, want ~1.0", intent, sum)
		}
	}
}

func TestGetWeights_WhyPrioritizesCausal(t *testing.T) {
	w := GetWeights(IntentWhy)
	if w["causal"] <= w["temporal"] || w["causal"] <= w["semantic"] || w["causal"] <= w["entity"] {
		t.Errorf("WHY intent should prioritize causal edges, got %v", w)
	}
}

func TestGetWeights_WhenPrioritizesTemporal(t *testing.T) {
	w := GetWeights(IntentWhen)
	if w["temporal"] <= w["causal"] || w["temporal"] <= w["semantic"] || w["temporal"] <= w["entity"] {
		t.Errorf("WHEN intent should prioritize temporal edges, got %v", w)
	}
}

func TestGetWeights_EntityPrioritizesEntity(t *testing.T) {
	w := GetWeights(IntentEntity)
	if w["entity"] <= w["temporal"] || w["entity"] <= w["causal"] {
		t.Errorf("ENTITY intent should prioritize entity edges, got %v", w)
	}
}

func TestGetWeights_UnknownFallsBackToGeneral(t *testing.T) {
	w := GetWeights(Intent("NONEXISTENT"))
	general := GetWeights(IntentGeneral)
	for k, v := range general {
		if w[k] != v {
			t.Errorf("unknown intent: weight[%s]=%f, want %f (GENERAL default)", k, w[k], v)
		}
	}
}
