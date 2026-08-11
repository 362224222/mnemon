package search

import (
	"container/heap"
	"testing"
)

func TestBeamHeap_MaxHeap(t *testing.T) {
	h := &beamHeap{}
	heap.Init(h)

	heap.Push(h, beamItem{id: "low", score: 1.0, depth: 0})
	heap.Push(h, beamItem{id: "high", score: 5.0, depth: 0})
	heap.Push(h, beamItem{id: "mid", score: 3.0, depth: 0})

	// Pop should return highest score first (max-heap)
	first := heap.Pop(h).(beamItem)
	if first.id != "high" || first.score != 5.0 {
		t.Errorf("first pop: want high(5.0), got %s(%f)", first.id, first.score)
	}

	second := heap.Pop(h).(beamItem)
	if second.id != "mid" || second.score != 3.0 {
		t.Errorf("second pop: want mid(3.0), got %s(%f)", second.id, second.score)
	}

	third := heap.Pop(h).(beamItem)
	if third.id != "low" || third.score != 1.0 {
		t.Errorf("third pop: want low(1.0), got %s(%f)", third.id, third.score)
	}
}

func TestBeamHeap_Empty(t *testing.T) {
	h := &beamHeap{}
	heap.Init(h)
	if h.Len() != 0 {
		t.Errorf("empty heap: want len 0, got %d", h.Len())
	}
}

func TestBeamHeap_DuplicateScores(t *testing.T) {
	h := &beamHeap{}
	heap.Init(h)

	heap.Push(h, beamItem{id: "a", score: 3.0, depth: 0})
	heap.Push(h, beamItem{id: "b", score: 3.0, depth: 0})

	if h.Len() != 2 {
		t.Errorf("want 2 items, got %d", h.Len())
	}

	// Both should pop successfully
	first := heap.Pop(h).(beamItem)
	second := heap.Pop(h).(beamItem)
	if first.score != 3.0 || second.score != 3.0 {
		t.Errorf("both should have score 3.0: got %f, %f", first.score, second.score)
	}
}

func TestGetTraversalParams_KnownIntents(t *testing.T) {
	for _, intent := range []Intent{IntentWhy, IntentWhen, IntentEntity, IntentGeneral} {
		p := getTraversalParams(intent)
		if p.BeamWidth <= 0 || p.MaxDepth <= 0 || p.MaxVisited <= 0 {
			t.Errorf("getTraversalParams(%q): got invalid params %+v", intent, p)
		}
	}
}

func TestGetTraversalParams_WhyHasLargerBeam(t *testing.T) {
	why := getTraversalParams(IntentWhy)
	general := getTraversalParams(IntentGeneral)
	if why.BeamWidth <= general.BeamWidth {
		t.Errorf("WHY should have larger beam width: WHY=%d, GENERAL=%d",
			why.BeamWidth, general.BeamWidth)
	}
}

func TestGetTraversalParams_UnknownFallback(t *testing.T) {
	p := getTraversalParams(Intent("UNKNOWN"))
	general := getTraversalParams(IntentGeneral)
	if p != general {
		t.Errorf("unknown intent should fall back to GENERAL params")
	}
}
