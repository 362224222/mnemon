package main

import (
	"strings"
	"testing"
)

func TestCommittedManifestChainRejectsRaiseThenLower(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	base := commitTestRepository(t, root, "base")
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	baseline := validBaselineManifest()
	baseline.SourceCommit = base
	baseline.Entries = []baselineEntry{entry}
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "bootstrap")
	baseline.Entries[0].Ceiling = 100
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "illegal raise")
	baseline.Entries[0].Ceiling = 90
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "hide raise")
	if err := validateCommittedManifestChain(root, base); err == nil || !strings.Contains(err.Error(), "raises") {
		t.Fatalf("chain error = %v", err)
	}
}

func TestContractBundlesRejectBaselineToExceptionConversion(t *testing.T) {
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	prior := contractBundle{baseline: validBaselineManifest(), exceptions: exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}}, architecture: architectureManifest{SchemaVersion: 1, SourceCommit: strings.Repeat("a", 40), Entries: []architectureEntry{}}}
	prior.baseline.Entries = []baselineEntry{entry}
	current := prior
	current.baseline.Entries = []baselineEntry{}
	current.exceptions.Entries = []exceptionEntry{{
		Rule: entry.Rule, Identity: entry.Identity, Path: entry.Path, Symbol: entry.Symbol, Ceiling: 90,
		Reason: "move debt", Risk: "medium", Owner: "team", RemovalCheckpoint: "7R",
	}}
	if err := compareContractBundles(prior, current); err == nil || !strings.Contains(err.Error(), "cannot move") {
		t.Fatalf("conversion error = %v", err)
	}
}

func TestCommittedManifestLedgerRejectsReclassificationAcrossThreeCommitGap(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	anchor := commitTestRepository(t, root, "base")
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	baseline := validBaselineManifest()
	baseline.SourceCommit = anchor
	baseline.Entries = []baselineEntry{entry}
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	commitTestRepository(t, root, "bootstrap")

	baseline.Entries = []baselineEntry{}
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	commitTestRepository(t, root, "remove baseline debt")
	commitHistoryGaps(t, root, 3)

	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 90)}}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	commitTestRepository(t, root, "reclassify removed debt")
	if err := validateCommittedManifestChain(root, anchor); err == nil || !strings.Contains(err.Error(), "historical baseline identity") {
		t.Fatalf("lifetime reclassification error = %v", err)
	}
}

func TestCommittedManifestLedgerRejectsExceptionResurrectionAcrossThreeCommitGap(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	anchor := commitTestRepository(t, root, "base")
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	baseline := validBaselineManifest()
	baseline.SourceCommit = anchor
	baseline.Entries = []baselineEntry{}
	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 90)}}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	commitTestRepository(t, root, "bootstrap")

	exceptions.Entries = []exceptionEntry{}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	commitTestRepository(t, root, "remove exception")
	commitHistoryGaps(t, root, 3)

	exceptions.Entries = []exceptionEntry{ratchetException(entry, 91)}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	commitTestRepository(t, root, "raise restored exception")
	if err := validateCommittedManifestChain(root, anchor); err == nil || !strings.Contains(err.Error(), "cannot be resurrected") {
		t.Fatalf("lifetime exception resurrection error = %v", err)
	}
}

func TestManifestHistoryUsesLifetimeAnchorBeforeCurrentPRBase(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	anchor := commitTestRepository(t, root, "base")
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	baseline := validBaselineManifest()
	baseline.SourceCommit = anchor
	baseline.Entries = []baselineEntry{entry}
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	commitTestRepository(t, root, "bootstrap")

	baseline.Entries = []baselineEntry{}
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	currentPRBase := commitTestRepository(t, root, "previous PR removes debt")
	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 90)}}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	architecture := architectureManifest{SchemaVersion: 1, SourceCommit: anchor, Entries: []architectureEntry{}}

	if err := compareManifestHistory(root, currentPRBase, baseline, exceptions, architecture); err == nil || !strings.Contains(err.Error(), "historical baseline identity") {
		t.Fatalf("cross-PR lifetime error = %v", err)
	}
}

func TestManifestDAGAllowsDisjointSiblingRemovalsAtMerge(t *testing.T) {
	root, anchor, bootstrap, baseline := bootstrapBaselineHistory(t, ratchetMetricEntry("x"), ratchetMetricEntry("y"))
	x, y := baseline.Entries[0], baseline.Entries[1]
	checkoutTestBranch(t, root, "remove-x", bootstrap)
	baseline.Entries = []baselineEntry{y}
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "remove x")

	checkoutTestBranch(t, root, "remove-y", bootstrap)
	baseline.Entries = []baselineEntry{x}
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "remove y")

	checkoutTestReference(t, root, "remove-x")
	beginTestMerge(t, root, "remove-y")
	baseline.Entries = []baselineEntry{}
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "merge both removals")
	if err := validateCommittedManifestChain(root, anchor); err != nil {
		t.Fatalf("disjoint removal merge: %v", err)
	}
}

func TestManifestDAGRejectsMergeResurrectionFromSecondParent(t *testing.T) {
	root, anchor, bootstrap, baseline := bootstrapBaselineHistory(t, ratchetMetricEntry("x"))
	original := append([]baselineEntry(nil), baseline.Entries...)
	checkoutTestBranch(t, root, "removed", bootstrap)
	baseline.Entries = []baselineEntry{}
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "remove x")

	checkoutTestBranch(t, root, "retained", bootstrap)
	writeTestFile(t, root, "retained.txt", "side branch\n")
	commitTestRepository(t, root, "retain old manifest")

	checkoutTestReference(t, root, "removed")
	beginTestMerge(t, root, "retained")
	baseline.Entries = original
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "resurrect from second parent")
	if err := validateCommittedManifestChain(root, anchor); err == nil || !strings.Contains(err.Error(), "adds identity") {
		t.Fatalf("merge resurrection error = %v", err)
	}
}

func TestManifestDAGMergesExceptionCeilingsAtHistoricalMinimum(t *testing.T) {
	for _, test := range []struct {
		name         string
		mergeCeiling int
		wantError    bool
	}{
		{name: "reject one hundred", mergeCeiling: 100, wantError: true},
		{name: "accept ninety", mergeCeiling: 90, wantError: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := initTestRepository(t)
			writeTestFile(t, root, "README.md", "base\n")
			anchor := commitTestRepository(t, root, "base")
			entry := ratchetMetricEntry("exception")
			baseline := validBaselineManifest()
			baseline.SourceCommit = anchor
			baseline.Entries = []baselineEntry{}
			writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 100)}})
			bootstrap := commitTestRepository(t, root, "bootstrap")

			checkoutTestBranch(t, root, "lower", bootstrap)
			writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 90)}})
			commitTestRepository(t, root, "lower exception")
			checkoutTestBranch(t, root, "retain", bootstrap)
			writeTestFile(t, root, "retain.txt", "retain\n")
			commitTestRepository(t, root, "retain exception")

			checkoutTestReference(t, root, "lower")
			beginTestMerge(t, root, "retain")
			writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, test.mergeCeiling)}})
			commitTestRepository(t, root, "merge exception ceilings")
			err := validateCommittedManifestChain(root, anchor)
			if test.wantError && (err == nil || !strings.Contains(err.Error(), "raises ceiling")) {
				t.Fatalf("ceiling merge error = %v", err)
			}
			if !test.wantError && err != nil {
				t.Fatalf("ceiling merge: %v", err)
			}
		})
	}
}

func TestManifestDAGRejectsExceptionKeptAfterSiblingRemoval(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	anchor := commitTestRepository(t, root, "base")
	entry := ratchetMetricEntry("exception")
	baseline := validBaselineManifest()
	baseline.SourceCommit = anchor
	baseline.Entries = []baselineEntry{}
	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 90)}}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	bootstrap := commitTestRepository(t, root, "bootstrap")

	checkoutTestBranch(t, root, "remove-exception", bootstrap)
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	commitTestRepository(t, root, "remove exception")
	checkoutTestBranch(t, root, "keep-exception", bootstrap)
	writeTestFile(t, root, "keep.txt", "keep\n")
	commitTestRepository(t, root, "keep exception")

	checkoutTestReference(t, root, "remove-exception")
	beginTestMerge(t, root, "keep-exception")
	writeRatchetContractBundle(t, root, baseline, exceptions)
	commitTestRepository(t, root, "resurrect exception at merge")
	if err := validateCommittedManifestChain(root, anchor); err == nil || !strings.Contains(err.Error(), "cannot be resurrected") {
		t.Fatalf("exception merge resurrection error = %v", err)
	}
}

func TestManifestHistoryAcceptsRawHeadAgainstAdvancedUnrelatedBase(t *testing.T) {
	root, anchor, bootstrap, baseline := bootstrapBaselineHistory(t, ratchetMetricEntry("x"))
	checkoutTestBranch(t, root, "advanced-base", bootstrap)
	writeTestFile(t, root, "base.txt", "advanced base\n")
	base := commitTestRepository(t, root, "advance base")

	checkoutTestBranch(t, root, "raw-head", bootstrap)
	baseline.Entries = []baselineEntry{}
	writeRatchetBundle(t, root, baseline)
	commitTestRepository(t, root, "remove x on raw head")
	architecture := architectureManifest{SchemaVersion: 1, SourceCommit: anchor, Entries: []architectureEntry{}}
	if err := compareManifestHistory(root, base, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}}, architecture); err != nil {
		t.Fatalf("advanced base/raw head: %v", err)
	}
}

func TestManifestHistoryRejectsRawHeadDebtRemovedOnAdvancedBase(t *testing.T) {
	root, anchor, bootstrap, baseline := bootstrapBaselineHistory(t, ratchetMetricEntry("x"))
	checkoutTestBranch(t, root, "advanced-base", bootstrap)
	baseBaseline := baseline
	baseBaseline.Entries = []baselineEntry{}
	writeRatchetBundle(t, root, baseBaseline)
	base := commitTestRepository(t, root, "remove x on base")

	checkoutTestBranch(t, root, "stale-head", bootstrap)
	writeTestFile(t, root, "head.txt", "stale head\n")
	commitTestRepository(t, root, "retain x on raw head")
	architecture := architectureManifest{SchemaVersion: 1, SourceCommit: anchor, Entries: []architectureEntry{}}
	if err := compareManifestHistory(root, base, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}}, architecture); err == nil || !strings.Contains(err.Error(), "not monotone relative") {
		t.Fatalf("stale raw head error = %v", err)
	}
}

func TestManifestHistoryRejectsRawHeadExceptionRemovedOnAdvancedBase(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	anchor := commitTestRepository(t, root, "base")
	entry := ratchetMetricEntry("exception")
	baseline := validBaselineManifest()
	baseline.SourceCommit = anchor
	baseline.Entries = []baselineEntry{}
	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{ratchetException(entry, 90)}}
	writeRatchetContractBundle(t, root, baseline, exceptions)
	bootstrap := commitTestRepository(t, root, "bootstrap")

	checkoutTestBranch(t, root, "advanced-base", bootstrap)
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	base := commitTestRepository(t, root, "remove exception on base")
	checkoutTestBranch(t, root, "stale-head", bootstrap)
	writeTestFile(t, root, "head.txt", "stale exception\n")
	commitTestRepository(t, root, "retain exception on raw head")
	architecture := architectureManifest{SchemaVersion: 1, SourceCommit: anchor, Entries: []architectureEntry{}}
	if err := compareManifestHistory(root, base, baseline, exceptions, architecture); err == nil || !strings.Contains(err.Error(), "cannot be resurrected") {
		t.Fatalf("stale raw head exception error = %v", err)
	}
}

func writeRatchetBundle(t *testing.T, root string, baseline baselineManifest) {
	t.Helper()
	writeRatchetContractBundle(t, root, baseline, exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
}

func writeRatchetContractBundle(t *testing.T, root string, baseline baselineManifest, exceptions exceptionManifest) {
	t.Helper()
	writeCanonicalTestFile(t, root, baselinePath, baseline)
	writeCanonicalTestFile(t, root, exceptionsPath, exceptions)
	writeCanonicalTestFile(t, root, architecturePath, architectureManifest{SchemaVersion: 1, SourceCommit: baseline.SourceCommit, Entries: []architectureEntry{}})
}

func ratchetException(entry baselineEntry, ceiling int) exceptionEntry {
	return exceptionEntry{
		Rule: entry.Rule, Identity: entry.Identity, Path: entry.Path, Symbol: entry.Symbol, Ceiling: ceiling,
		Reason: "temporary reviewed debt", Risk: "medium", Owner: "quality", RemovalCheckpoint: "7R",
	}
}

func commitHistoryGaps(t *testing.T, root string, count int) {
	t.Helper()
	for index := 1; index <= count; index++ {
		writeTestFile(t, root, "README.md", strings.Repeat("gap\n", index))
		commitTestRepository(t, root, "unrelated gap")
	}
}

func bootstrapBaselineHistory(t *testing.T, entries ...baselineEntry) (string, string, string, baselineManifest) {
	t.Helper()
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	anchor := commitTestRepository(t, root, "base")
	baseline := validBaselineManifest()
	baseline.SourceCommit = anchor
	baseline.Entries = append([]baselineEntry(nil), entries...)
	writeRatchetBundle(t, root, baseline)
	bootstrap := commitTestRepository(t, root, "bootstrap")
	return root, anchor, bootstrap, baseline
}

func ratchetMetricEntry(name string) baselineEntry {
	path := "harness/" + name + ".go"
	symbol := strings.ToUpper(name[:1]) + name[1:]
	return baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, path, symbol), Path: path, Symbol: symbol, Ceiling: 90}
}

func checkoutTestBranch(t *testing.T, root, name, start string) {
	t.Helper()
	if _, err := runGit(root, "checkout", "-b", name, start); err != nil {
		t.Fatal(err)
	}
}

func checkoutTestReference(t *testing.T, root, reference string) {
	t.Helper()
	if _, err := runGit(root, "checkout", reference); err != nil {
		t.Fatal(err)
	}
}

func beginTestMerge(t *testing.T, root, reference string) {
	t.Helper()
	_, _ = runGit(root, "merge", "--no-ff", "--no-commit", reference)
	if _, err := runGit(root, "rev-parse", "--verify", "MERGE_HEAD"); err != nil {
		t.Fatalf("merge %s did not enter a merge state: %v", reference, err)
	}
}
