package corecontract

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestTrackedContractLoadsCanonicalAuthority(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(contract.Requirements) != RequirementCount {
		t.Fatalf("requirements = %d, want %d", len(contract.Requirements), RequirementCount)
	}
	wantGates := []string{
		"G-CONTRACT", "G-DOCKER", "G-EVIDENCE", "G-LIVE", "G-PROCESS", "G-ROOT", "G-UNIT",
	}
	if got := contract.GateIDs(); !slices.Equal(got, wantGates) {
		t.Fatalf("gates = %v, want %v", got, wantGates)
	}
	if err := ValidateOwnerDirectories(root, contract); err != nil {
		t.Fatal(err)
	}
}

func TestParseRejectsDuplicateSectionsRowsAndUnknownGates(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("../../..", filepath.FromSlash(DocumentPath)))
	if err != nil {
		t.Fatal(err)
	}
	requirementPrefix := "| SC-01 | MUST | Root release packages"
	requirementRow := ""
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.HasPrefix(line, requirementPrefix) {
			requirementRow = line
			break
		}
	}
	if requirementRow == "" {
		t.Fatalf("test requirement row %q was not found", requirementPrefix)
	}
	cases := []struct {
		name        string
		old         string
		replacement string
		want        string
	}{
		{
			name:        "requirement section",
			old:         "## 8. Canonical requirements",
			replacement: "## 8. Canonical requirements\n## 8. Canonical requirements",
			want:        "repeats canonical requirements section",
		},
		{
			name:        "gate section",
			old:         "## 9. Closed evidence gates",
			replacement: "## 9. Closed evidence gates\n## 9. Closed evidence gates",
			want:        "repeats closed evidence gates section",
		},
		{
			name:        "requirement row",
			old:         requirementRow,
			replacement: requirementRow + "\n" + requirementRow,
			want:        "repeats requirement SC-01",
		},
		{
			name:        "unknown primary gate",
			old:         "`G-ROOT` static + process",
			replacement: "`G-UNKNOWN` static + process",
			want:        "references unknown primary gate G-UNKNOWN",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			changed := strings.Replace(string(contents), testCase.old, testCase.replacement, 1)
			if changed == string(contents) {
				t.Fatalf("test input %q was not found", testCase.old)
			}
			if _, err := Parse([]byte(changed)); err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Parse error = %v, want containing %q", err, testCase.want)
			}
		})
	}
}
