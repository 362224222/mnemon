package corecontract

import (
	"strings"
	"testing"
)

func TestGoTestOracleRequiresOneActualPassAndRejectsSkipOrDuplicate(t *testing.T) {
	ref := "./internal/agency::TestProof"
	required := map[string]struct{}{ref: {}}
	pass := `{"Action":"pass","Package":"example/harness/internal/agency","Test":"TestProof"}` + "\n"
	got, err := parseGoTestJSON([]byte(pass), required)
	if err != nil || len(got) != 1 || got[0] != ref {
		t.Fatalf("pass = %v, %v", got, err)
	}
	for name, output := range map[string]string{
		"skip":      `{"Action":"skip","Package":"example/harness/internal/agency","Test":"TestProof"}` + "\n",
		"duplicate": pass + pass,
		"unknown":   `not-json` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseGoTestJSON([]byte(output), required); err == nil {
				t.Fatalf("output unexpectedly passed")
			}
		})
	}
}

func TestShellOracleRequiresOneExactStdoutLine(t *testing.T) {
	step := GateStep{Kind: "shell", Oracles: []string{"stdout:proof passed"}}
	if _, err := verifyStepOracles(step, []byte("prefix proof passed\nproof passed\n")); err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{"prefix proof passed\n", "proof passed\nproof passed\n"} {
		if _, err := verifyStepOracles(step, []byte(output)); err == nil ||
			!strings.Contains(err.Error(), "exactly one") {
			t.Fatalf("output %q error = %v", output, err)
		}
	}
}
