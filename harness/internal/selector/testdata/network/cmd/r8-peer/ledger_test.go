package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestAttemptLedgerPersistsQueryOnceAcrossRestart(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := openAttemptLedger(directory, 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := ledger.claim("out:peer-b", "selection/1/nonce")
	if err != nil || !fresh {
		t.Fatalf("first claim = fresh:%t error:%v", fresh, err)
	}
	fresh, err = ledger.claim("out:peer-b", "selection/1/nonce")
	if err != nil || fresh {
		t.Fatalf("same-process replay = fresh:%t error:%v", fresh, err)
	}

	reopened, err := openAttemptLedger(directory, 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err = reopened.claim("out:peer-b", "selection/1/nonce")
	if err != nil || fresh {
		t.Fatalf("restart replay = fresh:%t error:%v", fresh, err)
	}
}

func TestAttemptLedgerBoundsDistinctQueriesPerPeer(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := openAttemptLedger(directory, 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1; index++ {
		fresh, claimErr := ledger.claim("in:peer-a", string(rune('a'+index)))
		if claimErr != nil || !fresh {
			t.Fatalf("claim %d = fresh:%t error:%v", index, fresh, claimErr)
		}
	}
	if fresh, err := ledger.claim("in:peer-a", "overflow"); err == nil || fresh {
		t.Fatalf("per-peer overflow = fresh:%t error:%v", fresh, err)
	}
}

func TestAttemptLedgerUsesFrozenRoundBoundWithoutFixtureClamp(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	const rounds = 65
	ledger, err := openAttemptLedger(directory, 5, rounds)
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= rounds; round++ {
		fresh, claimErr := ledger.claim("out:peer-b", fmt.Sprintf("selection/%d/nonce", round))
		if claimErr != nil || !fresh {
			t.Fatalf("claim %d = fresh:%t error:%v", round, fresh, claimErr)
		}
	}
	if fresh, err := ledger.claim("out:peer-b", "selection/66/nonce"); err == nil || fresh {
		t.Fatalf("round overflow = fresh:%t error:%v", fresh, err)
	}
}
