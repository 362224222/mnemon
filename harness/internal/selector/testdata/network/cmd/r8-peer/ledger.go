package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

const (
	attemptLedgerVersion  = 1
	maxAttemptLedgerBytes = 2 << 20
)

type attemptLedger struct {
	mu           sync.Mutex
	path         string
	maximum      int
	perBucket    int
	entries      map[string]struct{}
	bucketCounts map[string]int
}

type attemptLedgerWire struct {
	Version uint32   `json:"version"`
	Entries []string `json:"entries"`
}

func openAttemptLedger(stateDirectory string, peerCount int, maxRounds uint32) (*attemptLedger, error) {
	perBucket := int(maxRounds)
	maximum := peerCount * perBucket * 2
	if maximum > selector.MaxSelectionQueryMessages {
		maximum = selector.MaxSelectionQueryMessages
	}
	ledger := &attemptLedger{path: filepath.Join(stateDirectory, "network-attempts.json"),
		maximum: maximum, perBucket: perBucket, entries: make(map[string]struct{}),
		bucketCounts: make(map[string]int)}
	if err := ledger.load(); err != nil {
		return nil, err
	}
	return ledger, nil
}

// claim persists an attempt before I/O. False means the exact query was
// already attempted; the adapter must not retry it and treats response loss as
// no-vote.
func (ledger *attemptLedger) claim(bucket, value string) (bool, error) {
	if bucket == "" || value == "" {
		return false, errors.New("attempt ledger key is incomplete")
	}
	key := bucket + "|" + agency.Sum([]byte(value)).String()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if _, present := ledger.entries[key]; present {
		return false, nil
	}
	if len(ledger.entries) >= ledger.maximum || ledger.bucketCounts[bucket] >= ledger.perBucket {
		return false, errors.New("network attempt budget exhausted")
	}
	ledger.entries[key] = struct{}{}
	ledger.bucketCounts[bucket]++
	if err := ledger.persist(); err != nil {
		delete(ledger.entries, key)
		ledger.bucketCounts[bucket]--
		return false, err
	}
	return true, nil
}

func (ledger *attemptLedger) load() error {
	raw, err := os.ReadFile(ledger.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || len(raw) == 0 || len(raw) > maxAttemptLedgerBytes {
		return errors.New("network attempt ledger is unavailable or over its bound")
	}
	info, err := os.Lstat(ledger.path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return errors.New("network attempt ledger is not an owner-only regular file")
	}
	var wire attemptLedgerWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || requireEOF(decoder) != nil ||
		wire.Version != attemptLedgerVersion || len(wire.Entries) > ledger.maximum ||
		!sort.StringsAreSorted(wire.Entries) {
		return errors.New("network attempt ledger is malformed")
	}
	for index, entry := range wire.Entries {
		separator := bytes.IndexByte([]byte(entry), '|')
		if separator <= 0 || (index > 0 && wire.Entries[index-1] == entry) {
			return errors.New("network attempt ledger contains an invalid entry")
		}
		bucket := entry[:separator]
		ledger.bucketCounts[bucket]++
		if ledger.bucketCounts[bucket] > ledger.perBucket {
			return errors.New("network attempt ledger exceeds a peer budget")
		}
		ledger.entries[entry] = struct{}{}
	}
	canonical, _ := json.Marshal(wire)
	if !bytes.Equal(raw, canonical) {
		return errors.New("network attempt ledger is not canonical JSON")
	}
	return nil
}

func (ledger *attemptLedger) persist() error {
	entries := make([]string, 0, len(ledger.entries))
	for entry := range ledger.entries {
		entries = append(entries, entry)
	}
	sort.Strings(entries)
	raw, err := json.Marshal(attemptLedgerWire{Version: attemptLedgerVersion, Entries: entries})
	if err != nil {
		return err
	}
	directory := filepath.Dir(ledger.path)
	temporary, err := os.CreateTemp(directory, ".network-attempts-*")
	if err != nil {
		return fmt.Errorf("create attempt ledger: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, ledger.path); err != nil {
		return err
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(directoryHandle.Sync(), directoryHandle.Close())
}
