package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"sort"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestConfigFreezesSortedRosterAndProfile(t *testing.T) {
	createdAt := time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC)
	wire := testConfigFile(t, createdAt)
	config, err := wire.runtime()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(config.descriptor.ParticipantRoster()); got != 5 {
		t.Fatalf("roster size = %d, want 5", got)
	}
	if got := config.descriptor.Profile().SampleSize(); got != 1 {
		t.Fatalf("sample size = %d, want 1", got)
	}
	wire.Peers[0], wire.Peers[1] = wire.Peers[1], wire.Peers[0]
	if _, err := wire.runtime(); err == nil {
		t.Fatal("unsorted frozen roster was accepted")
	}
}

func TestConfigBindsParticipantIDToPublicKey(t *testing.T) {
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peers := []peerConfig{{ID: "peer-claimed", Address: "peer-a:8448",
		PublicKey: base64.StdEncoding.EncodeToString(public)}}
	if _, _, err := parsePeers(peers); err == nil {
		t.Fatal("roster accepted a participant ID unrelated to its public key")
	}
	derived, err := participantIDForPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	peers[0].ID = derived.String()
	if _, _, err := parsePeers(peers); err != nil {
		t.Fatalf("roster rejected its key-derived participant ID: %v", err)
	}
}

func testConfigFile(t testing.TB, createdAt time.Time) configFile {
	t.Helper()
	peers := make([]peerConfig, 5)
	for index := range peers {
		public, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		id, err := participantIDForPublicKey(public)
		if err != nil {
			t.Fatal(err)
		}
		peers[index] = peerConfig{ID: id.String(),
			Address:   "peer-" + string(rune('a'+index)) + ":8448",
			PublicKey: base64.StdEncoding.EncodeToString(public)}
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	return configFile{Version: configVersion,
		QuestionDigest:   agency.Sum([]byte("question")).String(),
		CandidateADigest: agency.Sum([]byte("candidate-a")).String(),
		CandidateBDigest: agency.Sum([]byte("candidate-b")).String(),
		CreatedAt:        createdAt.Format(time.RFC3339Nano),
		ExpiresAt:        createdAt.Add(time.Hour).Format(time.RFC3339Nano),
		Profile: profileFile{SampleSize: 1, Alpha: 1, Threshold: 1,
			MaxRounds: 2, RoundTimeoutMilli: 2_000},
		Peers: peers}
}
