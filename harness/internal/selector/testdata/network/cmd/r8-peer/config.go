package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

const (
	configVersion     = 1
	maxConfigBytes    = 32 << 10
	privateKeyName    = "identity.key"
	participantIDName = "participant.id"
	databaseName      = "selector.db"
)

type configFile struct {
	Version          uint32       `json:"version"`
	QuestionDigest   string       `json:"question_digest"`
	CandidateADigest string       `json:"candidate_a_digest"`
	CandidateBDigest string       `json:"candidate_b_digest"`
	CreatedAt        string       `json:"created_at"`
	ExpiresAt        string       `json:"expires_at"`
	Profile          profileFile  `json:"profile"`
	Peers            []peerConfig `json:"peers"`
}

type profileFile struct {
	SampleSize        uint32 `json:"sample_size"`
	Alpha             uint32 `json:"alpha"`
	Threshold         uint32 `json:"threshold"`
	MaxRounds         uint32 `json:"max_rounds"`
	RoundTimeoutMilli int64  `json:"round_timeout_ms"`
}

type peerConfig struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	PublicKey string `json:"public_key"`
}

type runtimeConfig struct {
	descriptor selector.SelectionDescriptor
	peers      map[string]peerRuntime
}

type peerRuntime struct {
	id      selector.ParticipantID
	address string
	key     ed25519.PublicKey
}

func loadConfig(path string) (runtimeConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxConfigBytes {
		return runtimeConfig{}, errors.New("config is empty, unreadable, or over its bound")
	}
	return parseConfig(raw)
}

func parseConfig(raw []byte) (runtimeConfig, error) {
	var wire configFile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return runtimeConfig{}, fmt.Errorf("decode config: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return runtimeConfig{}, err
	}
	return wire.runtime()
}

func runInstallConfig(args []string) error {
	options, err := parseCommon("install-config", args,
		func(flags *flag.FlagSet, options *commonOptions) {
			flags.StringVar(&options.stateDir, "state-dir", "", "private selector state directory")
		})
	if err != nil {
		return err
	}
	if err := requireValues(options.stateDir); err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, maxConfigBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxConfigBytes {
		return errors.New("config stdin is empty, unreadable, or over its bound")
	}
	if _, err := parseConfig(raw); err != nil {
		return err
	}
	var wire configFile
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	canonical, err := json.Marshal(wire)
	if err != nil {
		return err
	}
	return writePrivateFile(filepath.Join(options.stateDir, "config.json"), canonical)
}

func writePrivateFile(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func (wire configFile) runtime() (runtimeConfig, error) {
	if wire.Version != configVersion {
		return runtimeConfig{}, fmt.Errorf("config version %d is unsupported", wire.Version)
	}
	question, err := agency.ParseDigest(wire.QuestionDigest)
	if err != nil {
		return runtimeConfig{}, err
	}
	candidateA, err := agency.ParseDigest(wire.CandidateADigest)
	if err != nil {
		return runtimeConfig{}, err
	}
	candidateB, err := agency.ParseDigest(wire.CandidateBDigest)
	if err != nil {
		return runtimeConfig{}, err
	}
	profile, err := selector.NewProfile(wire.Profile.SampleSize, wire.Profile.Alpha,
		wire.Profile.Threshold, wire.Profile.MaxRounds,
		time.Duration(wire.Profile.RoundTimeoutMilli)*time.Millisecond)
	if err != nil {
		return runtimeConfig{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("parse creation time: %w", err)
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, wire.ExpiresAt)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("parse expiry time: %w", err)
	}
	peers, roster, err := parsePeers(wire.Peers)
	if err != nil {
		return runtimeConfig{}, err
	}
	descriptor, err := selector.NewSelectionDescriptor(question, candidateA, candidateB,
		roster, profile, createdAt, expiresAt)
	if err != nil {
		return runtimeConfig{}, err
	}
	return runtimeConfig{descriptor: descriptor, peers: peers}, nil
}

func parsePeers(values []peerConfig) (map[string]peerRuntime, []selector.ParticipantID, error) {
	if len(values) == 0 || !sort.SliceIsSorted(values, func(i, j int) bool { return values[i].ID < values[j].ID }) {
		return nil, nil, errors.New("peer roster must be non-empty and sorted by ID")
	}
	peers := make(map[string]peerRuntime, len(values))
	roster := make([]selector.ParticipantID, len(values))
	for index, value := range values {
		if value.Address == "" || value.PublicKey == "" || (index > 0 && values[index-1].ID == value.ID) {
			return nil, nil, errors.New("peer roster contains an incomplete or duplicate entry")
		}
		host, portValue, err := net.SplitHostPort(value.Address)
		port, portErr := strconv.Atoi(portValue)
		if err != nil || portErr != nil || host == "" || port < 1 || port > 65_535 {
			return nil, nil, fmt.Errorf("peer %s has an invalid network address", value.ID)
		}
		key, err := base64.StdEncoding.DecodeString(value.PublicKey)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return nil, nil, fmt.Errorf("peer %s has an invalid public key", value.ID)
		}
		id, err := participantIDForPublicKey(ed25519.PublicKey(key))
		if err != nil {
			return nil, nil, err
		}
		if value.ID != id.String() {
			return nil, nil, fmt.Errorf("peer %s ID is not derived from its public key", value.ID)
		}
		roster[index] = id
		peers[value.ID] = peerRuntime{id: id, address: value.Address,
			key: ed25519.PublicKey(append([]byte(nil), key...))}
	}
	return peers, roster, nil
}

func (config runtimeConfig) peer(value string) (peerRuntime, error) {
	peer, ok := config.peers[value]
	if !ok {
		return peerRuntime{}, fmt.Errorf("peer %q is outside the frozen roster", value)
	}
	return peer, nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON input has trailing data")
	}
	return nil
}

func writeJSON(destination io.Writer, value any) error {
	encoder := json.NewEncoder(destination)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}
