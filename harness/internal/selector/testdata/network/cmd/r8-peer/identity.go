package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

func runKeygen(args []string) error {
	options, err := parseCommon("keygen", args, func(flags *flag.FlagSet, options *commonOptions) {
		flags.StringVar(&options.stateDir, "state-dir", "", "private selector state directory")
	})
	if err != nil {
		return err
	}
	if err := requireValues(options.stateDir); err != nil {
		return err
	}
	if err := ensurePrivateDirectory(options.stateDir); err != nil {
		return err
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate peer identity: %w", err)
	}
	path := filepath.Join(options.stateDir, privateKeyName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create private key: %w", err)
	}
	if _, err := file.Write(private); err != nil {
		_ = file.Close()
		return fmt.Errorf("write private key: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync private key: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close private key: %w", err)
	}
	participantID, err := participantIDForPublicKey(public)
	if err != nil {
		return err
	}
	if err := writePrivateFile(filepath.Join(options.stateDir, participantIDName),
		[]byte(participantID.String()+"\n")); err != nil {
		return fmt.Errorf("write participant ID: %w", err)
	}
	return writeJSON(os.Stdout, keyOutput{ParticipantID: participantID.String(),
		PublicKey: base64.StdEncoding.EncodeToString(public)})
}

type keyOutput struct {
	ParticipantID string `json:"participant_id"`
	PublicKey     string `json:"public_key"`
}

func participantIDForPublicKey(public ed25519.PublicKey) (selector.ParticipantID, error) {
	if len(public) != ed25519.PublicKeySize {
		return selector.ParticipantID{}, errors.New("public key is malformed")
	}
	return selector.NewParticipantID("ed25519:" + agency.Sum(public).String())
}

func ensurePrivateDirectory(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("state directory must be an absolute clean path")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("state directory must be a private real directory")
	}
	return nil
}

func loadPrivateKey(stateDirectory string) (ed25519.PrivateKey, error) {
	path := filepath.Join(stateDirectory, privateKeyName)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("private key must be an owner-only regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) != ed25519.PrivateKeySize {
		return nil, errors.New("private key is unavailable or malformed")
	}
	return ed25519.PrivateKey(append([]byte(nil), raw...)), nil
}
