package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var tokenPrincipal string

var tokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Manage local access credentials",
}

var tokenRotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Rotate a principal's bearer token (revocation = rotation: the old value dies with it)",
	RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(tokenPrincipal) == "" {
			return fmt.Errorf("token rotate requires --principal")
		}
		path, err := rotateToken(projectRoot(), tokenPrincipal)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "token rotated for %s (%s)\n", tokenPrincipal, path)
		fmt.Fprintln(cmd.OutOrStdout(), "restart `local run` to apply: tokens load at boot")
		return nil
	},
}

func init() {
	tokenRotateCmd.Flags().StringVar(&tokenPrincipal, "principal", "", "principal whose token to rotate")
	tokenCmd.AddCommand(tokenRotateCmd)
	tokenCmd.GroupID = groupSpine
	rootCmd.AddCommand(tokenCmd)
}

// rotateToken force-writes a fresh bearer token for the principal's credential_ref as recorded in
// bindings.json (the ONLY rotation target — the legacy tokens dir is not consulted). It cannot
// reuse app's writeTokenFile, which is deliberately idempotent (a setup rerun must never lock a
// running server out); rotation is the one EXPLICIT overwrite. Same convention as setup: 24
// random bytes, hex + newline, 0600. Revocation = rotation: the old value is invalid after the
// next `local run` restart (tokens load at boot).
func rotateToken(root, principal string) (string, error) {
	bindingsPath := filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json")
	raw, err := os.ReadFile(bindingsPath)
	if err != nil {
		return "", fmt.Errorf("read bindings: %w", err)
	}
	// Local doc struct mirroring the binding file's relevant fields (precedent: sync.go's
	// credential parsing) — channel exports no credential_ref accessor by design.
	var doc struct {
		Bindings []struct {
			Principal     string `json:"principal"`
			CredentialRef string `json:"credential_ref"`
		} `json:"bindings"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", fmt.Errorf("parse bindings: %w", err)
	}
	for _, b := range doc.Bindings {
		if b.Principal != principal {
			continue
		}
		if strings.TrimSpace(b.CredentialRef) == "" {
			return "", fmt.Errorf("principal %q has no credential_ref (trusted-header binding; nothing to rotate)", principal)
		}
		path := b.CredentialRef
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, filepath.FromSlash(b.CredentialRef))
		}
		if _, err := os.Stat(path); err != nil {
			return "", fmt.Errorf("token file %s: %w", b.CredentialRef, err)
		}
		buf := make([]byte, 24)
		if _, err := rand.Read(buf); err != nil {
			return "", fmt.Errorf("generate token: %w", err)
		}
		if err := os.WriteFile(path, []byte(hex.EncodeToString(buf)+"\n"), 0o600); err != nil {
			return "", err
		}
		return b.CredentialRef, nil
	}
	return "", fmt.Errorf("no binding for principal %q in %s", principal, bindingsPath)
}
