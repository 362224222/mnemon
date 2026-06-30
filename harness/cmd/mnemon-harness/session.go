package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	sessionstore "github.com/mnemon-dev/mnemon/harness/internal/session"
	"github.com/spf13/cobra"
)

var (
	sessionRoot             string
	sessionStartID          string
	sessionStartTitle       string
	sessionStartCarrier     string
	sessionStartDupPolicy   string
	sessionAttachID         string
	sessionAttachSurface    string
	sessionAttachExternal   string
	sessionAttachSetPrimary bool
)

var sessionCmd = &cobra.Command{
	Use:   "session",
	Short: "Start and attach harness sessions",
}

var sessionStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a harness session record",
	RunE:  runSessionStart,
}

var sessionAttachCmd = &cobra.Command{
	Use:   "attach",
	Short: "Attach an external reference to a harness session",
	RunE:  runSessionAttach,
}

func init() {
	sessionCmd.PersistentFlags().StringVar(&sessionRoot, "root", ".", "project root")
	sessionStartCmd.Flags().StringVar(&sessionStartID, "id", "", "session id; generated when empty")
	sessionStartCmd.Flags().StringVar(&sessionStartTitle, "title", "", "session title")
	sessionStartCmd.Flags().StringVar(&sessionStartCarrier, "primary-carrier", "", "primary activation carrier")
	sessionStartCmd.Flags().StringVar(&sessionStartDupPolicy, "duplicate-activation-policy", "", "duplicate activation policy")
	sessionAttachCmd.Flags().StringVar(&sessionAttachID, "id", "", "session id")
	sessionAttachCmd.Flags().StringVar(&sessionAttachSurface, "surface", "", "external surface name")
	sessionAttachCmd.Flags().StringVar(&sessionAttachExternal, "external-ref", "", "external reference to attach")
	sessionAttachCmd.Flags().BoolVar(&sessionAttachSetPrimary, "primary", false, "make this attachment the primary activation carrier")
	sessionCmd.AddCommand(sessionStartCmd, sessionAttachCmd)
	sessionCmd.GroupID = groupSpine
	rootCmd.AddCommand(sessionCmd)
}

func runSessionStart(cmd *cobra.Command, args []string) error {
	root := sessionProjectRoot()
	cfg := sessionConfigDefaults(root)
	id := strings.TrimSpace(sessionStartID)
	if id == "" {
		id = "session-" + time.Now().UTC().Format("20060102T150405Z")
	}
	carrier := firstSessionValue(sessionStartCarrier, cfg.Sessions.PrimaryActivationCarrier, "local")
	dupPolicy := firstSessionValue(sessionStartDupPolicy, cfg.Sessions.DuplicateActivationPolicy, productconfig.DuplicateActivationSuppress)
	record, err := sessionstore.NewFileStore(sessionstore.DefaultDir(root, ""), nil).Start(sessionstore.Record{
		ID:                        id,
		Title:                     sessionStartTitle,
		PrimaryActivationCarrier:  carrier,
		DuplicateActivationPolicy: dupPolicy,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Session: %s\n", record.ID)
	fmt.Fprintf(cmd.OutOrStdout(), "Primary activation carrier: %s\n", record.PrimaryActivationCarrier)
	fmt.Fprintf(cmd.OutOrStdout(), "Duplicate activation policy: %s\n", record.DuplicateActivationPolicy)
	return nil
}

func runSessionAttach(cmd *cobra.Command, args []string) error {
	root := sessionProjectRoot()
	if strings.TrimSpace(sessionAttachID) == "" {
		return fmt.Errorf("--id is required")
	}
	record, err := sessionstore.NewFileStore(sessionstore.DefaultDir(root, ""), nil).Attach(sessionAttachID, sessionstore.Attachment{
		Surface:     sessionAttachSurface,
		ExternalRef: sessionAttachExternal,
		Primary:     sessionAttachSetPrimary,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Session: %s\n", record.ID)
	fmt.Fprintf(cmd.OutOrStdout(), "Attachments: %d\n", len(record.Attachments))
	fmt.Fprintf(cmd.OutOrStdout(), "Primary activation carrier: %s\n", record.PrimaryActivationCarrier)
	return nil
}

func sessionProjectRoot() string {
	root := strings.TrimSpace(sessionRoot)
	if root == "" {
		return "."
	}
	return root
}

func sessionConfigDefaults(root string) productconfig.Config {
	if cfg, err := productconfig.Load(productconfig.DefaultPath(root, "")); err == nil {
		return cfg
	}
	if cfg, found, err := productconfig.FromLegacy(root); err == nil && found {
		return cfg
	}
	return productconfig.Default()
}

func firstSessionValue(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
