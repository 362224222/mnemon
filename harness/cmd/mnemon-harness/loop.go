package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/spf13/cobra"
)

var (
	loopRoot         string
	loopCapsJSON     bool
	loopSchemaType   string
	loopObserveWrite string
)

var loopCmd = &cobra.Command{
	Use:    "loop",
	Short:  "Inspect and validate harness capabilities",
	Hidden: true,
}

var loopValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate embedded and external capability packages",
	RunE:  runLoopValidate,
}

var loopAddCmd = &cobra.Command{
	Use:   "add <dir>",
	Short: "Register an external capability package from a directory",
	Args:  cobra.ExactArgs(1),
	RunE:  runLoopAdd,
}

var loopCapabilitiesCmd = &cobra.Command{
	Use:   "capabilities",
	Short: "List the resolvable capability kinds (embedded + external packages)",
	RunE:  runLoopCapabilities,
}

var loopSchemaCmd = &cobra.Command{
	Use:   "schema --type KIND",
	Short: "Show one capability kind's schema (types, required fields, sync)",
	RunE:  runLoopSchema,
}

var loopObserveSkillCmd = &cobra.Command{
	Use:   "observe-skill",
	Short: "Generate the generic mnemon-observe skill from this project's catalog",
	RunE:  runLoopObserveSkill,
}

func init() {
	loopCmd.PersistentFlags().StringVar(&loopRoot, "root", ".", "repository root containing harness declarations")
	loopCapabilitiesCmd.Flags().BoolVar(&loopCapsJSON, "json", false, "emit the capability list as JSON")
	loopSchemaCmd.Flags().StringVar(&loopSchemaType, "type", "", "resource kind to describe")
	loopSchemaCmd.Flags().BoolVar(&loopCapsJSON, "json", false, "emit the schema as JSON")
	loopObserveSkillCmd.Flags().StringVar(&loopObserveWrite, "write", "", "write SKILL.md into this directory instead of stdout")
	loopCmd.AddCommand(loopValidateCmd, loopAddCmd, loopCapabilitiesCmd, loopSchemaCmd, loopObserveSkillCmd)
	loopCmd.GroupID = groupSpine
	rootCmd.AddCommand(loopCmd)
}

func runLoopObserveSkill(cmd *cobra.Command, args []string) error {
	content, err := app.New(loopRoot).RenderObserveSkill()
	if err != nil {
		return err
	}
	if loopObserveWrite == "" {
		fmt.Fprint(cmd.OutOrStdout(), content)
		return nil
	}
	if err := os.MkdirAll(loopObserveWrite, 0o755); err != nil {
		return err
	}
	path := filepath.Join(loopObserveWrite, "SKILL.md")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "wrote %s\n", path)
	return nil
}

func runLoopCapabilities(cmd *cobra.Command, args []string) error {
	infos, err := app.New(loopRoot).LoopCapabilities()
	if err != nil {
		return err
	}
	if loopCapsJSON {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(infos)
	}
	for _, info := range infos {
		sync := "no"
		if info.Importable {
			sync = "import:" + info.Merge
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s (%s) observe=%s required=[%s] sync=%s\n",
			info.Kind, info.Source, info.ObservedType, strings.Join(info.Required, ","), sync)
	}
	return nil
}

func runLoopSchema(cmd *cobra.Command, args []string) error {
	if loopSchemaType == "" {
		return fmt.Errorf("loop schema requires --type KIND")
	}
	info, err := app.New(loopRoot).LoopSchema(loopSchemaType)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(info)
}

func runLoopAdd(cmd *cobra.Command, args []string) error {
	name, err := app.New(loopRoot).LoopAdd(args[0])
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "added loop %q under .mnemon/loops/%s; enable it with: mnemon-harness setup --host HOST --loop %s\n", name, name, name)
	return nil
}

func runLoopValidate(cmd *cobra.Command, args []string) error {
	lines, err := app.New(loopRoot).LoopValidate()
	if err != nil {
		return err
	}
	for _, line := range lines {
		fmt.Fprintln(cmd.OutOrStdout(), line)
	}
	return nil
}
