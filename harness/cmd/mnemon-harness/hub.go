package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	"github.com/spf13/cobra"
)

const defaultCloudflareEnvPath = "~/.mnemon/cloudflare-bootstrap.env"

var (
	hubRoot                 string
	hubConfigPath           string
	hubCloudflareEnvFile    string
	hubCloudflareWorkerName string
	hubCloudflareSubdomain  string
	hubCloudflareAccountID  string
	hubCloudflarePrincipal  string
	hubCloudflareReplicaID  string
	hubCloudflareScope      []string
	hubCloudflareRemoteID   string
	hubCloudflareTimeout    time.Duration
	hubCloudflareNoDeploy   bool
)

var hubCmd = &cobra.Command{
	Use:   "hub",
	Short: "Manage harness MnemonHub connections",
}

var hubBootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Bootstrap a managed MnemonHub backend",
}

var hubBootstrapCloudflareCmd = &cobra.Command{
	Use:   "cloudflare",
	Short: "Deploy and connect a Cloudflare Durable Object MnemonHub",
	RunE:  runHubBootstrapCloudflare,
}

var hubDoctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Report harness MnemonHub connection readiness",
	RunE:  runHubDoctor,
}

type commandRunner func(context.Context, commandInvocation) (commandResult, error)

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type commandInvocation struct {
	Dir    string
	Env    []string
	Name   string
	Args   []string
	Stdin  string
	Redact []string
}

type commandResult struct {
	Stdout string
	Stderr string
}

type cloudflareBootstrapPlan struct {
	Root          string
	ConfigPath    string
	EnvFile       string
	APIToken      string
	AccountID     string
	WorkerName    string
	Subdomain     string
	Principal     string
	ReplicaID     string
	ReplicaToken  string
	RemoteID      string
	Scopes        []contract.ResourceRef
	ProjectDir    string
	Endpoint      string
	Timeout       time.Duration
	NoDeploy      bool
	CommandRunner commandRunner
	HTTPClient    httpDoer
	APIBaseURL    string
}

type cloudflareBootstrapResult struct {
	Endpoint                 string
	ConfigPath               string
	TokenRef                 string
	Principal                string
	ReplicaID                string
	SmokePushOK              bool
	SmokePullOK              bool
	SmokeStatusOK            bool
	CloudflareTokenKeptLocal bool
}

func init() {
	hubCmd.PersistentFlags().StringVar(&hubRoot, "root", ".", "project root")
	hubCmd.PersistentFlags().StringVar(&hubConfigPath, "config", "", "harness product config path")
	_ = hubCmd.PersistentFlags().MarkHidden("config")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflareEnvFile, "env-file", "", "Cloudflare bootstrap env file")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflareWorkerName, "worker-name", "", "Cloudflare Worker name")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflareSubdomain, "subdomain", "", "Cloudflare account workers.dev subdomain; empty reuses existing or creates a deterministic Mnemon subdomain")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflareAccountID, "account-id", "", "Cloudflare account id")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflarePrincipal, "principal", "mnemon-replica@team", "local replica principal for MnemonHub sync")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflareReplicaID, "replica-id", "", "local replica id; empty generates one")
	hubBootstrapCloudflareCmd.Flags().StringArrayVar(&hubCloudflareScope, "scope", []string{"memory/project"}, "granted resource scope kind/id; repeatable")
	hubBootstrapCloudflareCmd.Flags().StringVar(&hubCloudflareRemoteID, "remote", "cloudflare", "local Remote Workspace id")
	hubBootstrapCloudflareCmd.Flags().DurationVar(&hubCloudflareTimeout, "timeout", 2*time.Minute, "bootstrap command timeout")
	hubBootstrapCloudflareCmd.Flags().BoolVar(&hubCloudflareNoDeploy, "no-deploy", false, "prepare local config without running wrangler deploy or smoke")
	hubBootstrapCmd.AddCommand(hubBootstrapCloudflareCmd)
	hubCmd.AddCommand(hubBootstrapCmd, hubDoctorCmd)
	hubCmd.GroupID = groupSpine
	rootCmd.AddCommand(hubCmd)
}

func runHubBootstrapCloudflare(cmd *cobra.Command, args []string) error {
	plan, err := buildCloudflareBootstrapPlan(hubRoot, hubConfigPath)
	if err != nil {
		return err
	}
	result, err := bootstrapCloudflareHub(cmd.Context(), plan)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "MnemonHub connected")
	fmt.Fprintf(cmd.OutOrStdout(), "Endpoint: %s\n", result.Endpoint)
	fmt.Fprintf(cmd.OutOrStdout(), "Principal: %s\n", result.Principal)
	fmt.Fprintf(cmd.OutOrStdout(), "Replica ID: %s\n", result.ReplicaID)
	fmt.Fprintf(cmd.OutOrStdout(), "Config: %s\n", result.ConfigPath)
	fmt.Fprintf(cmd.OutOrStdout(), "Token file: %s\n", result.TokenRef)
	if !plan.NoDeploy {
		fmt.Fprintf(cmd.OutOrStdout(), "Smoke: push=%s pull=%s status=%s\n", okWord(result.SmokePushOK), okWord(result.SmokePullOK), okWord(result.SmokeStatusOK))
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Security: Cloudflare bootstrap token was not persisted by Mnemon.")
	return nil
}

func runHubDoctor(cmd *cobra.Command, args []string) error {
	root := strings.TrimSpace(hubRoot)
	if root == "" {
		root = "."
	}
	cfg, status, detail := doctorProductConfig(root)
	fmt.Fprintln(cmd.OutOrStdout(), "MnemonHub doctor")
	fmt.Fprintf(cmd.OutOrStdout(), "- Product config: %s", status)
	if detail != "" {
		fmt.Fprintf(cmd.OutOrStdout(), " (%s)", detail)
	}
	fmt.Fprintln(cmd.OutOrStdout())
	if cfg.Connections.Mnemonhub.Enabled {
		fmt.Fprintf(cmd.OutOrStdout(), "- Endpoint: %s\n", cfg.Connections.Mnemonhub.Endpoint)
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "- Endpoint: missing")
	}
	remotesPath := filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json")
	if remote, err := exchange.LoadRemoteEntry(remotesPath, "default"); err == nil {
		fmt.Fprintf(cmd.OutOrStdout(), "- Remote Workspace: %s backend=%s credential_ref=%s\n", remote.ID, remote.NormalizedBackend(), remote.CredentialRef)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "- Remote Workspace: missing (%v)\n", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), "- Free-plan estimate: 5 agents at 30s sync interval ~= 14,400 pulls/day before push/status overhead")
	return nil
}

func buildCloudflareBootstrapPlan(root, configPath string) (cloudflareBootstrapPlan, error) {
	root = filepath.Clean(root)
	env, envFile, err := loadCloudflareBootstrapEnv(hubCloudflareEnvFile)
	if err != nil {
		return cloudflareBootstrapPlan{}, err
	}
	apiToken := strings.TrimSpace(env["CLOUDFLARE_API_TOKEN"])
	if apiToken == "" && !hubCloudflareNoDeploy {
		return cloudflareBootstrapPlan{}, fmt.Errorf("CLOUDFLARE_API_TOKEN is required")
	}
	accountID := firstNonEmpty(hubCloudflareAccountID, env["CLOUDFLARE_ACCOUNT_ID"])
	if strings.TrimSpace(accountID) == "" && !hubCloudflareNoDeploy {
		return cloudflareBootstrapPlan{}, fmt.Errorf("CLOUDFLARE_ACCOUNT_ID is required")
	}
	workerName := firstNonEmpty(hubCloudflareWorkerName, env["MNEMON_CLOUDFLARE_WORKER_NAME"], "mnemon-r3-1-hub")
	if err := validateCloudflareWorkerName(workerName); err != nil {
		return cloudflareBootstrapPlan{}, err
	}
	subdomain := firstNonEmpty(hubCloudflareSubdomain, env["MNEMON_CLOUDFLARE_SUBDOMAIN"])
	if subdomain != "" {
		if err := validateCloudflareWorkersSubdomain(subdomain); err != nil {
			return cloudflareBootstrapPlan{}, err
		}
	}
	replicaID := strings.TrimSpace(hubCloudflareReplicaID)
	if replicaID == "" {
		replicaID = "local-" + randomSuffix(6)
	}
	scopes, err := parseScopeRefs(hubCloudflareScope)
	if err != nil {
		return cloudflareBootstrapPlan{}, err
	}
	replicaToken, err := randomHex(32)
	if err != nil {
		return cloudflareBootstrapPlan{}, err
	}
	return cloudflareBootstrapPlan{
		Root:          root,
		ConfigPath:    configPath,
		EnvFile:       envFile,
		APIToken:      apiToken,
		AccountID:     strings.TrimSpace(accountID),
		WorkerName:    workerName,
		Subdomain:     subdomain,
		Principal:     strings.TrimSpace(hubCloudflarePrincipal),
		ReplicaID:     replicaID,
		ReplicaToken:  replicaToken,
		RemoteID:      firstNonEmpty(hubCloudflareRemoteID, "cloudflare"),
		Scopes:        scopes,
		ProjectDir:    filepath.Join(repoRootForHarness(), "harness", "cloudflare", "mnemonhub"),
		Timeout:       hubCloudflareTimeout,
		NoDeploy:      hubCloudflareNoDeploy,
		CommandRunner: defaultCommandRunner,
		HTTPClient:    http.DefaultClient,
		APIBaseURL:    "https://api.cloudflare.com/client/v4",
	}, nil
}

func bootstrapCloudflareHub(ctx context.Context, plan cloudflareBootstrapPlan) (cloudflareBootstrapResult, error) {
	if strings.TrimSpace(plan.Principal) == "" {
		return cloudflareBootstrapResult{}, fmt.Errorf("--principal is required")
	}
	if strings.TrimSpace(plan.ReplicaID) == "" {
		return cloudflareBootstrapResult{}, fmt.Errorf("replica id is required")
	}
	if plan.CommandRunner == nil {
		plan.CommandRunner = defaultCommandRunner
	}
	if plan.HTTPClient == nil {
		plan.HTTPClient = http.DefaultClient
	}
	if strings.TrimSpace(plan.APIBaseURL) == "" {
		plan.APIBaseURL = "https://api.cloudflare.com/client/v4"
	}
	endpoint := plan.Endpoint
	if !plan.NoDeploy {
		deployCtx := ctx
		var cancel context.CancelFunc
		if plan.Timeout > 0 {
			deployCtx, cancel = context.WithTimeout(ctx, plan.Timeout)
			defer cancel()
		}
		if _, err := ensureCloudflareWorkersSubdomain(deployCtx, plan); err != nil {
			return cloudflareBootstrapResult{}, err
		}
		out, err := deployCloudflareWorker(deployCtx, plan)
		if err != nil {
			return cloudflareBootstrapResult{}, err
		}
		endpoint = cloudflareEndpointFromWranglerOutput(out.Stdout + "\n" + out.Stderr)
		if endpoint == "" {
			return cloudflareBootstrapResult{}, fmt.Errorf("wrangler deploy did not report a workers.dev endpoint")
		}
		if err := putCloudflareSecret(deployCtx, plan, "MNEMON_HUB_TOKENS_JSON", cloudflareTokensJSON(plan)); err != nil {
			return cloudflareBootstrapResult{}, err
		}
		if err := putCloudflareSecret(deployCtx, plan, "MNEMON_HUB_GRANTS_JSON", cloudflareGrantsJSON(plan)); err != nil {
			return cloudflareBootstrapResult{}, err
		}
		if err := smokeCloudflareHub(deployCtx, endpoint, plan); err != nil {
			return cloudflareBootstrapResult{}, err
		}
	} else if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.example.invalid", plan.WorkerName)
	}
	tokenRef, err := writeCloudflareLocalSyncConfig(plan, endpoint)
	if err != nil {
		return cloudflareBootstrapResult{}, err
	}
	cfgPath, err := writeCloudflareProductConfig(plan, endpoint)
	if err != nil {
		return cloudflareBootstrapResult{}, err
	}
	return cloudflareBootstrapResult{
		Endpoint:                 endpoint,
		ConfigPath:               cfgPath,
		TokenRef:                 tokenRef,
		Principal:                plan.Principal,
		ReplicaID:                plan.ReplicaID,
		SmokePushOK:              !plan.NoDeploy,
		SmokePullOK:              !plan.NoDeploy,
		SmokeStatusOK:            !plan.NoDeploy,
		CloudflareTokenKeptLocal: true,
	}, nil
}

func putCloudflareSecret(ctx context.Context, plan cloudflareBootstrapPlan, name, value string) error {
	cmd := cloudflareWranglerCommand(ctx, plan, "secret", "put", name, "--name", plan.WorkerName)
	cmd.Stdin = value + "\n"
	_, err := plan.CommandRunner(ctx, cmd)
	return err
}

func deployCloudflareWorker(ctx context.Context, plan cloudflareBootstrapPlan) (commandResult, error) {
	return plan.CommandRunner(ctx, cloudflareWranglerCommand(ctx, plan, "deploy", "--name", plan.WorkerName))
}

func ensureCloudflareWorkersSubdomain(ctx context.Context, plan cloudflareBootstrapPlan) (string, error) {
	existing, err := getCloudflareWorkersSubdomain(ctx, plan)
	if err != nil {
		return "", err
	}
	if existing != "" {
		return existing, nil
	}
	subdomain := strings.TrimSpace(plan.Subdomain)
	if subdomain == "" {
		subdomain = defaultCloudflareWorkersSubdomain(plan)
	}
	if err := validateCloudflareWorkersSubdomain(subdomain); err != nil {
		return "", err
	}
	created, err := putCloudflareWorkersSubdomain(ctx, plan, subdomain)
	if err != nil {
		return "", err
	}
	return created, nil
}

func getCloudflareWorkersSubdomain(ctx context.Context, plan cloudflareBootstrapPlan) (string, error) {
	var resp struct {
		Success bool `json:"success"`
		Result  struct {
			Subdomain string `json:"subdomain"`
		} `json:"result"`
		Errors []cloudflareAPIError `json:"errors"`
	}
	status, err := doCloudflareAPI(ctx, plan, http.MethodGet, "/accounts/"+url.PathEscape(plan.AccountID)+"/workers/subdomain", nil, &resp)
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound {
		return "", nil
	}
	if !resp.Success {
		return "", fmt.Errorf("Cloudflare workers.dev subdomain lookup failed: %s", cloudflareErrorSummary(resp.Errors))
	}
	return strings.TrimSpace(resp.Result.Subdomain), nil
}

func putCloudflareWorkersSubdomain(ctx context.Context, plan cloudflareBootstrapPlan, subdomain string) (string, error) {
	body, _ := json.Marshal(map[string]string{"subdomain": subdomain})
	var resp struct {
		Success bool `json:"success"`
		Result  struct {
			Subdomain string `json:"subdomain"`
		} `json:"result"`
		Errors []cloudflareAPIError `json:"errors"`
	}
	_, err := doCloudflareAPI(ctx, plan, http.MethodPut, "/accounts/"+url.PathEscape(plan.AccountID)+"/workers/subdomain", body, &resp)
	if err != nil {
		return "", err
	}
	if !resp.Success {
		return "", fmt.Errorf("Cloudflare workers.dev subdomain create failed for %q: %s; choose another --subdomain or MNEMON_CLOUDFLARE_SUBDOMAIN", subdomain, cloudflareErrorSummary(resp.Errors))
	}
	if strings.TrimSpace(resp.Result.Subdomain) == "" {
		return "", fmt.Errorf("Cloudflare workers.dev subdomain create returned empty subdomain")
	}
	return strings.TrimSpace(resp.Result.Subdomain), nil
}

type cloudflareAPIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func doCloudflareAPI(ctx context.Context, plan cloudflareBootstrapPlan, method, path string, body []byte, out any) (int, error) {
	base := strings.TrimRight(plan.APIBaseURL, "/")
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+plan.APIToken)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := plan.HTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if len(data) > 0 && out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode Cloudflare API response: %w", err)
		}
	}
	if resp.StatusCode >= 500 {
		return resp.StatusCode, fmt.Errorf("Cloudflare API %s %s returned HTTP %d", method, path, resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func cloudflareErrorSummary(errors []cloudflareAPIError) string {
	if len(errors) == 0 {
		return "unknown error"
	}
	parts := make([]string, 0, len(errors))
	for _, item := range errors {
		if item.Code != 0 {
			parts = append(parts, fmt.Sprintf("%d %s", item.Code, strings.TrimSpace(item.Message)))
			continue
		}
		parts = append(parts, strings.TrimSpace(item.Message))
	}
	return strings.Join(parts, "; ")
}

func defaultCloudflareWorkersSubdomain(plan cloudflareBootstrapPlan) string {
	account := strings.ToLower(strings.TrimSpace(plan.AccountID))
	if len(account) > 8 {
		account = account[:8]
	}
	return "mnemon-" + sanitizeCloudflareLabel(account)
}

func smokeCloudflareHub(ctx context.Context, endpoint string, plan cloudflareBootstrapPlan) error {
	if err := waitCloudflareEndpointReady(ctx, endpoint); err != nil {
		return err
	}
	client := access.NewClientWithToken(endpoint, plan.ReplicaToken)
	smokeID := "cloudflare-smoke-" + randomSuffix(6)
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	fields := map[string]any{"content": smokeID}
	fieldsJSON, _ := json.Marshal(fields)
	sum := sha256.Sum256(fieldsJSON)
	env, err := contract.SyncedEventEnvelopeFromMaterial(contract.SyncedEventMaterial{
		OriginReplicaID: plan.ReplicaID,
		LocalDecisionID: smokeID,
		LocalIngestSeq:  1,
		Actor:           contract.ActorID(plan.Principal),
		ResourceRef:     ref,
		ResourceVersion: 1,
		FieldsDigest:    hex.EncodeToString(sum[:]),
		Fields:          fields,
		DecidedAt:       time.Now().UTC().Format(time.RFC3339),
		Status:          "pending",
	})
	if err != nil {
		return err
	}
	var lastErr error
	for i := 0; i < 10; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		push, err := client.SyncPush(contract.SyncPushRequest{ReplicaID: plan.ReplicaID, BatchID: smokeID, Events: []eventmodel.EventEnvelope{env}})
		if err == nil && len(push.Accepted) == 1 {
			if _, err = client.SyncPull(contract.SyncPullRequest{ReplicaID: plan.ReplicaID}); err != nil {
				return err
			}
			if _, err = client.SyncStatus(); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("smoke push %s", syncPushDiagnostic(push))
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return fmt.Errorf("cloudflare smoke failed: %w", lastErr)
}

func syncPushDiagnostic(push contract.SyncPushResponse) string {
	parts := []string{fmt.Sprintf("accepted=%d rejected=%d conflicts=%d", len(push.Accepted), len(push.Rejected), len(push.Conflicts))}
	for _, item := range push.Rejected {
		parts = append(parts, fmt.Sprintf("rejected event=%s subject=%s diagnostic=%q", item.EventID, item.Subject, item.Diagnostic))
	}
	for _, item := range push.Conflicts {
		parts = append(parts, fmt.Sprintf("conflict event=%s subject=%s diagnostic=%q", item.EventID, item.Subject, item.Diagnostic))
	}
	return strings.Join(parts, "; ")
}

func waitCloudflareEndpointReady(ctx context.Context, endpoint string) error {
	client := &http.Client{Timeout: 20 * time.Second}
	var lastErr error
	for i := 0; i < 10; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
			lastErr = fmt.Errorf("endpoint returned HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	return fmt.Errorf("cloudflare endpoint readiness failed: %w", lastErr)
}

func writeCloudflareLocalSyncConfig(plan cloudflareBootstrapPlan, endpoint string) (string, error) {
	remoteID := strings.TrimSpace(plan.RemoteID)
	if remoteID == "" {
		remoteID = "cloudflare"
	}
	if err := upsertSyncRemote(filepath.Join(plan.Root, ".mnemon", "harness", "sync", "remotes.json"), plan.Root, remoteID, exchange.RemoteBackendHTTP, "", endpoint, plan.ReplicaToken, "", ""); err != nil {
		return "", err
	}
	return filepath.ToSlash(filepath.Join(".mnemon", "harness", "sync", "credentials", remoteID+".token")), nil
}

func writeCloudflareProductConfig(plan cloudflareBootstrapPlan, endpoint string) (string, error) {
	cfg, _, err := loadHarnessProductConfig(plan.Root, plan.ConfigPath)
	if err != nil {
		return "", err
	}
	cfg.Connections.Mnemonhub = productconfig.MnemonhubConnection{Enabled: true, Endpoint: endpoint}
	cfg.Daemon.InteractionWatchers = appendUniqueString(cfg.Daemon.InteractionWatchers, productconfig.ConnectionMnemonhub)
	return saveHarnessProductConfig(plan.Root, plan.ConfigPath, cfg)
}

func cloudflareWranglerCommand(ctx context.Context, plan cloudflareBootstrapPlan, args ...string) commandInvocation {
	name, baseArgs := resolveWranglerCommand()
	env := append(os.Environ(),
		"CLOUDFLARE_API_TOKEN="+plan.APIToken,
		"CLOUDFLARE_ACCOUNT_ID="+plan.AccountID,
	)
	allArgs := append(baseArgs, args...)
	return commandInvocation{
		Dir:    plan.ProjectDir,
		Env:    env,
		Name:   name,
		Args:   allArgs,
		Redact: []string{plan.APIToken, plan.ReplicaToken},
	}
}

func resolveWranglerCommand() (string, []string) {
	if path, err := exec.LookPath("wrangler"); err == nil {
		return path, nil
	}
	if runtime.GOOS == "windows" {
		return "npx.cmd", []string{"--yes", "wrangler"}
	}
	return "npx", []string{"--yes", "wrangler"}
}

func defaultCommandRunner(ctx context.Context, inv commandInvocation) (commandResult, error) {
	cmd := exec.CommandContext(ctx, inv.Name, inv.Args...)
	cmd.Dir = inv.Dir
	cmd.Env = inv.Env
	if inv.Stdin != "" {
		cmd.Stdin = strings.NewReader(inv.Stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := commandResult{Stdout: redact(stdout.String(), inv.Redact), Stderr: redact(stderr.String(), inv.Redact)}
	if err != nil {
		return result, fmt.Errorf("%s %s failed: %w\n%s", filepath.Base(inv.Name), strings.Join(inv.Args, " "), err, result.Stderr)
	}
	return result, nil
}

func cloudflareTokensJSON(plan cloudflareBootstrapPlan) string {
	raw, _ := json.Marshal(map[string]string{plan.ReplicaToken: plan.Principal})
	return string(raw)
}

func cloudflareGrantsJSON(plan cloudflareBootstrapPlan) string {
	raw, _ := json.Marshal(map[string]map[string]any{
		plan.Principal: {
			"principal": plan.Principal,
			"scopes":    plan.Scopes,
		},
	})
	return string(raw)
}

func cloudflareEndpointFromWranglerOutput(text string) string {
	re := regexp.MustCompile(`https://[A-Za-z0-9][A-Za-z0-9.-]*\.workers\.dev`)
	matches := re.FindAllString(text, -1)
	if len(matches) == 0 {
		return ""
	}
	return matches[len(matches)-1]
}

func loadCloudflareBootstrapEnv(explicit string) (map[string]string, string, error) {
	out := map[string]string{}
	for _, key := range []string{"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "MNEMON_CLOUDFLARE_WORKER_NAME", "MNEMON_CLOUDFLARE_SUBDOMAIN"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			out[key] = value
		}
	}
	for _, path := range []string{explicit, expandHome(defaultCloudflareEnvPath)} {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		values, ok, err := readCloudflareEnvFile(path)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			continue
		}
		for key, value := range values {
			if strings.TrimSpace(out[key]) == "" {
				out[key] = value
			}
		}
		return out, path, nil
	}
	return out, "", nil
}

func readCloudflareEnvFile(path string) (map[string]string, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, false, fmt.Errorf("Cloudflare env file %s must be permission 0600 or stricter", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	allowed := map[string]bool{"CLOUDFLARE_API_TOKEN": true, "CLOUDFLARE_ACCOUNT_ID": true, "MNEMON_CLOUDFLARE_WORKER_NAME": true, "MNEMON_CLOUDFLARE_SUBDOMAIN": true}
	values := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, false, fmt.Errorf("invalid env line in %s", path)
		}
		key = strings.TrimSpace(key)
		if !allowed[key] {
			return nil, false, fmt.Errorf("unsupported Cloudflare env key %q in %s", key, path)
		}
		values[key] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	return values, true, scanner.Err()
}

func parseScopeRefs(values []string) ([]contract.ResourceRef, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("at least one --scope kind/id is required")
	}
	seen := map[string]bool{}
	var out []contract.ResourceRef
	for _, value := range values {
		kind, id, ok := strings.Cut(strings.TrimSpace(value), "/")
		if !ok || strings.TrimSpace(kind) == "" || strings.TrimSpace(id) == "" {
			return nil, fmt.Errorf("scope %q must be kind/id", value)
		}
		ref := contract.ResourceRef{Kind: contract.ResourceKind(strings.TrimSpace(kind)), ID: contract.ResourceID(strings.TrimSpace(id))}
		key := string(ref.Kind) + "/" + string(ref.ID)
		if !seen[key] {
			out = append(out, ref)
			seen[key] = true
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return string(out[i].Kind)+"/"+string(out[i].ID) < string(out[j].Kind)+"/"+string(out[j].ID)
	})
	return out, nil
}

func validateCloudflareWorkerName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("Cloudflare worker name is required")
	}
	if len(name) > 63 {
		return fmt.Errorf("Cloudflare worker name must be at most 63 characters")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return fmt.Errorf("Cloudflare worker name %q must use lowercase letters, numbers, or dash", name)
	}
	return nil
}

func validateCloudflareWorkersSubdomain(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("Cloudflare workers.dev subdomain is required")
	}
	if len(name) > 63 {
		return fmt.Errorf("Cloudflare workers.dev subdomain must be at most 63 characters")
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") {
		return fmt.Errorf("Cloudflare workers.dev subdomain must not start or end with dash")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return fmt.Errorf("Cloudflare workers.dev subdomain %q must use lowercase letters, numbers, or dash", name)
	}
	return nil
}

func sanitizeCloudflareLabel(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "hub"
	}
	if len(out) > 56 {
		out = strings.Trim(out[:56], "-")
	}
	if out == "" {
		out = "hub"
	}
	return out
}

func randomHex(bytesLen int) (string, error) {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func randomSuffix(bytesLen int) string {
	value, err := randomHex(bytesLen)
	if err != nil {
		return "unknown"
	}
	return value
}

func redact(text string, secrets []string) string {
	for _, secret := range secrets {
		if strings.TrimSpace(secret) != "" {
			text = strings.ReplaceAll(text, secret, "<redacted>")
		}
	}
	return text
}

func expandHome(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func repoRootForHarness() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func okWord(ok bool) string {
	if ok {
		return "ok"
	}
	return "skipped"
}

func validateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("endpoint must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("endpoint host is required")
	}
	return nil
}
