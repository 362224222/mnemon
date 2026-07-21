package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

const maxDoctorResponseBytes = localapi.MaxStatusResponseBytes + 4096

const (
	doctorModeOnline  = "online"
	doctorModeOffline = "offline"
	doctorModeUnknown = "unknown"

	doctorHealthy      = "healthy"
	doctorDegraded     = "degraded"
	doctorInconclusive = "inconclusive"

	doctorPass       = "pass"
	doctorFail       = "fail"
	doctorUnobserved = "unobserved"

	doctorIssueNone                 = "none"
	doctorIssueAuthorityDisabled    = "authority_disabled"
	doctorIssueAuthorityStale       = "daemon_authority_stale"
	doctorIssueAuthorityUnavailable = "authority_unavailable"
	doctorIssueAssetsUnavailable    = "canonical_assets_unavailable"
	doctorIssueAssetMismatch        = "asset_revision_mismatch"
	doctorIssueProjection           = "host_projection_unavailable"
	doctorIssueRegistration         = "host_registration_unavailable"
	doctorIssueDaemon               = "daemon_unavailable"
	doctorIssueRuntimeStarting      = "runtime_starting"
	doctorIssueRuntimeRecovering    = "runtime_recovering"
	doctorIssueRuntimeRetrying      = "runtime_retrying"
	doctorIssueRuntimeFailed        = "runtime_failed"
	doctorIssueRuntimeUnobserved    = "runtime_unobserved"
	doctorIssueChannelQueued        = "channel_progress_queued"
	doctorIssueChannelDegraded      = "channel_progress_degraded"
	doctorIssueChannelUnobserved    = "channel_progress_unobserved"
	doctorIssueObservationUnknown   = "observation_inconclusive"

	doctorRemedyNone   = "none"
	doctorRemedySetup  = "run_setup"
	doctorRemedyRetry  = "retry"
	doctorRemedyDoctor = "rerun_doctor"
)

var doctorCheckNames = [...]string{
	"node_authority",
	"canonical_assets",
	"host_projection",
	"host_registration",
	"daemon",
	"managed_runtime",
	"channel_progress",
}

type doctorChecks [len(doctorCheckNames)]doctorCheck

type doctorControlClient interface {
	statusControlClient
	ReadAuthority(context.Context) (localapi.AuthorityResponse, *localapi.APIError)
}

type doctorCompanion interface {
	Inspect(context.Context) (localapi.AuthorityResponse, error)
}

type doctorLifecycleLease interface {
	Close() error
}

type doctorDependencies struct {
	workingDirectory func() (string, error)
	newClient        func(string) (doctorControlClient, error)
	ensureDaemon     func(context.Context, string, string, daemonHealthClient) *localapi.APIError
	loadBundle       func() (assets.Bundle, error)
	verifyNodeBundle func(string, assets.Bundle) error
	verifyProjection func(string, string, assets.Host, assets.Bundle) error
	inspectHost      func(context.Context, assets.Host) (integration.HostObservation, error)
	verifyActivation func(context.Context, string, string, integration.HostObservation,
		assets.Bundle) error
	newCompanion     func(context.Context, string, string) (doctorCompanion, error)
	acquireLifecycle func(context.Context, node.DaemonLifecycleOptions) (doctorLifecycleLease, error)
}

type doctorApp struct {
	stdout  io.Writer
	stderr  io.Writer
	version string
	deps    doctorDependencies
}

type doctorCheck struct {
	Issue  string `json:"issue"`
	Name   string `json:"name"`
	Remedy string `json:"remedy"`
	Status string `json:"status"`
}

type doctorReport struct {
	Channels      []localapi.StatusChannel `json:"channels"`
	Checks        []doctorCheck            `json:"checks"`
	Mode          string                   `json:"mode"`
	SchemaVersion int                      `json:"schema_version"`
	Scope         string                   `json:"scope"`
	Status        string                   `json:"status"`
}

type doctorObservation struct {
	report doctorReport
	exit   int
}

func productionDoctorDependencies() doctorDependencies {
	return doctorDependencies{
		workingDirectory: os.Getwd,
		newClient: func(nodeState string) (doctorControlClient, error) {
			return localapi.NewClient(nodeState)
		},
		ensureDaemon:     ensureAgentDaemon,
		loadBundle:       assets.Load,
		verifyNodeBundle: integration.VerifyNodeBundle,
		verifyProjection: integration.VerifyHostProjection,
		inspectHost:      integration.InspectHost,
		verifyActivation: integration.VerifyHostActivation,
		newCompanion: func(ctx context.Context, workspace, version string) (doctorCompanion, error) {
			return newCompanionRunner(ctx, workspace, version)
		},
		acquireLifecycle: func(ctx context.Context,
			options node.DaemonLifecycleOptions,
		) (doctorLifecycleLease, error) {
			return node.AcquireDaemonLifecycle(ctx, options)
		},
	}
}

// RunDoctor observes but never repairs. Online checks use authenticated local
// control; offline checks retain ensure.lock while an exact-version companion
// proves exclusive Store access. An unproved writer state is inconclusive,
// never guessed from a missing socket.
func RunDoctor(ctx context.Context, args []string, stdout, stderr io.Writer, version string) int {
	app := &doctorApp{stdout: stdout, stderr: stderr, version: version,
		deps: productionDoctorDependencies()}
	return app.run(ctx, args)
}

func (app *doctorApp) run(ctx context.Context, args []string) int {
	if app == nil || ctx == nil || app.stdout == nil || app.stderr == nil ||
		!validCompanionVersion(app.version) || !validDoctorDependencies(app.deps) {
		return 1
	}
	if len(args) != 0 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"doctor accepts no arguments"))
	}
	workspace, nodeState, err := resolveManagedWorkspace(app.deps.workingDirectory)
	if err != nil {
		return app.writeError(localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"Mnemon Harness is not set up in this workspace; run setup"))
	}

	if client, clientErr := app.deps.newClient(nodeState); clientErr == nil && client != nil {
		status, apiErr := client.ReadStatus(ctx)
		if apiErr == nil {
			return app.writeObservation(app.observeOnline(ctx, workspace, nodeState, client, status))
		}
		if apiErr.Code == localapi.CodeMnemondUnavailable {
			if ensureErr := app.deps.ensureDaemon(ctx, workspace, nodeState, client); ensureErr == nil {
				status, apiErr = client.ReadStatus(ctx)
				if apiErr == nil {
					return app.writeObservation(app.observeOnline(ctx, workspace, nodeState, client, status))
				}
			}
		}
	}
	return app.writeObservation(app.observeOffline(ctx, workspace, nodeState))
}

func (app *doctorApp) observeOnline(ctx context.Context, workspace, nodeState string,
	client doctorControlClient, status localapi.StatusResponse,
) doctorObservation {
	authority, apiErr := client.ReadAuthority(ctx)
	if apiErr != nil {
		return inconclusiveDoctorObservation()
	}
	if _, err := localapi.AuthorityDigest(authority); err != nil {
		return inconclusiveDoctorObservation()
	}
	checks, installExit := app.installationChecks(ctx, workspace, nodeState, authority)
	if status.AssetRevision != authority.AssetRevision ||
		status.AssetRevision != authority.ActiveAssetRevision {
		checks[0] = failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityStale,
			doctorRemedySetup)
		installExit = mergeDoctorExit(installExit, 3)
	}
	if status.Activation.State == "failed" {
		switch status.Activation.Issue {
		case "durable_authority_mismatch":
			if checks[0].Status == doctorPass {
				checks[0] = failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityStale,
					doctorRemedySetup)
			}
		case "asset_revision_mismatch":
			if !doctorInstallationFailure(checks) {
				checks[0] = failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityStale,
					doctorRemedySetup)
			}
		default:
			if !doctorInstallationFailure(checks) {
				checks[0] = failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityUnavailable,
					doctorRemedyDoctor)
				installExit = mergeDoctorExit(installExit, 1)
			}
		}
		installExit = mergeDoctorExit(installExit, 3)
	}
	checks[4] = passedDoctorCheck(doctorCheckNames[4])
	runtime, runtimeExit := doctorRuntimeCheck(status.Runtime.State)
	checks[5] = runtime
	channel, channelExit := doctorChannelCheck(status.Channels)
	checks[6] = channel
	exit := mergeDoctorExit(status.ExitStatus(), installExit)
	exit = mergeDoctorExit(exit, runtimeExit)
	exit = mergeDoctorExit(exit, channelExit)
	return doctorObservation{report: newDoctorReportWithChannels(doctorModeOnline, checks,
		status.Channels, exit), exit: exit}
}

func (app *doctorApp) observeOffline(ctx context.Context, workspace,
	nodeState string,
) doctorObservation {
	lease, err := app.deps.acquireLifecycle(ctx, node.DaemonLifecycleOptions{
		Workspace: workspace, NodeState: nodeState})
	if err != nil || lease == nil {
		return inconclusiveDoctorObservation()
	}
	closed := false
	closeLease := func() error {
		if closed {
			return nil
		}
		closed = true
		return lease.Close()
	}
	defer func() { _ = closeLease() }()

	companion, err := app.deps.newCompanion(ctx, workspace, app.version)
	if err != nil || companion == nil {
		_ = closeLease()
		return inconclusiveDoctorObservation()
	}
	authority, err := companion.Inspect(ctx)
	if err != nil {
		_ = closeLease()
		return inconclusiveDoctorObservation()
	}
	if _, err := localapi.AuthorityDigest(authority); err != nil {
		_ = closeLease()
		return inconclusiveDoctorObservation()
	}
	checks, installExit := app.installationChecks(ctx, workspace, nodeState, authority)
	checks[4] = failedDoctorCheck(doctorCheckNames[4], doctorIssueDaemon, doctorRemedyRetry)
	checks[5] = unobservedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeUnobserved,
		doctorRemedyRetry)
	checks[6] = unobservedDoctorCheck(doctorCheckNames[6], doctorIssueChannelUnobserved,
		doctorRemedyRetry)
	exit := mergeDoctorExit(5, installExit)
	report := newDoctorReport(doctorModeOffline, checks, exit)
	if err := closeLease(); err != nil {
		return inconclusiveDoctorObservation()
	}
	return doctorObservation{report: report, exit: exit}
}

func (app *doctorApp) installationChecks(ctx context.Context, workspace, nodeState string,
	authority localapi.AuthorityResponse,
) (doctorChecks, int) {
	checks := unobservedDoctorChecks()
	exit := 0
	if _, err := localapi.AuthorityDigest(authority); err != nil {
		checks[0] = failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityUnavailable,
			doctorRemedyDoctor)
		return checks, 1
	}
	if !authority.Enabled {
		checks[0] = failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityDisabled,
			doctorRemedySetup)
		exit = mergeDoctorExit(exit, 3)
	} else {
		checks[0] = passedDoctorCheck(doctorCheckNames[0])
	}

	bundle, err := app.deps.loadBundle()
	if err != nil {
		checks[1] = failedDoctorCheck(doctorCheckNames[1], doctorIssueAssetsUnavailable,
			doctorRemedySetup)
		return checks, mergeDoctorExit(exit, 3)
	}
	revision := bundle.Manifest().AssetRevision
	if authority.AssetRevision != revision || authority.ActiveAssetRevision != revision ||
		app.deps.verifyNodeBundle(nodeState, bundle) != nil {
		checks[1] = failedDoctorCheck(doctorCheckNames[1], doctorIssueAssetMismatch,
			doctorRemedySetup)
		return checks, mergeDoctorExit(exit, 3)
	}
	checks[1] = passedDoctorCheck(doctorCheckNames[1])

	host := assets.Host(authority.Host)
	if !host.Valid() || app.deps.verifyProjection(workspace, nodeState, host, bundle) != nil {
		checks[2] = failedDoctorCheck(doctorCheckNames[2], doctorIssueProjection,
			doctorRemedySetup)
		return checks, mergeDoctorExit(exit, 3)
	}
	checks[2] = passedDoctorCheck(doctorCheckNames[2])

	observation, err := app.deps.inspectHost(ctx, host)
	if err != nil || app.deps.verifyActivation(ctx, workspace, nodeState, observation, bundle) != nil {
		checks[3] = failedDoctorCheck(doctorCheckNames[3], doctorIssueRegistration,
			doctorRemedySetup)
		return checks, mergeDoctorExit(exit, 3)
	}
	checks[3] = passedDoctorCheck(doctorCheckNames[3])
	return checks, exit
}

func doctorRuntimeCheck(state string) (doctorCheck, int) {
	switch state {
	case "ready":
		return passedDoctorCheck(doctorCheckNames[5]), 0
	case "starting":
		return failedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeStarting,
			doctorRemedyRetry), 5
	case "recovering":
		return failedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeRecovering,
			doctorRemedyRetry), 5
	case "retrying":
		return failedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeRetrying,
			doctorRemedyRetry), 5
	default:
		return failedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeFailed,
			doctorRemedyDoctor), 1
	}
}

func passedDoctorCheck(name string) doctorCheck {
	return doctorCheck{Issue: doctorIssueNone, Name: name, Remedy: doctorRemedyNone,
		Status: doctorPass}
}

func failedDoctorCheck(name, issue, remedy string) doctorCheck {
	return doctorCheck{Issue: issue, Name: name, Remedy: remedy, Status: doctorFail}
}

func unobservedDoctorCheck(name, issue, remedy string) doctorCheck {
	return doctorCheck{Issue: issue, Name: name, Remedy: remedy, Status: doctorUnobserved}
}

func unobservedDoctorChecks() doctorChecks {
	var checks doctorChecks
	for index, name := range doctorCheckNames {
		checks[index] = unobservedDoctorCheck(name, doctorIssueObservationUnknown,
			doctorRemedyDoctor)
	}
	return checks
}

func inconclusiveDoctorObservation() doctorObservation {
	checks := unobservedDoctorChecks()
	return doctorObservation{report: doctorReport{Channels: []localapi.StatusChannel{},
		Checks: checks[:], Mode: doctorModeUnknown,
		SchemaVersion: localapi.SchemaVersion, Scope: "managed_agent", Status: doctorInconclusive}, exit: 1}
}

func doctorInstallationFailure(checks doctorChecks) bool {
	for index := 0; index < 4; index++ {
		if checks[index].Status == doctorFail {
			return true
		}
	}
	return false
}

func newDoctorReport(mode string, checks doctorChecks, exit int) doctorReport {
	return newDoctorReportWithChannels(mode, checks, nil, exit)
}

func newDoctorReportWithChannels(mode string, checks doctorChecks,
	channels []localapi.StatusChannel, exit int,
) doctorReport {
	status := doctorHealthy
	if exit != 0 {
		status = doctorDegraded
	}
	return doctorReport{Channels: append([]localapi.StatusChannel{}, channels...),
		Checks: checks[:], Mode: mode, SchemaVersion: localapi.SchemaVersion,
		Scope: "managed_agent", Status: status}
}

func mergeDoctorExit(current, candidate int) int {
	rank := func(exit int) int {
		switch exit {
		case 1:
			return 3
		case 3:
			return 2
		case 5:
			return 1
		default:
			return 0
		}
	}
	if rank(candidate) > rank(current) {
		return candidate
	}
	return current
}

func validDoctorDependencies(deps doctorDependencies) bool {
	return deps.workingDirectory != nil && deps.newClient != nil && deps.ensureDaemon != nil &&
		deps.loadBundle != nil && deps.verifyNodeBundle != nil && deps.verifyProjection != nil &&
		deps.inspectHost != nil && deps.verifyActivation != nil && deps.newCompanion != nil &&
		deps.acquireLifecycle != nil
}

func (app *doctorApp) writeObservation(observation doctorObservation) int {
	if !validDoctorReport(observation.report) ||
		observation.exit != doctorReportExit(observation.report) {
		return app.writeError(localapi.NewAPIError(localapi.CodeInternal,
			"doctor could not construct a trusted report"))
	}
	raw, err := model.CanonicalMarshal(observation.report)
	if err != nil || len(raw)+1 > maxDoctorResponseBytes {
		return app.writeError(localapi.NewAPIError(localapi.CodeInternal,
			"doctor report exceeds its closed bound"))
	}
	if _, err := app.stdout.Write(append(raw, '\n')); err != nil {
		return 1
	}
	return observation.exit
}

func (app *doctorApp) writeError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = localapi.NewAPIError(localapi.CodeInternal, "internal doctor error")
	}
	if _, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message); err != nil {
		return 1
	}
	return apiErr.ExitStatus()
}
