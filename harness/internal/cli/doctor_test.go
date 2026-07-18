package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type fakeDoctorClient struct {
	statuses        []localapi.StatusResponse
	statusErrors    []*localapi.APIError
	authorities     []localapi.AuthorityResponse
	authorityErrors []*localapi.APIError
	statusReads     int
	authorityReads  int
	probes          int
}

func (client *fakeDoctorClient) ProbeHealth(context.Context) (localapi.HealthResponse,
	*localapi.APIError,
) {
	client.probes++
	return localapi.HealthResponse{}, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"fake health unavailable")
}

func (client *fakeDoctorClient) ReadStatus(context.Context) (localapi.StatusResponse,
	*localapi.APIError,
) {
	index := client.statusReads
	client.statusReads++
	var response localapi.StatusResponse
	var apiErr *localapi.APIError
	if index < len(client.statuses) {
		response = client.statuses[index]
	}
	if index < len(client.statusErrors) {
		apiErr = client.statusErrors[index]
	}
	return response, apiErr
}

func (client *fakeDoctorClient) ReadAuthority(context.Context) (localapi.AuthorityResponse,
	*localapi.APIError,
) {
	index := client.authorityReads
	client.authorityReads++
	var response localapi.AuthorityResponse
	var apiErr *localapi.APIError
	if index < len(client.authorities) {
		response = client.authorities[index]
	}
	if index < len(client.authorityErrors) {
		apiErr = client.authorityErrors[index]
	}
	return response, apiErr
}

type fakeDoctorCompanion struct {
	authority localapi.AuthorityResponse
	err       error
	inspect   func()
}

func (companion *fakeDoctorCompanion) Inspect(context.Context) (localapi.AuthorityResponse,
	error,
) {
	if companion.inspect != nil {
		companion.inspect()
	}
	return companion.authority, companion.err
}

type fakeDoctorLease struct {
	closeErr error
	closes   int
	held     bool
}

func (lease *fakeDoctorLease) Close() error {
	lease.closes++
	lease.held = false
	return lease.closeErr
}

type doctorFenceWriter struct {
	buffer bytes.Buffer
	lease  *fakeDoctorLease
	t      *testing.T
}

func (writer *doctorFenceWriter) Write(value []byte) (int, error) {
	writer.t.Helper()
	if writer.lease.held {
		writer.t.Fatal("doctor wrote its report before releasing the lifecycle fence")
	}
	return writer.buffer.Write(value)
}

func (writer *doctorFenceWriter) String() string { return writer.buffer.String() }

type doctorTestFixture struct {
	workspace string
	nodeState string
	bundle    assets.Bundle
	authority localapi.AuthorityResponse
}

func TestDoctorAppReportsClosedHealthyOnlineObservation(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	client := &fakeDoctorClient{statuses: []localapi.StatusResponse{
		doctorTestStatus(t, fixture.bundle.Manifest().AssetRevision, "ready", true, ""),
	}, authorities: []localapi.AuthorityResponse{fixture.authority}}
	checks := make([]string, 0, 4)
	app, stdout, stderr := fixture.app(t, client, doctorTestOverrides{
		verifyNodeBundle: func(nodeState string, bundle assets.Bundle) error {
			if nodeState != fixture.nodeState || bundle.Manifest().AssetRevision !=
				fixture.bundle.Manifest().AssetRevision {
				t.Fatal("doctor verified another Node bundle")
			}
			checks = append(checks, "node")
			return nil
		},
		verifyProjection: func(workspace, nodeState string, host assets.Host,
			bundle assets.Bundle,
		) error {
			if workspace != fixture.workspace || nodeState != fixture.nodeState ||
				host != assets.HostCodex || bundle.Manifest().AssetRevision !=
				fixture.bundle.Manifest().AssetRevision {
				t.Fatal("doctor verified another Host projection")
			}
			checks = append(checks, "projection")
			return nil
		},
		inspectHost: func(context.Context, assets.Host) (integration.HostObservation, error) {
			checks = append(checks, "host")
			return integration.HostObservation{Host: assets.HostCodex}, nil
		},
		verifyActivation: func(_ context.Context, workspace, nodeState string,
			observation integration.HostObservation, bundle assets.Bundle,
		) error {
			if workspace != fixture.workspace || nodeState != fixture.nodeState ||
				observation.Host != assets.HostCodex || bundle.Manifest().AssetRevision !=
				fixture.bundle.Manifest().AssetRevision {
				t.Fatal("doctor verified another Host registration")
			}
			checks = append(checks, "registration")
			return nil
		},
	})
	exit := app.run(context.Background(), nil)
	report := decodeDoctorReport(t, stdout.String())
	if exit != 0 || stderr.Len() != 0 || client.statusReads != 1 ||
		client.authorityReads != 1 || client.probes != 0 || report.Mode != doctorModeOnline ||
		report.Scope != "managed_agent" || report.Status != doctorHealthy ||
		!allDoctorChecksPass(report.Checks) || strings.Join(checks, ",") !=
		"node,projection,host,registration" {
		t.Fatalf("online doctor = exit %d report %#v stderr %q reads=(%d,%d) probes=%d checks=%v",
			exit, report, stderr.String(), client.statusReads, client.authorityReads,
			client.probes, checks)
	}
}

func TestDoctorAppEnsuresOnlyForTransportUnavailabilityThenObservesOnline(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	ready := doctorTestStatus(t, fixture.bundle.Manifest().AssetRevision, "ready", true, "")
	client := &fakeDoctorClient{
		statuses: []localapi.StatusResponse{{}, ready},
		statusErrors: []*localapi.APIError{localapi.NewAPIError(
			localapi.CodeMnemondUnavailable, "secret-socket-detail"), nil},
		authorities: []localapi.AuthorityResponse{fixture.authority},
	}
	ensured := 0
	app, stdout, stderr := fixture.app(t, client, doctorTestOverrides{
		ensureDaemon: func(_ context.Context, workspace, nodeState string,
			got daemonHealthClient,
		) *localapi.APIError {
			if workspace != fixture.workspace || nodeState != fixture.nodeState || got != client {
				t.Fatal("doctor ensure escaped its managed Node")
			}
			ensured++
			return nil
		},
	})
	exit := app.run(context.Background(), nil)
	report := decodeDoctorReport(t, stdout.String())
	if exit != 0 || stderr.Len() != 0 || client.statusReads != 2 ||
		client.authorityReads != 1 || client.probes != 0 || ensured != 1 ||
		report.Mode != doctorModeOnline || report.Status != doctorHealthy {
		t.Fatalf("ensured doctor = exit %d report %#v stderr %q reads=(%d,%d) ensure=%d probes=%d",
			exit, report, stderr.String(), client.statusReads, client.authorityReads,
			ensured, client.probes)
	}
}

func TestDoctorAppReportsInstallationAndRuntimeFailuresWithoutDiagnostics(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	secret := "secret-provider-diagnostic"
	tests := []struct {
		name       string
		status     localapi.StatusResponse
		overrides  doctorTestOverrides
		wantExit   int
		wantCheck  int
		wantIssue  string
		wantRemedy string
	}{
		{name: "registration", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "ready", true, ""),
			overrides: doctorTestOverrides{verifyActivation: func(context.Context, string, string,
				integration.HostObservation, assets.Bundle,
			) error {
				return errors.New(secret)
			}}, wantExit: 3, wantCheck: 3, wantIssue: doctorIssueRegistration,
			wantRemedy: doctorRemedySetup},
		{name: "registration before transient Runtime", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "starting", true, ""),
			overrides: doctorTestOverrides{verifyActivation: func(context.Context, string, string,
				integration.HostObservation, assets.Bundle,
			) error {
				return errors.New(secret)
			}}, wantExit: 3, wantCheck: 3, wantIssue: doctorIssueRegistration,
			wantRemedy: doctorRemedySetup},
		{name: "fatal Runtime before registration", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "failed", true, secret),
			overrides: doctorTestOverrides{verifyActivation: func(context.Context, string, string,
				integration.HostObservation, assets.Bundle,
			) error {
				return errors.New(secret)
			}}, wantExit: 1, wantCheck: 3, wantIssue: doctorIssueRegistration,
			wantRemedy: doctorRemedySetup},
		{name: "starting", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "starting", true, ""),
			wantExit: 5, wantCheck: 5, wantIssue: doctorIssueRuntimeStarting,
			wantRemedy: doctorRemedyRetry},
		{name: "recovering", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "recovering", true, ""),
			wantExit: 5, wantCheck: 5, wantIssue: doctorIssueRuntimeRecovering,
			wantRemedy: doctorRemedyRetry},
		{name: "retrying", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "retrying", true, ""),
			wantExit: 5, wantCheck: 5, wantIssue: doctorIssueRuntimeRetrying,
			wantRemedy: doctorRemedyRetry},
		{name: "failed", status: doctorTestStatus(t,
			fixture.bundle.Manifest().AssetRevision, "failed", true, secret),
			wantExit: 1, wantCheck: 5, wantIssue: doctorIssueRuntimeFailed,
			wantRemedy: doctorRemedyDoctor},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeDoctorClient{statuses: []localapi.StatusResponse{test.status},
				authorities: []localapi.AuthorityResponse{fixture.authority}}
			app, stdout, stderr := fixture.app(t, client, test.overrides)
			exit := app.run(context.Background(), nil)
			report := decodeDoctorReport(t, stdout.String())
			check := report.Checks[test.wantCheck]
			if exit != test.wantExit || stderr.Len() != 0 || report.Mode != doctorModeOnline ||
				report.Status != doctorDegraded || check.Status != doctorFail ||
				check.Issue != test.wantIssue || check.Remedy != test.wantRemedy ||
				strings.Contains(stdout.String(), secret) {
				t.Fatalf("failed doctor = exit %d report %#v stderr %q", exit, report,
					stderr.String())
			}
		})
	}
}

func TestDoctorAppKeepsIndependentInstallationDiagnosisAndRejectsSnapshotDrift(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	tests := []struct {
		name      string
		status    localapi.StatusResponse
		override  doctorTestOverrides
		wantCheck int
		wantIssue string
		wantNode  string
	}{
		{name: "aggregate activation does not overwrite projection",
			status: doctorTestActivationStatus(t, fixture.bundle.Manifest().AssetRevision,
				"asset_revision_mismatch"),
			override: doctorTestOverrides{verifyProjection: func(string, string, assets.Host,
				assets.Bundle,
			) error {
				return errors.New("projection drift")
			}}, wantCheck: 2, wantIssue: doctorIssueProjection, wantNode: doctorIssueNone},
		{name: "durable authority drift remains visible beside projection drift",
			status: doctorTestActivationStatus(t, fixture.bundle.Manifest().AssetRevision,
				"durable_authority_mismatch"),
			override: doctorTestOverrides{verifyProjection: func(string, string, assets.Host,
				assets.Bundle,
			) error {
				return errors.New("projection drift")
			}}, wantCheck: 2, wantIssue: doctorIssueProjection,
			wantNode: doctorIssueAuthorityStale},
		{name: "status and authority generations differ",
			status: doctorTestStatus(t, model.Sum([]byte("another-doctor-generation")).String(),
				"ready", true, ""),
			wantCheck: 0, wantIssue: doctorIssueAuthorityStale,
			wantNode: doctorIssueAuthorityStale},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeDoctorClient{statuses: []localapi.StatusResponse{test.status},
				authorities: []localapi.AuthorityResponse{fixture.authority}}
			app, stdout, stderr := fixture.app(t, client, test.override)
			exit := app.run(context.Background(), nil)
			report := decodeDoctorReport(t, stdout.String())
			if exit != 3 || stderr.Len() != 0 || report.Status != doctorDegraded ||
				report.Checks[test.wantCheck].Issue != test.wantIssue ||
				report.Checks[0].Issue != test.wantNode ||
				report.Checks[1] != passedDoctorCheck(doctorCheckNames[1]) {
				t.Fatalf("drift doctor = exit %d report %#v stderr %q", exit, report,
					stderr.String())
			}
		})
	}
}

func TestDoctorAppOfflineProofHoldsLifecycleFenceThroughAllChecks(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	client := &fakeDoctorClient{statusErrors: []*localapi.APIError{localapi.NewAPIError(
		localapi.CodeMnemondUnavailable, "secret-offline-transport")}}
	lease := &fakeDoctorLease{held: true}
	assertHeld := func(stage string) {
		if !lease.held {
			t.Fatalf("lifecycle fence was released before %s", stage)
		}
	}
	companion := &fakeDoctorCompanion{authority: fixture.authority,
		inspect: func() { assertHeld("Store inspection") }}
	app, _, stderr := fixture.app(t, client, doctorTestOverrides{
		ensureDaemon: func(context.Context, string, string,
			daemonHealthClient,
		) *localapi.APIError {
			return localapi.NewAPIError(localapi.CodeMnemondUnavailable,
				"secret-daemon-launch-detail")
		},
		acquireLifecycle: func(_ context.Context,
			options node.DaemonLifecycleOptions,
		) (doctorLifecycleLease, error) {
			if options.Workspace != fixture.workspace || options.NodeState != fixture.nodeState {
				t.Fatal("doctor fenced another Node")
			}
			return lease, nil
		},
		newCompanion: func(_ context.Context, workspace,
			version string,
		) (doctorCompanion, error) {
			assertHeld("companion validation")
			if workspace != fixture.workspace || version != "r5-test" {
				t.Fatal("doctor selected an unbound companion")
			}
			return companion, nil
		},
		verifyNodeBundle: func(string, assets.Bundle) error {
			assertHeld("Node bundle verification")
			return nil
		},
		verifyProjection: func(string, string, assets.Host, assets.Bundle) error {
			assertHeld("Host projection verification")
			return nil
		},
		inspectHost: func(context.Context, assets.Host) (integration.HostObservation, error) {
			assertHeld("Host inspection")
			return integration.HostObservation{Host: assets.HostCodex}, nil
		},
		verifyActivation: func(context.Context, string, string, integration.HostObservation,
			assets.Bundle,
		) error {
			assertHeld("Host registration verification")
			return nil
		},
	})
	stdout := &doctorFenceWriter{lease: lease, t: t}
	app.stdout = stdout

	exit := app.run(context.Background(), nil)
	report := decodeDoctorReport(t, stdout.String())
	if exit != 5 || stderr.Len() != 0 || lease.held || lease.closes != 1 ||
		client.statusReads != 1 || client.authorityReads != 0 || report.Mode != doctorModeOffline ||
		report.Status != doctorDegraded || report.Checks[4].Issue != doctorIssueDaemon ||
		report.Checks[5].Status != doctorUnobserved ||
		report.Checks[5].Issue != doctorIssueRuntimeUnobserved ||
		strings.Contains(stdout.String(), "secret-") {
		t.Fatalf("offline doctor = exit %d report %#v stderr %q lease=(held=%v closes=%d)",
			exit, report, stderr.String(), lease.held, lease.closes)
	}
}

func TestDoctorAppOfflineInstallationFailureOutranksDaemonUnavailability(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	otherRevision := model.Sum([]byte("offline-doctor-assets")).String()
	authority := fixture.authority
	authority.AssetRevision = otherRevision
	authority.ActiveAssetRevision = otherRevision
	client := &fakeDoctorClient{statusErrors: []*localapi.APIError{
		localapi.NewAPIError(localapi.CodeAuthenticationFailed, "offline")}}
	lease := &fakeDoctorLease{held: true}
	app, stdout, stderr := fixture.app(t, client, doctorTestOverrides{
		acquireLifecycle: func(context.Context,
			node.DaemonLifecycleOptions,
		) (doctorLifecycleLease, error) {
			return lease, nil
		},
		newCompanion: func(context.Context, string, string) (doctorCompanion, error) {
			return &fakeDoctorCompanion{authority: authority}, nil
		},
	})
	exit := app.run(context.Background(), nil)
	report := decodeDoctorReport(t, stdout.String())
	if exit != 3 || stderr.Len() != 0 || lease.closes != 1 ||
		report.Mode != doctorModeOffline || report.Checks[1].Issue != doctorIssueAssetMismatch ||
		report.Checks[4].Issue != doctorIssueDaemon {
		t.Fatalf("offline installation doctor = exit %d report %#v stderr %q closes=%d",
			exit, report, stderr.String(), lease.closes)
	}
}

func TestDoctorAppNeverGuessesOfflineWithoutExclusiveWriterProof(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	secret := "secret-offline-proof"
	tests := []struct {
		name      string
		configure func(*doctorTestOverrides, *fakeDoctorLease)
		wantClose int
	}{
		{name: "lifecycle", configure: func(overrides *doctorTestOverrides,
			_ *fakeDoctorLease,
		) {
			overrides.acquireLifecycle = func(context.Context,
				node.DaemonLifecycleOptions,
			) (doctorLifecycleLease, error) {
				return nil, errors.New(secret)
			}
		}},
		{name: "companion", wantClose: 1, configure: func(overrides *doctorTestOverrides,
			_ *fakeDoctorLease,
		) {
			overrides.newCompanion = func(context.Context, string, string) (doctorCompanion, error) {
				return nil, errors.New(secret)
			}
		}},
		{name: "writer", wantClose: 1, configure: func(overrides *doctorTestOverrides,
			_ *fakeDoctorLease,
		) {
			overrides.newCompanion = func(context.Context, string, string) (doctorCompanion, error) {
				return &fakeDoctorCompanion{err: errors.New(secret)}, nil
			}
		}},
		{name: "lease close", wantClose: 1, configure: func(_ *doctorTestOverrides,
			lease *fakeDoctorLease,
		) {
			lease.closeErr = errors.New(secret)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeDoctorClient{statusErrors: []*localapi.APIError{
				localapi.NewAPIError(localapi.CodeAuthenticationFailed, secret)}}
			lease := &fakeDoctorLease{held: true}
			overrides := doctorTestOverrides{
				acquireLifecycle: func(context.Context,
					node.DaemonLifecycleOptions,
				) (doctorLifecycleLease, error) {
					return lease, nil
				},
				newCompanion: func(context.Context, string, string) (doctorCompanion, error) {
					return &fakeDoctorCompanion{authority: fixture.authority}, nil
				},
			}
			test.configure(&overrides, lease)
			app, stdout, stderr := fixture.app(t, client, overrides)
			exit := app.run(context.Background(), nil)
			report := decodeDoctorReport(t, stdout.String())
			if exit != 1 || stderr.Len() != 0 || report.Mode != doctorModeUnknown ||
				report.Status != doctorInconclusive || report.Checks[4].Status !=
				doctorUnobserved || report.Checks[4].Issue != doctorIssueObservationUnknown ||
				lease.closes != test.wantClose ||
				strings.Contains(stdout.String(), secret) {
				t.Fatalf("unproved doctor = exit %d report %#v stderr %q closes=%d",
					exit, report, stderr.String(), lease.closes)
			}
		})
	}
}

func TestDoctorAppValidatesInvocationAndReportShape(t *testing.T) {
	fixture := newDoctorTestFixture(t)
	client := &fakeDoctorClient{}
	app, stdout, stderr := fixture.app(t, client, doctorTestOverrides{})
	if exit := app.run(context.Background(), []string{"--json"}); exit != 2 ||
		stdout.Len() != 0 || stderr.String() !=
		"invalid_argument: doctor accepts no arguments\n" || client.statusReads != 0 {
		t.Fatalf("doctor arguments = exit %d stdout %q stderr %q reads=%d", exit,
			stdout.String(), stderr.String(), client.statusReads)
	}

	checks := unobservedDoctorChecks()
	report := newDoctorReport(doctorModeOnline, checks, 0)
	if validDoctorReport(report) {
		t.Fatal("healthy report admitted unobserved checks")
	}
	report = inconclusiveDoctorObservation().report
	report.Checks[0].Issue = "secret-open-world-issue"
	if validDoctorReport(report) {
		t.Fatal("doctor admitted an open-world issue")
	}
	var sink bytes.Buffer
	badApp := &doctorApp{stdout: &sink, stderr: io.Discard, version: "r5-test",
		deps: app.deps}
	if exit := badApp.writeObservation(doctorObservation{report: report, exit: 1}); exit != 1 ||
		sink.Len() != 0 {
		t.Fatalf("invalid report = exit %d output %q", exit, sink.String())
	}

	var passed [6]doctorCheck
	for index, name := range doctorCheckNames {
		passed[index] = passedDoctorCheck(name)
	}
	healthy := newDoctorReport(doctorModeOnline, passed, 0)
	if !validDoctorReport(healthy) {
		t.Fatal("doctor rejected its closed healthy report")
	}
	sink.Reset()
	if exit := badApp.writeObservation(doctorObservation{report: healthy, exit: 1}); exit != 1 ||
		sink.Len() != 0 {
		t.Fatalf("exit/report mismatch = exit %d output %q", exit, sink.String())
	}
	healthy.Mode = doctorModeOffline
	if validDoctorReport(healthy) {
		t.Fatal("doctor admitted an offline healthy report")
	}
	healthy = newDoctorReport(doctorModeOnline, passed, 0)
	healthy.Checks[4] = failedDoctorCheck(doctorCheckNames[4], doctorIssueProjection,
		doctorRemedySetup)
	if validDoctorReport(healthy) {
		t.Fatal("doctor admitted a projection issue under the daemon check")
	}
}

type doctorTestOverrides struct {
	ensureDaemon     func(context.Context, string, string, daemonHealthClient) *localapi.APIError
	verifyNodeBundle func(string, assets.Bundle) error
	verifyProjection func(string, string, assets.Host, assets.Bundle) error
	inspectHost      func(context.Context, assets.Host) (integration.HostObservation, error)
	verifyActivation func(context.Context, string, string, integration.HostObservation,
		assets.Bundle) error
	newCompanion     func(context.Context, string, string) (doctorCompanion, error)
	acquireLifecycle func(context.Context, node.DaemonLifecycleOptions) (doctorLifecycleLease, error)
}

func (fixture doctorTestFixture) app(t *testing.T, client *fakeDoctorClient,
	overrides doctorTestOverrides,
) (*doctorApp, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	failOffline := func(stage string) {
		t.Fatal("online doctor attempted offline " + stage)
	}
	deps := doctorDependencies{
		workingDirectory: func() (string, error) { return fixture.workspace, nil },
		newClient: func(nodeState string) (doctorControlClient, error) {
			if nodeState != fixture.nodeState {
				t.Fatalf("doctor client Node = %q", nodeState)
			}
			return client, nil
		},
		ensureDaemon: func(context.Context, string, string,
			daemonHealthClient,
		) *localapi.APIError {
			t.Fatal("doctor unexpectedly ensured the daemon")
			return localapi.NewAPIError(localapi.CodeInternal, "unexpected ensure")
		},
		loadBundle:       func() (assets.Bundle, error) { return fixture.bundle, nil },
		verifyNodeBundle: func(string, assets.Bundle) error { return nil },
		verifyProjection: func(string, string, assets.Host, assets.Bundle) error { return nil },
		inspectHost: func(_ context.Context, host assets.Host) (integration.HostObservation, error) {
			return integration.HostObservation{Host: host}, nil
		},
		verifyActivation: func(context.Context, string, string,
			integration.HostObservation, assets.Bundle,
		) error {
			return nil
		},
		newCompanion: func(context.Context, string, string) (doctorCompanion, error) {
			failOffline("companion discovery")
			return nil, errors.New("unexpected companion")
		},
		acquireLifecycle: func(context.Context,
			node.DaemonLifecycleOptions,
		) (doctorLifecycleLease, error) {
			failOffline("lifecycle acquisition")
			return nil, errors.New("unexpected lifecycle")
		},
	}
	if overrides.ensureDaemon != nil {
		deps.ensureDaemon = overrides.ensureDaemon
	}
	if overrides.verifyNodeBundle != nil {
		deps.verifyNodeBundle = overrides.verifyNodeBundle
	}
	if overrides.verifyProjection != nil {
		deps.verifyProjection = overrides.verifyProjection
	}
	if overrides.inspectHost != nil {
		deps.inspectHost = overrides.inspectHost
	}
	if overrides.verifyActivation != nil {
		deps.verifyActivation = overrides.verifyActivation
	}
	if overrides.newCompanion != nil {
		deps.newCompanion = overrides.newCompanion
	}
	if overrides.acquireLifecycle != nil {
		deps.acquireLifecycle = overrides.acquireLifecycle
	}
	return &doctorApp{stdout: &stdout, stderr: &stderr, version: "r5-test", deps: deps},
		&stdout, &stderr
}

func newDoctorTestFixture(t *testing.T) doctorTestFixture {
	t.Helper()
	workspace, nodeState := statusWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := model.ParsePeerID("peer-doctor-test")
	if err != nil {
		t.Fatal(err)
	}
	authority, err := localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer, Enabled: true,
		AssetRevision: bundle.Manifest().AssetRevision, ActiveAssetRevision: bundle.Manifest().AssetRevision,
		UpdatedAt: time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC), PeerID: peerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return doctorTestFixture{workspace: workspace, nodeState: nodeState, bundle: bundle,
		authority: authority}
}

func doctorTestStatus(t *testing.T, revision, state string, activationReady bool,
	issue string,
) localapi.StatusResponse {
	t.Helper()
	runtime := localapi.RuntimeStatusSnapshot{}
	switch state {
	case "ready":
		runtime.Running, runtime.Ready, runtime.Healthy = true, true, true
	case "starting":
		runtime.Healthy = true
	case "recovering":
		runtime.Running, runtime.Healthy, runtime.Recovering = true, true, true
	case "retrying":
		runtime.Running, runtime.Healthy = true, true
	case "failed":
		runtime.Issue = issue
	default:
		t.Fatalf("unsupported test Runtime state %q", state)
	}
	response, err := localapi.NewStatusResponse(localapi.StatusSnapshot{
		AssetRevision: revision, ActivationReady: activationReady, Runtime: runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func doctorTestActivationStatus(t *testing.T, revision, issue string) localapi.StatusResponse {
	t.Helper()
	response, err := localapi.NewStatusResponse(localapi.StatusSnapshot{
		AssetRevision: revision, ActivationIssue: issue,
		Runtime: localapi.RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeDoctorReport(t *testing.T, output string) doctorReport {
	t.Helper()
	if output == "" || !strings.HasSuffix(output, "\n") || strings.Count(output, "\n") != 1 {
		t.Fatalf("doctor output is not one line: %q", output)
	}
	var report doctorReport
	if err := json.Unmarshal([]byte(strings.TrimSuffix(output, "\n")), &report); err != nil {
		t.Fatalf("decode doctor report: %v", err)
	}
	if !validDoctorReport(report) {
		t.Fatalf("doctor output is not closed: %#v", report)
	}
	want, err := model.CanonicalMarshal(report)
	if err != nil || output != string(append(want, '\n')) {
		t.Fatalf("doctor output is not canonical: %q", output)
	}
	return report
}
