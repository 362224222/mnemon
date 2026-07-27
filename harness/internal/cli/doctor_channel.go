package cli

import "github.com/mnemon-dev/mnemon/harness/internal/localapi"

func newOnlineDoctorReport(checks doctorChecks, status localapi.StatusResponse,
	exit int,
) doctorReport {
	report := newDoctorReportWithChannels(doctorModeOnline, checks, status.Channels, exit)
	if status.ArtifactTransfer != nil {
		artifactTransfer := *status.ArtifactTransfer
		report.ArtifactTransfer = &artifactTransfer
	}
	return report
}

func doctorChannelCheck(channels []localapi.StatusChannel) (doctorCheck, int) {
	if localapi.ValidateStatusChannels(channels) != nil {
		return failedDoctorCheck(doctorCheckNames[6], doctorIssueChannelDegraded,
			doctorRemedyDoctor), 1
	}
	queued := false
	for _, channel := range channels {
		switch channel.State {
		case "degraded":
			return failedDoctorCheck(doctorCheckNames[6], doctorIssueChannelDegraded,
				doctorRemedyDoctor), 1
		case "queued":
			queued = true
		}
	}
	if queued {
		return failedDoctorCheck(doctorCheckNames[6], doctorIssueChannelQueued,
			doctorRemedyRetry), 5
	}
	return passedDoctorCheck(doctorCheckNames[6]), 0
}

func validDoctorChannelReport(report doctorReport) bool {
	check, _ := doctorChannelCheck(report.Channels)
	return report.Checks[6] == check
}

func validDoctorReport(report doctorReport) bool {
	if report.SchemaVersion != localapi.SchemaVersion || report.Scope != "managed_agent" ||
		(report.Mode != doctorModeOnline && report.Mode != doctorModeOffline && report.Mode != doctorModeUnknown) ||
		(report.Status != doctorHealthy && report.Status != doctorDegraded &&
			report.Status != doctorInconclusive) || len(report.Checks) != len(doctorCheckNames) {
		return false
	}
	for index, check := range report.Checks {
		if check.Name != doctorCheckNames[index] || !validDoctorCheck(index, check) {
			return false
		}
	}
	if !validDoctorMode(report) {
		return false
	}
	exit := doctorReportExit(report)
	if exit == 0 && !allDoctorChecksPass(report.Checks) {
		return false
	}
	wantStatus := doctorHealthy
	if report.Mode == doctorModeUnknown {
		wantStatus = doctorInconclusive
	} else if exit != 0 {
		wantStatus = doctorDegraded
	}
	return report.Status == wantStatus
}

func validDoctorMode(report doctorReport) bool {
	switch report.Mode {
	case doctorModeOnline:
		return report.ArtifactTransfer != nil &&
			localapi.ValidateStatusArtifactTransfer(*report.ArtifactTransfer) == nil &&
			report.Checks[4] == passedDoctorCheck(doctorCheckNames[4]) &&
			report.Checks[5].Status != doctorUnobserved && report.Checks[6].Status != doctorUnobserved &&
			report.Status != doctorInconclusive && validDoctorChannelReport(report)
	case doctorModeOffline:
		return report.ArtifactTransfer == nil &&
			report.Checks[4] == failedDoctorCheck(doctorCheckNames[4], doctorIssueDaemon,
				doctorRemedyRetry) && report.Checks[5] == unobservedDoctorCheck(doctorCheckNames[5],
			doctorIssueRuntimeUnobserved, doctorRemedyRetry) &&
			report.Checks[6] == unobservedDoctorCheck(doctorCheckNames[6],
				doctorIssueChannelUnobserved, doctorRemedyRetry) && len(report.Channels) == 0 &&
			report.Status == doctorDegraded
	case doctorModeUnknown:
		return validUnknownDoctorReport(report)
	default:
		return false
	}
}

func validUnknownDoctorReport(report doctorReport) bool {
	if report.ArtifactTransfer != nil || report.Status != doctorInconclusive ||
		len(report.Channels) != 0 {
		return false
	}
	for index, check := range report.Checks {
		if check != unobservedDoctorCheck(doctorCheckNames[index],
			doctorIssueObservationUnknown, doctorRemedyDoctor) {
			return false
		}
	}
	return true
}

func validDoctorCheck(index int, check doctorCheck) bool {
	if check == passedDoctorCheck(doctorCheckNames[index]) {
		return true
	}
	unknown := unobservedDoctorCheck(doctorCheckNames[index], doctorIssueObservationUnknown,
		doctorRemedyDoctor)
	if check == unknown {
		return true
	}
	switch index {
	case 0:
		return validDoctorAuthorityCheck(check)
	case 1:
		return check == failedDoctorCheck(doctorCheckNames[index], doctorIssueAssetsUnavailable,
			doctorRemedySetup) || check == failedDoctorCheck(doctorCheckNames[index],
			doctorIssueAssetMismatch, doctorRemedySetup)
	case 2:
		return check == failedDoctorCheck(doctorCheckNames[index], doctorIssueProjection,
			doctorRemedySetup)
	case 3:
		return check == failedDoctorCheck(doctorCheckNames[index], doctorIssueRegistration,
			doctorRemedySetup)
	case 4:
		return check == failedDoctorCheck(doctorCheckNames[index], doctorIssueDaemon,
			doctorRemedyRetry)
	case 5:
		return validDoctorRuntimeCheck(check)
	case 6:
		return validDoctorChannelCheck(check)
	default:
		return false
	}
}

func validDoctorAuthorityCheck(check doctorCheck) bool {
	return check == failedDoctorCheck(doctorCheckNames[0], doctorIssueAuthorityDisabled,
		doctorRemedySetup) || check == failedDoctorCheck(doctorCheckNames[0],
		doctorIssueAuthorityStale, doctorRemedySetup) || check == failedDoctorCheck(
		doctorCheckNames[0], doctorIssueAuthorityUnavailable, doctorRemedyDoctor)
}

func validDoctorRuntimeCheck(check doctorCheck) bool {
	return check == failedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeStarting,
		doctorRemedyRetry) || check == failedDoctorCheck(doctorCheckNames[5],
		doctorIssueRuntimeRecovering, doctorRemedyRetry) || check == failedDoctorCheck(
		doctorCheckNames[5], doctorIssueRuntimeRetrying, doctorRemedyRetry) ||
		check == failedDoctorCheck(doctorCheckNames[5], doctorIssueRuntimeFailed,
			doctorRemedyDoctor) || check == unobservedDoctorCheck(doctorCheckNames[5],
		doctorIssueRuntimeUnobserved, doctorRemedyRetry)
}

func validDoctorChannelCheck(check doctorCheck) bool {
	return check == failedDoctorCheck(doctorCheckNames[6], doctorIssueChannelQueued,
		doctorRemedyRetry) || check == failedDoctorCheck(doctorCheckNames[6],
		doctorIssueChannelDegraded, doctorRemedyDoctor) || check == unobservedDoctorCheck(
		doctorCheckNames[6], doctorIssueChannelUnobserved, doctorRemedyRetry)
}

func allDoctorChecksPass(checks []doctorCheck) bool {
	for _, check := range checks {
		if check.Status != doctorPass {
			return false
		}
	}
	return true
}

func doctorReportExit(report doctorReport) int {
	exit := 0
	if report.Mode == doctorModeUnknown {
		exit = 1
	}
	for _, check := range report.Checks {
		switch check.Issue {
		case doctorIssueAuthorityUnavailable, doctorIssueRuntimeFailed, doctorIssueChannelDegraded:
			exit = mergeDoctorExit(exit, 1)
		case doctorIssueAuthorityDisabled, doctorIssueAuthorityStale,
			doctorIssueAssetsUnavailable, doctorIssueAssetMismatch, doctorIssueProjection,
			doctorIssueRegistration:
			exit = mergeDoctorExit(exit, 3)
		case doctorIssueDaemon, doctorIssueRuntimeStarting, doctorIssueRuntimeRecovering,
			doctorIssueRuntimeRetrying, doctorIssueRuntimeUnobserved, doctorIssueChannelQueued,
			doctorIssueChannelUnobserved:
			exit = mergeDoctorExit(exit, 5)
		}
	}
	return exit
}
