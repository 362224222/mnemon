package main

import (
	"errors"
	"fmt"
	"strings"
)

func validateWorld(report liveReport) error {
	if len(report.World.Episodes) != 2 || report.World.Episodes[0].ID != "episode-1" ||
		report.World.Episodes[1].ID != "episode-2" {
		return errors.New("sanitized live report does not contain the two ordered episodes")
	}
	prefixes := make(map[string]struct{}, 6)
	for index, episode := range report.World.Episodes {
		expectedRoute := []string{"east", "west"}[index]
		if err := validateEpisode(episode, expectedRoute); err != nil {
			return fmt.Errorf("%s: %w", episode.ID, err)
		}
		for _, summary := range []loadSummary{episode.Baseline, episode.Recovery, episode.Stability} {
			if summary.Prefix == "" {
				return errors.New("sanitized live report has an empty load identity")
			}
			if _, duplicate := prefixes[summary.Prefix]; duplicate {
				return errors.New("sanitized live report reuses a load identity")
			}
			prefixes[summary.Prefix] = struct{}{}
		}
	}
	return validateGlobalChargeIdentity(report.World.Episodes)
}

func validateGlobalChargeIdentity(episodes []episodeReport) error {
	sequences := make(map[int64]struct{})
	attempts := make(map[string]struct{})
	for _, episode := range episodes {
		for _, charges := range []domainChargeResult{episode.IncidentCharges,
			episode.RecoveryCharges, episode.StabilityCharges} {
			for _, charge := range charges.Result {
				if _, duplicate := sequences[charge.Sequence]; duplicate {
					return errors.New("sanitized live report reuses a global ledger sequence")
				}
				if _, duplicate := attempts[charge.AttemptKey]; duplicate {
					return errors.New("sanitized live report reuses a global attempt identity")
				}
				sequences[charge.Sequence] = struct{}{}
				attempts[charge.AttemptKey] = struct{}{}
			}
		}
	}
	return nil
}

func validateEpisode(episode episodeReport, expectedRoute string) error {
	baseline := episode.Baseline
	if baseline.Sent != 4 || baseline.Accepted != 4 || baseline.Failed != 0 ||
		len(baseline.Receipts) != 4 || baseline.Observed.Gateway.Route != expectedRoute ||
		baseline.Observed.Ledger != (ledgerStatus{
			Charges: 8, ActiveCharges: 8, UniqueBusinesses: 4, DuplicateBusinesses: 4,
		}) {
		return errors.New("sanitized live report does not prove the initial incident")
	}
	if !validFreshSummary(episode.Recovery, 6) ||
		!validFreshSummary(episode.Stability, 6) {
		return errors.New("sanitized live report does not prove recovery and stability")
	}
	incident := episode.IncidentAfter
	if incident.Role != "data" || incident.Result != (ledgerStatus{
		Charges: 8, ActiveCharges: 4, VoidedCharges: 4,
		UniqueBusinesses: 4, DuplicateBusinesses: 0,
	}) {
		return errors.New("sanitized live report does not prove historical reconciliation")
	}
	checks := []struct {
		summary loadSummary
		charges domainChargeResult
		copies  int
		voided  int
	}{
		{episode.Baseline, episode.IncidentCharges, 2, 1},
		{episode.Recovery, episode.RecoveryCharges, 1, 0},
		{episode.Stability, episode.StabilityCharges, 1, 0},
	}
	for _, check := range checks {
		if err := validateServiceReceiptEvidence(check.summary, check.charges,
			check.copies, check.voided); err != nil {
			return err
		}
	}
	return nil
}

func validFreshSummary(summary loadSummary, count int) bool {
	if summary.Prefix == "" || summary.Sent != count || summary.Accepted != count ||
		summary.Failed != 0 || len(summary.Receipts) != count ||
		summary.Observed.Ledger != (ledgerStatus{
			Charges: count, ActiveCharges: count, UniqueBusinesses: count,
		}) {
		return false
	}
	seen := make(map[string]struct{}, count)
	for _, receipt := range summary.Receipts {
		if !strings.HasPrefix(receipt.BusinessID, summary.Prefix+"-") || receipt.CaptureID <= 0 {
			return false
		}
		if _, duplicate := seen[receipt.BusinessID]; duplicate {
			return false
		}
		seen[receipt.BusinessID] = struct{}{}
	}
	return true
}
