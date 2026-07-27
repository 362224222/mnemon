package corecontract

import (
	"fmt"
	"os"
	"path/filepath"
)

type gateBundleIdentity struct {
	reference, digest string
}

func completedGateBundle(root, runtimeName, runID string, source GateSource,
	pairedRun string,
) (GateBundleRef, gateBundleIdentity, error) {
	base := filepath.ToSlash(filepath.Join(".testdata", "r5", "runs", runID))
	reportPath := base + "/report.json"
	manifestPath := base + "/manifest.json"
	reportData, reportDigest, err := readPrivateGateFile(root, reportPath)
	if err != nil {
		return GateBundleRef{}, gateBundleIdentity{}, err
	}
	var suite runtimeSuiteReport
	if err := decodeStrictJSON(reportData, &suite); err != nil {
		return GateBundleRef{}, gateBundleIdentity{},
			fmt.Errorf("decode completed %s suite: %w", runtimeName, err)
	}
	if !completedSuiteIdentity(suite, runtimeName, runID, source, pairedRun) {
		return GateBundleRef{}, gateBundleIdentity{},
			fmt.Errorf("completed %s suite identity or verdict is invalid", runtimeName)
	}
	_, manifestDigest, err := readPrivateGateFile(root, manifestPath)
	if err != nil {
		return GateBundleRef{}, gateBundleIdentity{}, err
	}
	ref := GateBundleRef{
		Runtime: runtimeName, RunID: runID, ReportPath: reportPath,
		ReportSHA256: reportDigest, ManifestPath: manifestPath,
		ManifestSHA256: manifestDigest,
	}
	return ref, gateBundleIdentity{
		reference: suite.Image.Reference, digest: suite.Image.Digest,
	}, nil
}

func completedSuiteIdentity(suite runtimeSuiteReport, runtimeName, runID string,
	source GateSource, pairedRun string,
) bool {
	if suite.SchemaVersion != 1 || suite.BundleKind != "single-case" ||
		suite.RunID != runID || suite.Runtime != runtimeName || suite.Status != "passed" ||
		suite.GitSHA != source.Commit || suite.Image.Reference == "" ||
		suite.Image.Revision != source.Commit || suite.Image.SourceTree != source.Tree ||
		!digestPattern.MatchString(suite.Image.Digest) {
		return false
	}
	if runtimeName == "scripted" {
		return suite.PairedHermeticRun == nil
	}
	return suite.PairedHermeticRun != nil && *suite.PairedHermeticRun == pairedRun
}

func readPrivateGateFile(root, relative string) ([]byte, string, error) {
	filename := filepath.Join(root, filepath.FromSlash(relative))
	info, err := os.Lstat(filename)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 {
		return nil, "", fmt.Errorf("Core gate runtime file %s is not private and regular", relative)
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, "", err
	}
	return data, bytesDigest(data), nil
}
