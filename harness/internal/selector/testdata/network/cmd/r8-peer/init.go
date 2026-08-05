package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
	"github.com/mnemon-dev/mnemon/harness/internal/cas"
	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

type initOptions struct {
	stateDirectory string
	projectRoot    string
	configPath     string
	self           string
	preference     string
}

func runInit(ctx context.Context, args []string) error {
	options, err := parseInitOptions(args)
	if err != nil {
		return err
	}
	config, err := loadConfig(options.configPath)
	if err != nil {
		return err
	}
	self, err := requireSelfIdentity(config, options.self, options.stateDirectory)
	if err != nil {
		return err
	}
	preference, err := selector.ParsePreference(options.preference)
	if err != nil {
		return err
	}
	seed, err := admitSeedOpinion(ctx, options.projectRoot, config.descriptor, preference)
	if err != nil {
		return err
	}
	store, err := selector.OpenStore(ctx, filepath.Join(options.stateDirectory, databaseName))
	if err != nil {
		return err
	}
	defer store.Close()
	if _, err := store.CreateOwnerSelection(ctx, config.descriptor, self.id); err != nil {
		return err
	}
	snapshot, err := store.SeedSelection(ctx, config.descriptor.ID(), seed)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, projectInitSnapshot(snapshot, seed))
}

func parseInitOptions(args []string) (initOptions, error) {
	options := initOptions{}
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&options.stateDirectory, "state-dir", "", "private selector state directory")
	flags.StringVar(&options.projectRoot, "project-root", "", "R7 workspace root")
	flags.StringVar(&options.configPath, "config", "", "frozen selector config")
	flags.StringVar(&options.self, "id", "", "local participant ID")
	flags.StringVar(&options.preference, "preference", "", "accepted local preference")
	if err := flags.Parse(args); err != nil {
		return initOptions{}, err
	}
	if flags.NArg() != 0 {
		return initOptions{}, errors.New("init accepts no positional arguments")
	}
	if err := requireValues(options.stateDirectory, options.projectRoot, options.configPath,
		options.self, options.preference); err != nil {
		return initOptions{}, err
	}
	return options, nil
}

func requireSelfIdentity(config runtimeConfig, selfValue, stateDirectory string) (peerRuntime, error) {
	self, err := config.peer(selfValue)
	if err != nil {
		return peerRuntime{}, err
	}
	private, err := loadPrivateKey(stateDirectory)
	if err != nil {
		return peerRuntime{}, err
	}
	public := private.Public().(ed25519.PublicKey)
	if !bytes.Equal(public, self.key) {
		return peerRuntime{}, fmt.Errorf("private key does not match frozen identity %s", selfValue)
	}
	return self, nil
}

// admitSeedOpinion uses the same CAS, authority store, binding, and Receipt
// types as mnemond. It runs before mnemond starts, closes the sole writer, and
// leaves the exact accepted root Event for that same daemon to adopt.
func admitSeedOpinion(ctx context.Context, projectRoot string, descriptor selector.SelectionDescriptor,
	preference selector.Preference,
) (selector.AcceptedSeedOpinion, error) {
	provisioned, err := daemon.Provision(ctx, projectRoot)
	if err != nil {
		return selector.AcceptedSeedOpinion{}, err
	}
	objects, err := cas.OpenExisting(filepath.Join(provisioned.StateDirectory(), "objects", "sha256"))
	if err != nil {
		return selector.AcceptedSeedOpinion{}, err
	}
	store, err := authority.OpenExistingWithArtifactVerifier(ctx,
		filepath.Join(provisioned.StateDirectory(), "agency.db"), objects)
	if err != nil {
		return selector.AcceptedSeedOpinion{}, err
	}
	if err := store.RequirePrincipal(ctx, provisioned.Principal()); err != nil {
		_ = store.Close()
		return selector.AcceptedSeedOpinion{}, err
	}
	opinion, err := selector.NewSeedOpinion(descriptor.ID(), preference)
	if err != nil {
		_ = store.Close()
		return selector.AcceptedSeedOpinion{}, err
	}
	if err := captureSeedArtifacts(ctx, objects, store, descriptor, opinion); err != nil {
		_ = store.Close()
		return selector.AcceptedSeedOpinion{}, err
	}
	request, receipt, err := admitSeedIntent(ctx, store, provisioned.Principal(), descriptor, opinion)
	if err != nil {
		_ = store.Close()
		return selector.AcceptedSeedOpinion{}, err
	}
	seed, bindErr := selector.BindAcceptedSeedOpinion(request, receipt, descriptor, opinion)
	return seed, errors.Join(bindErr, store.Close())
}

func captureSeedArtifacts(ctx context.Context, objects *cas.Store, store *authority.Store,
	descriptor selector.SelectionDescriptor, opinion selector.SeedOpinion,
) error {
	artifacts := []struct {
		digest  agency.Digest
		content []byte
	}{
		{digest: descriptor.ID().Digest(), content: descriptor.CanonicalBytes()},
		{digest: opinion.Digest(), content: opinion.CanonicalBytes()},
	}
	for _, artifact := range artifacts {
		if agency.Sum(artifact.content) != artifact.digest {
			return errors.New("R8 seed Artifact digest does not match its canonical bytes")
		}
		if _, err := objects.Put(ctx, artifact.digest, artifact.content); err != nil {
			return err
		}
		verified, err := authority.VerifyArtifact(artifact.content, time.Now())
		if err != nil {
			return err
		}
		if err := store.CatalogArtifact(ctx, verified); err != nil {
			return err
		}
	}
	return nil
}

func admitSeedIntent(ctx context.Context, store *authority.Store, principal agency.AgentPrincipalID,
	descriptor selector.SelectionDescriptor, opinion selector.SeedOpinion,
) (agency.BoundIntent, agency.Receipt, error) {
	boundary := agency.Sum([]byte("r8-seed-boundary:" + descriptor.ID().Digest().String()))
	proof, err := store.IssueInteractiveAttachment(ctx, principal, boundary)
	if err != nil {
		return agency.BoundIntent{}, agency.Receipt{}, err
	}
	currentKey, err := agency.NewOperationKey("operation.r8.seed.current")
	if err != nil {
		return agency.BoundIntent{}, agency.Receipt{}, err
	}
	currentOperation, err := authority.NewCurrentOperation(currentKey)
	if err != nil {
		return agency.BoundIntent{}, agency.Receipt{}, err
	}
	view, err := store.Current(ctx, proof, currentOperation)
	if err != nil {
		return agency.BoundIntent{}, agency.Receipt{}, err
	}
	request, err := bindSeedIntent(view, descriptor, opinion)
	if err != nil {
		return agency.BoundIntent{}, agency.Receipt{}, err
	}
	result, err := store.Admit(ctx, proof, request)
	if err != nil {
		return agency.BoundIntent{}, agency.Receipt{}, err
	}
	receipt, err := agency.ParseReceiptCanonicalJSON(result.ReceiptJSON())
	if err != nil || receipt.Outcome() != agency.ReceiptOutcomeAccepted {
		return agency.BoundIntent{}, agency.Receipt{}, errors.New("R7 seed Event was not accepted")
	}
	return request, receipt, nil
}

func bindSeedIntent(view authority.BoundView, descriptor selector.SelectionDescriptor,
	opinion selector.SeedOpinion,
) (agency.BoundIntent, error) {
	kind, err := agency.NewSemanticLabel("selection.seed")
	if err != nil {
		return agency.BoundIntent{}, err
	}
	payload, err := agency.NewSemanticPayload("local preference")
	if err != nil {
		return agency.BoundIntent{}, err
	}
	descriptorHandle, err := agency.NewOpaqueHandle("candidate.r8.seed.descriptor")
	if err != nil {
		return agency.BoundIntent{}, err
	}
	descriptorInput, err := agency.NewArtifactCandidate(descriptorHandle)
	if err != nil {
		return agency.BoundIntent{}, err
	}
	opinionHandle, err := agency.NewOpaqueHandle("candidate.r8.seed.opinion")
	if err != nil {
		return agency.BoundIntent{}, err
	}
	opinionInput, err := agency.NewArtifactCandidate(opinionHandle)
	if err != nil {
		return agency.BoundIntent{}, err
	}
	intent, err := agency.NewAgentIntent(agency.IntentSpec{Kind: kind, Payload: payload,
		Consequence: agency.ConsequenceCreateHandlings, Successors: []agency.TargetRef{agency.SelfTarget()},
		Artifacts: []agency.ArtifactInput{descriptorInput, opinionInput}})
	if err != nil {
		return agency.BoundIntent{}, err
	}
	operation, err := agency.NewOperationKey("operation.r8.seed.admit")
	if err != nil {
		return agency.BoundIntent{}, err
	}
	descriptorCandidate, err := agency.NewCapturedCandidate(operation, descriptorInput,
		descriptor.ID().Digest())
	if err != nil {
		return agency.BoundIntent{}, err
	}
	opinionCandidate, err := agency.NewCapturedCandidate(operation, opinionInput, opinion.Digest())
	if err != nil {
		return agency.BoundIntent{}, err
	}
	return view.Bind(intent, operation,
		[]agency.CapturedCandidate{descriptorCandidate, opinionCandidate})
}
