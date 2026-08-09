# R8 Selector Evaluation — Historical Record

Status: retired experiment. This document preserves the falsification results
that justified removing the unwired R8 selector implementation during S0.
It does not define a current product capability or protocol guarantee.

## Simulator result

R8 evaluated a sampled, cumulative-margin binary selector. The frozen profile
used `k=5`, `alpha=3`, cumulative margin `tau=4`, and at most 12 rounds. The
matrix covered `N=32/64`, exact 50/50 and rounded 55/45 populations, temporary
partitions, and a fault population of `floor(N/10)`. A separate `N=128`
characterization covered refusal, double-vote equivocation, requester-specific
strategic votes, and longer partitions.

The decisive no-fault counterexample was:

```text
N=32, 50/50, seed=190608936
threshold A=1, threshold B=31, opposite threshold observations=true
```

Disjoint holdout results remained adverse:

- `N=128`, 55/45, no injected fault produced opposite local threshold
  observations in 2 of 8 trials.
- `N=128`, 55/45, 20% strategic faults produced them in 5 of 8 trials.
- `N=128`, 50/50, partitions lasting through `tau` left inconclusive nodes in
  all 8 trials and opposite local threshold observations in 3 of 8 trials.

A local `threshold_reached` observation therefore did not establish agreement,
consensus, finality, truth, or BFT safety. The experiment was a bounded
characterization, not an activation gate or proof.

## Real-network boundary result

A separate five-container experiment showed only that a removable selector
adapter could cross process and network boundaries without becoming a second
authority plane. Each participant owned its filesystem, provisioned Agency
authority, private selector database, attempt ledger, and Ed25519 identity.

Queries were canonical, signed, bounded, and single-attempt. Missing responses
were no-votes and were not replaced. The receiver authenticated the signature
independently from the claimed source. The gate demonstrated one authenticated
recolor, rejection of an unknown selection and mismatched signer, and restart
persistence. It did not demonstrate agreement, finality, consensus, BFT
safety, or production network scale.

## Lasting boundary

Any future selector proposal must begin from these negative results. It must
not treat a local threshold observation as a global fact, mutate Agency
authority implicitly, or infer consensus from one sampled recolor. A new
proposal needs a concrete product use case, reviewed safety claim,
independently frozen profile, adversarial and holdout evidence, and a
deletion-safe integration boundary.
