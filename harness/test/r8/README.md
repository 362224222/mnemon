# R8 falsifiable simulator

This directory owns test-only R8 runner assets and results. The Go simulator
lives inside the removable module at `internal/selector/simtest`; it exercises
the exported selector API without adding a simulator, fallback, or scenario
policy to production code.

The experiment freezes one deliberately small profile before interpreting its
results:

| Parameter | Value |
|---|---:|
| sampled `k` | 5 |
| sampled `alpha` | 3 |
| cumulative margin `tau` | 4 |
| maximum / Slush rounds | 12 |
| base-matrix partition duration | first 3 rounds |
| base-matrix fault population | `floor(N / 10)` |
| characterization seeds | `190608936`, `240102811`, `20260803` |
| disjoint holdout seeds | 8 fixed seeds |

The base matrix covers `N=32/64`, an exact 50/50 split, and a 55/45 target split.
Because those node counts cannot represent 55% exactly, the A population is
`ceil(0.55 * N)`. Initial colors and faulty participants are independently
shuffled from each fixed seed. Separate adversarial characterization covers
`N=128`, 20% refusal, double-vote equivocation, requester-specific single-vote
behavior, and partitions that last through `tau`.

Each active selector sends one query to every frozen sample member. A normal
reply counts as one additional message, refusal counts as no reply, and an
equivocator sends both A and B frames. A strategic peer sends one authenticated
vote but tailors it to the requester's current preference. During a temporary partition, sampled
peers in the other half do not reply and are not replaced. Terminal selector
nodes stop polling but continue answering later samples with their frozen
preference.

Two controls isolate the mechanism:

- **all-to-all census** runs the same cumulative-margin selector through the
  same public API with `k=N-1` and the minimum strict majority. It isolates the
  message-cost effect of sampling.
- **fixed-round pure Slush** uses the same five-peer sample schedule and fault
  model, but has no cumulative margin or local threshold observation. Its A/B
  numbers are final colors after round 12, not proof of stability.

The allowance is intentionally honest rather than favorable. The frozen
profile permits at most two opposite local threshold observations and two
inconclusive nodes per run; 55/45 must leave at least `N-2` threshold A
observations. A specific no-fault counterexample is frozen in the test:

```text
N=32, 50/50, seed=190608936
threshold A=1, threshold B=31, opposite threshold observations=true
```

Therefore this experiment refutes any claim that a local
`threshold_reached` observation establishes agreement, consensus, finality, or
BFT. It only characterizes the current profile while measuring its rounds and
message cost. Changing the profile or allowances is a new reviewed experiment,
not a repair for an inconvenient result.

## Simulation scope

The R8 simulator is a fixed-profile characterization, not an activation gate,
an agreement proof, or a BFT claim. The disjoint holdout corpus intentionally
preserves adverse distributions rather than tuning the profile to them:

- N=128, 55/45, no injected fault has opposite local threshold observations in
  2/8 trials.
- N=128, 55/45, strategic 20% faults has them in 5/8 trials.
- N=128, 50/50, partitions lasting `tau` leave inconclusive nodes in 8/8
  trials and opposite local threshold observations in 3/8.

These results bound what R8 may claim. They do not authorize automatic
adoption, R7 mutation, consensus, finality, or BFT.

Run it with:

```sh
go -C harness test ./internal/selector/simtest -count=1
go -C harness test -race ./internal/selector/simtest -count=1
```
