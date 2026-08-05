# R8 real-network boundary proof

This directory owns the Docker and runner assets for a removable, test-only
adapter. The Go command lives under
`internal/selector/testdata/network/cmd/r8-peer`, so deleting the optional
`internal/selector` island removes every Go importer with it. The proof shows
that R8 can cross real process and network boundaries without becoming an R7
package or a second authority plane.

The Docker gate starts five isolated containers for a frozen `k=1` profile,
which satisfies `N >= 4k+1`. Every container has:

- its own filesystem and no host or peer mount;
- one provisioned R7 node and one live sibling `mnemond` process;
- one private selector database and durable network-attempt ledger;
- one Ed25519 key whose public-key digest is its `ParticipantID`;
- the same candidate binaries and one fixed roster/profile.

Before `mnemond` starts, the adapter submits one real R7 root Intent through
CAS capture, authority admission, and an accepted Receipt. That Event cites
both the exact canonical `SelectionDescriptor` and `SeedOpinion` Artifacts.
The selector accepts the seed only from those exact durable objects. The
descriptor binds the roster, profile, window, and candidate scope; the roster
in turn binds every ParticipantID to its authentication key.

Each machine query is canonical, signed, bounded, and sent once. The receiver
authenticates the signature independently of the claimed source. A missing or
lost response is a no-vote and is never replaced or retried. Votes are handled
entirely by the machine adapter; no Agent or LLM turn is spent per vote.

The gate verifies:

- five isolated instances run exactly one live `mnemond` beside the adapter;
- `mnemond` reads the accepted R7 seed Event and both exact Artifact refs;
- peer-a starts at A while every eligible sampled peer starts at B, so one real
  signed sample produces an observed A-to-B recolor and persisted
  `PreferenceObservation`;
- an unknown selection returns an authenticated no-vote;
- a source claim signed by another participant's key is rejected;
- the observation, R7 node identity, and pending seed responsibility survive a
  full container restart.

Run it from `harness/`:

```sh
go test ./internal/selector/testdata/network/cmd/r8-peer
go test -race ./internal/selector/testdata/network/cmd/r8-peer
bash test/r8/network/runner/run_docker.sh
```

On success the runner also writes a validated, metadata-only
`mnemon.test.trace` file to `.testdata/r8-network/last.trace`. Set
`R8_NETWORK_TRACE` to choose another path, then load the file in
`test/observer/index.html`. The trace's vote counts and recolor flag come from
the test adapter while it still owns the exact frozen round and authenticated
vote set; the trace converter does not infer them from the final margin.

This is a transport, identity, provenance, and persistence proof. Its small
profile and one local recolor deliberately do **not** establish agreement,
finality, consensus, BFT safety, or production network scale. Those claims
remain refuted or unproven by the separate falsifiable simulator.
