# Review case

This case is a bounded, one-to-one generator--critic exchange. `kind` values
below are opaque case vocabulary. Only the listed R7 consequences have machine
meaning.

## Actors and fixture rule

- `implementer` owns the initial responsibility and produces candidates.
- `reviewer` checks the exact Artifact bytes received through peer delivery.
- For this fixture, `total=42` is accepted. Any other total receives the exact
  contents of `artifacts/rework.txt`.
- At most one revision is requested.

## Event vocabulary

| Opaque kind | Closed consequence | Meaning in this case |
|---|---|---|
| `review.request` | `handling.create` | Send a candidate to `reviewer`; retain `self` as the local anchor. |
| `review.rework` | `handling.advance` | Return the rework Artifact to `implementer`; the current Handling remains the local anchor. |
| `review.revision` | `handling.advance` | Return the revised candidate to `reviewer`; the current Handling remains the local anchor. |
| `review.accept` | `handling.advance` | Return the acceptance Artifact to `implementer`; the current Handling remains the local anchor. |
| `review.done` | `handling.resolve.completed` | Close one local responsibility with the exact Artifact that proves its result. |

Every remote-directed root action includes both `self` and the remote target.
Every remote reply uses `handling.advance`; its current open Handling is the
required local responsibility anchor. After the peer-visible result is
accepted, each actor explicitly resolves its remaining local Handlings. A
transport acknowledgment, Runtime exit, or final text never resolves them.

## Deterministic trace and oracle

1. `implementer` sends `candidate-v1.txt`; `reviewer` returns `rework.txt`.
2. `implementer` sends `candidate-v2.txt`; `reviewer` returns `acceptance.txt`.
3. Both nodes explicitly drain their local responsibilities with
   `review.done` and a verified Artifact.

The case passes only when the reviewer reads both candidates from its local
CAS, the response bytes exactly match the fixtures, v1 is not accepted, v2 is
accepted once, every completed Handling cites a verified Artifact, and replay
of any submitted operation creates no additional Event or Handling.
