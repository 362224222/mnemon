# mnemon-harness R7 Quickstart

R7 is experimental and Pi-first. Build the two Harness binaries from the
repository root:

```sh
go -C harness build -o ../mnemon-harness ./cmd/mnemon-harness
go -C harness build -o ../mnemond ./cmd/mnemond
```

Put both binaries on `PATH` before starting Pi.

## One local Agent

From the physical project directory, run setup once:

```sh
mnemon-harness setup --runtime pi --project-root .
```

Setup provisions `.mnemon/harness/node/`, ensures the local daemon, and installs
two owned Pi projections:

```text
.pi/extensions/mnemond.ts
.pi/skills/mnemond/SKILL.md
```

Now use Pi normally. At an eligible turn boundary the fixed Hook cue tells the
Agent that bounded mnemond state is available. The installed guide teaches one
loop:

```text
View -> Intent -> Receipt -> View'
```

There is no per-task governance command for the user. Provider selection and
credentials stay in Pi's own configuration; mnemond never needs them.

## Two remote Agents

Prepare each node before setup, using an address reachable by the other node.
Each command prints that node's public Peer Card:

```sh
# Node A
mnemon-harness peer prepare \
  --listen 0.0.0.0:7447 --advertise node-a.example:7447 \
  --project-root /work/a > node-a.card.json

# Node B
mnemon-harness peer prepare \
  --listen 0.0.0.0:7447 --advertise node-b.example:7447 \
  --project-root /work/b > node-b.card.json
```

Exchange the public cards out of band, then enroll one stable local alias at
each node:

```sh
mnemon-harness peer enroll --alias node-b --project-root /work/a \
  < node-b.card.json
mnemon-harness peer enroll --alias node-a --project-root /work/b \
  < node-a.card.json
```

Finish the once-per-workspace Pi setup:

```sh
mnemon-harness setup --runtime pi --project-root /work/a
mnemon-harness setup --runtime pi --project-root /work/b
```

The two nodes still have separate authority. Sending to `node-b` creates a
durable outbound candidate while retaining local responsibility; Node B creates
its own Event and Handling only after authenticating, fetching and verifying
required Artifacts, and re-admitting the delivery.

## Verify the implementation

Repository maintainers run:

```sh
make test
make test-integration
```

During local iteration, run the focused Go package or scenario being changed.
The deterministic and integration levels are deliberately separate and do not
invoke one another.

The direct suites prove the ten R7 invariants, local continuity, federated
re-admission, and the data-only collaboration cases. A plain Go architecture
test keeps case vocabulary, fixtures, and the optional R8 selector outside the
R7 Core dependency graph.
