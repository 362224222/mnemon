#!/bin/sh
set -eu

# This deterministic Host policy is copied only into Hermetic Node A; it is not
# in the candidate image and is never copied into Live Nodes. It performs real
# workspace edits and public tests after a genuine remote delivery reaches the
# initiator. Hidden oracles remain outside every Node.
current=${1:-}
test "${R5_NODE_ALIAS:-}" = A
test -f "$current"
jail=$(pwd -P)
jq -e '.status == "actionable" and .action_work.local_role == "initiator" and
  .source_event.event_type == "review.delivered"' "$current" >/dev/null
install -d -m 0700 result
install -d -m 0700 .r5/go-cache .r5/go-tmp .r5/tmp
export GOCACHE="$jail/.r5/go-cache"
export GOTMPDIR="$jail/.r5/go-tmp"
export TMPDIR="$jail/.r5/tmp"
umask 077

case "${R5_SCENARIO:-}" in
  payment-review)
    cat >case/payment.go <<'EOF'
package payment

import (
	"errors"
	"fmt"
	"sync"
)

var ErrInvalidCharge = errors.New("invalid charge")

type Charge struct {
	ID    string
	Cents int64
}

type Processor struct {
	mu      sync.Mutex
	next    uint64
	charges map[string]Charge
}

func NewProcessor() *Processor {
	return &Processor{charges: make(map[string]Charge)}
}

func (p *Processor) Charge(idempotencyKey string, cents int64) (Charge, error) {
	if idempotencyKey == "" || cents <= 0 {
		return Charge{}, ErrInvalidCharge
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if prior, ok := p.charges[idempotencyKey]; ok {
		if prior.Cents != cents {
			return Charge{}, ErrInvalidCharge
		}
		return prior, nil
	}
	p.next++
	charge := Charge{ID: fmt.Sprintf("ch_%06d", p.next), Cents: cents}
	p.charges[idempotencyKey] = charge
	return charge, nil
}

func (p *Processor) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.charges)
}
EOF
    (cd case && gofmt -w payment.go && go test -race ./...)
    git diff -- case/payment.go >result/final.diff
    test -s result/final.diff
    cat >result/review-summary.json <<'EOF'
{"consumer_review":"pass","ledger_review":"pass","rework_count":1,"security_review":"pass","status":"verified"}
EOF
    ;;
  api-sdk-contract)
    cat >case/pagination.go <<'EOF'
package pagination

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

var ErrInvalidCursor = errors.New("invalid cursor")

type Cursor struct {
	Offset int `json:"offset"`
}

func EncodeCursor(cursor Cursor, signingKey []byte) (string, error) {
	if cursor.Offset < 0 || len(signingKey) == 0 {
		return "", ErrInvalidCursor
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func DecodeCursor(token string, signingKey []byte) (Cursor, error) {
	if token == "" || len(signingKey) == 0 {
		return Cursor{}, ErrInvalidCursor
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return Cursor{}, ErrInvalidCursor
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return Cursor{}, ErrInvalidCursor
	}
	var cursor Cursor
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.Offset < 0 {
		return Cursor{}, ErrInvalidCursor
	}
	return cursor, nil
}
EOF
    jq '.components = {schemas:{Cursor:{type:"string"},Problem:{type:"object",required:["code","message"],properties:{code:{type:"string"},message:{type:"string"}}}}}' \
      case/openapi.json >.r5/openapi.json
    mv .r5/openapi.json case/openapi.json
    (cd case && gofmt -w pagination.go && go test -race ./...)
    cat >result/review-summary.json <<'EOF'
{"compatibility":"pass","consumer":"pass","documentation":"pass","security":"pass","status":"verified"}
EOF
    cat >result/release-notes.md <<'EOF'
Signed opaque cursors now reject wrong keys and tampering. The OpenAPI contract documents stable cursor and problem shapes.
EOF
    ;;
  offline-incident)
    cat >case/replay.sh <<'EOF'
#!/bin/sh
set -eu
if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  echo "usage: replay.sh EVENTS_NDJSON" >&2
  exit 2
fi
jq -rs '
  reduce (.[] | select(.kind == "charge")) as $event
    ({seen:{}, rows:[]};
     if .seen[$event.request] then .
     else .seen[$event.request] = true |
       .rows += [[$event.request, ($event.amount | tostring)]] end) |
  .rows[] | @tsv
' "$1" | while IFS="$(printf '\t')" read -r request amount; do
  printf '%s %s\n' "$request" "$amount"
done
EOF
    chmod 0755 case/replay.sh
    (cd case && ./test_replay.sh)
    cat >result/incident-report.json <<'EOF'
{"consumer_review":"pass","recovery_review":"pass","regression_replay":"pass","remediation":"Deduplicate effects by immutable request identity before replay.","root_cause":"A response timeout retried an already committed request.","security_review":"pass","status":"verified"}
EOF
    ;;
  parallel-hardening)
    cat >case/upload.go <<'EOF'
package upload

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var ErrInvalidName = errors.New("invalid upload name")

func Save(root, name string, body io.Reader) (path string, result error) {
	if root == "" || name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name ||
		name == "." || name == ".." || filepath.Base(name) != name {
		return "", ErrInvalidName
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(root, ".upload-*")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	defer func() {
		if result != nil {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return "", err
	}
	buffer := make([]byte, 32*1024)
	if _, err := io.CopyBuffer(temporary, body, buffer); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	destination := filepath.Join(root, name)
	if err := os.Rename(temporaryName, destination); err != nil {
		return "", err
	}
	return destination, nil
}
EOF
    (cd case && gofmt -w upload.go && go test -race ./...)
    cat >result/hardening-report.json <<'EOF'
{"consumer":"pass","deployment":"pass","max_buffer_bytes":32768,"performance":"pass","security":"pass","status":"pass"}
EOF
    ;;
  overlapping-channels)
    jq '.security_reviewed = true | .deployment_reviewed = true' case/release.json >.r5/release.json
    mv .r5/release.json case/release.json
    (cd case && ./check-release.sh)
    cat >result/release-bundle.json <<'EOF'
{"api":"pass","causality":["A:alpha:C","C:beta:E","E:gamma:F","E:beta:C","C:alpha:A"],"consumer":"pass","dependency":"pass","deployment":"pass","reviewers":["B","C","D","E","F"],"security":"pass","status":"ready"}
EOF
    cat >result/verification.md <<'EOF'
Independent API, consumer, dependency, security, and deployment reviews followed the explicit three-Channel causality path.
EOF
    ;;
  *) exit 2 ;;
esac
