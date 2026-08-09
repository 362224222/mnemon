package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/internal/daemon"
)

type peerCommandFixture struct {
	root       string
	provision  daemon.ProvisionResult
	card       daemon.PeerCard
	enrollment daemon.PeerEnrollment
}

func newPeerCommandFixture(t *testing.T) peerCommandFixture {
	t.Helper()
	ctx := context.Background()
	localRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks(local) error = %v", err)
	}
	remoteRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks(remote) error = %v", err)
	}
	local, err := daemon.Provision(ctx, localRoot)
	if err != nil {
		t.Fatalf("Provision(local) error = %v", err)
	}
	remote, err := daemon.Provision(ctx, remoteRoot)
	if err != nil {
		t.Fatalf("Provision(remote) error = %v", err)
	}
	if _, err := daemon.ConfigureExchange(ctx, local.StateDirectory(),
		"127.0.0.1:41001", "peer-a.invalid:41001"); err != nil {
		t.Fatalf("ConfigureExchange(local) error = %v", err)
	}
	remoteCard, err := daemon.ConfigureExchange(ctx, remote.StateDirectory(),
		"127.0.0.1:41002", "peer-b.invalid:41002")
	if err != nil {
		t.Fatalf("ConfigureExchange(remote) error = %v", err)
	}
	enrollment, err := daemon.EnrollPeer(ctx, local.StateDirectory(), "target:peer-b", remoteCard)
	if err != nil {
		t.Fatalf("EnrollPeer() error = %v", err)
	}
	return peerCommandFixture{root: localRoot, provision: local, card: remoteCard,
		enrollment: enrollment}
}

func TestRunPeerPrepareUsesOneOrderedOwnerPath(t *testing.T) {
	fixture := newPeerCommandFixture(t)
	var calls []string
	deps := peerDependencies{
		workingDirectory: func() (string, error) {
			calls = append(calls, "cwd")
			return fixture.root, nil
		},
		resolveState: func(requested string) (string, string, error) {
			calls = append(calls, "resolve:"+requested)
			return fixture.root, fixture.provision.StateDirectory(), nil
		},
		provision: func(_ context.Context, root string) (daemon.ProvisionResult, error) {
			calls = append(calls, "provision:"+root)
			return fixture.provision, nil
		},
		configure: func(_ context.Context, state, listen, advertise string) (daemon.PeerCard, error) {
			calls = append(calls, fmt.Sprintf("configure:%s:%s:%s", state, listen, advertise))
			return fixture.card, nil
		},
		parseCard: daemon.ParsePeerCardCanonicalJSON,
		enroll: func(context.Context, string, string, daemon.PeerCard) (
			daemon.PeerEnrollment, error,
		) {
			t.Fatal("prepare invoked enroll")
			return daemon.PeerEnrollment{}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runPeer(context.Background(), []string{"prepare", "--listen", "0.0.0.0:7447",
		"--advertise", "peer-a.invalid:7447"}, strings.NewReader("ignored"),
		&stdout, &stderr, deps)
	wantCalls := []string{"cwd", "resolve:" + fixture.root, "provision:" + fixture.root,
		fmt.Sprintf("configure:%s:0.0.0.0:7447:peer-a.invalid:7447",
			fixture.provision.StateDirectory())}
	if exit != 0 || stderr.Len() != 0 ||
		stdout.String() != string(fixture.card.CanonicalJSON())+"\n" ||
		!reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("prepare = exit %d stdout %q stderr %q calls %#v",
			exit, stdout.String(), stderr.String(), calls)
	}
}

func TestRunPeerEnrollParsesBeforeItsOnlyMutation(t *testing.T) {
	fixture := newPeerCommandFixture(t)
	var calls []string
	resolveCalls := 0
	deps := peerDependencies{
		workingDirectory: func() (string, error) {
			calls = append(calls, "cwd")
			return fixture.root, nil
		},
		resolveState: func(requested string) (string, string, error) {
			resolveCalls++
			calls = append(calls, fmt.Sprintf("resolve%d:%s", resolveCalls, requested))
			return fixture.root, fixture.provision.StateDirectory(), nil
		},
		provision: func(context.Context, string) (daemon.ProvisionResult, error) {
			t.Fatal("enroll invoked provision")
			return daemon.ProvisionResult{}, nil
		},
		configure: func(context.Context, string, string, string) (daemon.PeerCard, error) {
			t.Fatal("enroll invoked configure")
			return daemon.PeerCard{}, nil
		},
		parseCard: func(raw []byte) (daemon.PeerCard, error) {
			calls = append(calls, "parse:"+string(raw))
			return daemon.ParsePeerCardCanonicalJSON(raw)
		},
		enroll: func(_ context.Context, state, alias string, card daemon.PeerCard) (
			daemon.PeerEnrollment, error,
		) {
			calls = append(calls, fmt.Sprintf("enroll:%s:%s:%s", state, alias, card.PeerID()))
			return fixture.enrollment, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runPeer(context.Background(), []string{"enroll", "--alias", "target:peer-b"},
		bytes.NewReader(append(fixture.card.CanonicalJSON(), '\n')), &stdout, &stderr, deps)
	wantCalls := []string{"cwd", "resolve1:" + fixture.root, "resolve2:" + fixture.root,
		"parse:" + string(fixture.card.CanonicalJSON()),
		fmt.Sprintf("enroll:%s:target:peer-b:%s", fixture.provision.StateDirectory(),
			fixture.card.PeerID())}
	if exit != 0 || stderr.Len() != 0 ||
		stdout.String() != string(fixture.enrollment.CanonicalJSON())+"\n" ||
		!reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("enroll = exit %d stdout %q stderr %q calls %#v",
			exit, stdout.String(), stderr.String(), calls)
	}
}

func TestRunPeerRejectsMalformedCommandsBeforeDependencies(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "missing subcommand"},
		{name: "unknown subcommand", args: []string{"connect"}},
		{name: "prepare missing advertise", args: []string{"prepare", "--listen", "127.0.0.1:1"}},
		{name: "prepare dangling flag", args: []string{"prepare", "--listen"}},
		{name: "prepare duplicate", args: []string{"prepare", "--listen", "a:1", "--listen", "b:2", "--advertise", "a:1"}},
		{name: "prepare unknown", args: []string{"prepare", "--listen", "a:1", "--advertise", "a:1", "--mode", "x"}},
		{name: "prepare empty explicit root", args: []string{"prepare", "--listen", "a:1", "--advertise", "a:1", "--project-root", ""}},
		{name: "enroll missing alias", args: []string{"enroll"}},
		{name: "enroll dangling flag", args: []string{"enroll", "--alias"}},
		{name: "enroll duplicate", args: []string{"enroll", "--alias", "target:a", "--alias", "target:b"}},
		{name: "enroll unknown", args: []string{"enroll", "--alias", "target:a", "--route", "x"}},
		{name: "enroll empty explicit root", args: []string{"enroll", "--alias", "target:a", "--project-root", ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			effects := 0
			deps := peerDependencies{
				workingDirectory: func() (string, error) { effects++; return "", nil },
				resolveState:     func(string) (string, string, error) { effects++; return "", "", nil },
				provision: func(context.Context, string) (daemon.ProvisionResult, error) {
					effects++
					return daemon.ProvisionResult{}, nil
				},
				configure: func(context.Context, string, string, string) (daemon.PeerCard, error) {
					effects++
					return daemon.PeerCard{}, nil
				},
				parseCard: func([]byte) (daemon.PeerCard, error) {
					effects++
					return daemon.PeerCard{}, nil
				},
				enroll: func(context.Context, string, string, daemon.PeerCard) (
					daemon.PeerEnrollment, error,
				) {
					effects++
					return daemon.PeerEnrollment{}, nil
				},
			}
			var stdout, stderr bytes.Buffer
			exit := runPeer(context.Background(), test.args, strings.NewReader("ignored"),
				&stdout, &stderr, deps)
			if exit != 2 || stdout.Len() != 0 || stderr.Len() == 0 || effects != 0 {
				t.Fatalf("malformed peer = exit %d stdout %q stderr %q effects %d",
					exit, stdout.String(), stderr.String(), effects)
			}
		})
	}
}

func TestRunPeerStopsAtTheFirstFailedDependency(t *testing.T) {
	fixture := newPeerCommandFixture(t)
	wantErr := errors.New("dependency failed")
	for _, test := range []struct {
		name      string
		args      []string
		stdin     io.Reader
		fail      string
		wantCalls []string
		wantExit  int
	}{
		{name: "prepare resolve", args: []string{"prepare", "--listen", "a:1", "--advertise", "b:2"}, fail: "resolve1", wantCalls: []string{"cwd", "resolve1"}, wantExit: 1},
		{name: "prepare provision", args: []string{"prepare", "--listen", "a:1", "--advertise", "b:2"}, fail: "provision", wantCalls: []string{"cwd", "resolve1", "provision"}, wantExit: 1},
		{name: "prepare configure", args: []string{"prepare", "--listen", "a:1", "--advertise", "b:2"}, fail: "configure", wantCalls: []string{"cwd", "resolve1", "provision", "configure"}, wantExit: 1},
		{name: "enroll second resolve", args: []string{"enroll", "--alias", "target:b"}, stdin: bytes.NewReader(fixture.card.CanonicalJSON()), fail: "resolve2", wantCalls: []string{"cwd", "resolve1", "resolve2"}, wantExit: 1},
		{name: "enroll parse", args: []string{"enroll", "--alias", "target:b"}, stdin: strings.NewReader("invalid"), fail: "parse", wantCalls: []string{"cwd", "resolve1", "resolve2", "parse"}, wantExit: 2},
		{name: "enroll mutation", args: []string{"enroll", "--alias", "target:b"}, stdin: bytes.NewReader(fixture.card.CanonicalJSON()), fail: "enroll", wantCalls: []string{"cwd", "resolve1", "resolve2", "parse", "enroll"}, wantExit: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls []string
			resolveCalls := 0
			fail := func(step string) error {
				calls = append(calls, step)
				if step == test.fail {
					return wantErr
				}
				return nil
			}
			deps := peerDependencies{
				workingDirectory: func() (string, error) {
					if err := fail("cwd"); err != nil {
						return "", err
					}
					return fixture.root, nil
				},
				resolveState: func(string) (string, string, error) {
					resolveCalls++
					if err := fail(fmt.Sprintf("resolve%d", resolveCalls)); err != nil {
						return "", "", err
					}
					return fixture.root, fixture.provision.StateDirectory(), nil
				},
				provision: func(context.Context, string) (daemon.ProvisionResult, error) {
					if err := fail("provision"); err != nil {
						return daemon.ProvisionResult{}, err
					}
					return fixture.provision, nil
				},
				configure: func(context.Context, string, string, string) (daemon.PeerCard, error) {
					if err := fail("configure"); err != nil {
						return daemon.PeerCard{}, err
					}
					return fixture.card, nil
				},
				parseCard: func(raw []byte) (daemon.PeerCard, error) {
					if err := fail("parse"); err != nil {
						return daemon.PeerCard{}, err
					}
					return daemon.ParsePeerCardCanonicalJSON(raw)
				},
				enroll: func(context.Context, string, string, daemon.PeerCard) (
					daemon.PeerEnrollment, error,
				) {
					if err := fail("enroll"); err != nil {
						return daemon.PeerEnrollment{}, err
					}
					return fixture.enrollment, nil
				},
			}
			stdin := test.stdin
			if stdin == nil {
				stdin = strings.NewReader("")
			}
			var stdout, stderr bytes.Buffer
			exit := runPeer(context.Background(), test.args, stdin, &stdout, &stderr, deps)
			if exit != test.wantExit || stdout.Len() != 0 ||
				!strings.Contains(stderr.String(), wantErr.Error()) ||
				!reflect.DeepEqual(calls, test.wantCalls) {
				t.Fatalf("failure = exit %d stdout %q stderr %q calls %#v",
					exit, stdout.String(), stderr.String(), calls)
			}
		})
	}
}

func TestReadPeerCardAcceptsOnlyCanonicalBodyWithOneOptionalLF(t *testing.T) {
	fixture := newPeerCommandFixture(t)
	canonical := fixture.card.CanonicalJSON()
	for _, input := range [][]byte{canonical, append(append([]byte(nil), canonical...), '\n')} {
		card, err := readPeerCard(bytes.NewReader(input), daemon.ParsePeerCardCanonicalJSON)
		if err != nil || card.PeerID() != fixture.card.PeerID() {
			t.Fatalf("readPeerCard(valid) = peer %s error %v", card.PeerID(), err)
		}
	}
	for _, suffix := range []string{"\r\n", "\n\n", " "} {
		input := append(append([]byte(nil), canonical...), suffix...)
		if _, err := readPeerCard(bytes.NewReader(input), daemon.ParsePeerCardCanonicalJSON); err == nil {
			t.Fatalf("readPeerCard accepted non-canonical suffix %q", suffix)
		}
	}
	parseCalls := 0
	parse := func([]byte) (daemon.PeerCard, error) {
		parseCalls++
		return fixture.card, nil
	}
	if _, err := readPeerCard(strings.NewReader(""), parse); err == nil || parseCalls != 0 {
		t.Fatalf("empty input = error %v parse calls %d", err, parseCalls)
	}
	maxCanonical := append(bytes.Repeat([]byte{'x'}, maxPeerCardInputBytes-1), '\n')
	if _, err := readPeerCard(bytes.NewReader(maxCanonical), parse); err != nil || parseCalls != 1 {
		t.Fatalf("maximum input = error %v parse calls %d", err, parseCalls)
	}
	oversized := append(bytes.Repeat([]byte{'x'}, maxPeerCardInputBytes), '\n')
	if _, err := readPeerCard(bytes.NewReader(oversized), parse); err == nil || parseCalls != 1 {
		t.Fatalf("oversized input = error %v parse calls %d", err, parseCalls)
	}
}
