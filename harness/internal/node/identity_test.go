package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestIdentityCreateRestartAndSharedDerivation(t *testing.T) {
	nodeState := newIdentityNodeState(t)
	created, err := EnsureIdentity(nodeState)
	if err != nil {
		t.Fatalf("EnsureIdentity() error = %v", err)
	}
	if created.PeerID().IsZero() || created.PrivateKey() == nil || created.PublicationSigner() == nil {
		t.Fatalf("created identity is incomplete: %#v", created)
	}
	keyPath := filepath.Join(nodeState, identityKeyName)
	keyInfo, err := os.Lstat(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	assertIdentityPath(t, keyInfo, identityKeyMode, false)
	encoded, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	storedKey, err := libp2pcrypto.UnmarshalPrivateKey(encoded)
	if err != nil || storedKey.Type() != libp2pcrypto.Ed25519 {
		t.Fatalf("stored key = (%T, %v)", storedKey, err)
	}
	canonical, err := libp2pcrypto.MarshalPrivateKey(storedKey)
	if err != nil || !bytes.Equal(encoded, canonical) {
		t.Fatalf("stored key is not canonical: %v", err)
	}

	derivedPeer, err := libp2ppeer.IDFromPrivateKey(created.PrivateKey())
	if err != nil || created.PeerID().String() != derivedPeer.String() {
		t.Fatalf("PeerID = %q, derived %q, %v", created.PeerID().String(), derivedPeer, err)
	}
	rawPublic, err := created.PrivateKey().GetPublic().Raw()
	if err != nil || !bytes.Equal(created.PublicKey(), rawPublic) {
		t.Fatalf("public key differs from libp2p identity: %x/%x, %v", created.PublicKey(), rawPublic, err)
	}
	message := []byte("one Node identity signs Event publications")
	signature, err := created.PublicationSigner().Sign(context.Background(), message)
	if err != nil || !ed25519.Verify(created.PublicKey(), message, signature) {
		t.Fatalf("publication signature did not verify: %v", err)
	}
	returnedPublic := created.PublicKey()
	returnedPublic[0] ^= 0xff
	if bytes.Equal(returnedPublic, created.PublicKey()) {
		t.Fatal("PublicKey returned mutable identity storage")
	}

	restarted, err := LoadIdentity(nodeState)
	if err != nil {
		t.Fatalf("LoadIdentity() error = %v", err)
	}
	createdRaw, _ := created.PrivateKey().Raw()
	restartedRaw, _ := restarted.PrivateKey().Raw()
	if restarted.PeerID() != created.PeerID() || !bytes.Equal(restartedRaw, createdRaw) {
		t.Fatalf("restart identity changed: %s/%s", created.PeerID().String(), restarted.PeerID().String())
	}
	beforeSecondEnsure, err := os.Lstat(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	ensured, err := EnsureIdentity(nodeState)
	if err != nil {
		t.Fatalf("second EnsureIdentity() error = %v", err)
	}
	afterSecondEnsure, err := os.Lstat(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if ensured.PeerID() != created.PeerID() || !os.SameFile(beforeSecondEnsure, afterSecondEnsure) {
		t.Fatal("second EnsureIdentity replaced the persistent key")
	}
	assertOnlyIdentityKey(t, nodeState)
}

func TestIdentityConcurrentCreateConvergesWithoutClobber(t *testing.T) {
	nodeState := newIdentityNodeState(t)
	const callers = 32
	start := make(chan struct{})
	results := make(chan struct {
		identity *Identity
		err      error
	}, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for index := 0; index < callers; index++ {
		go func() {
			ready.Done()
			<-start
			identity, err := EnsureIdentity(nodeState)
			results <- struct {
				identity *Identity
				err      error
			}{identity, err}
		}()
	}
	ready.Wait()
	close(start)
	var wantPeer string
	for index := 0; index < callers; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent EnsureIdentity() error = %v", result.err)
		}
		if wantPeer == "" {
			wantPeer = result.identity.PeerID().String()
		}
		if result.identity.PeerID().String() != wantPeer {
			t.Fatalf("concurrent identity = %s, want %s", result.identity.PeerID().String(), wantPeer)
		}
	}
	restarted, err := LoadIdentity(nodeState)
	if err != nil || restarted.PeerID().String() != wantPeer {
		t.Fatalf("LoadIdentity() = (%v, %v), want %s", restarted, err, wantPeer)
	}
	assertOnlyIdentityKey(t, nodeState)
}

func TestIdentityRejectsUnsafeStateAndKeyPaths(t *testing.T) {
	t.Run("relative Node state", func(t *testing.T) {
		if _, err := EnsureIdentity("relative/node"); !errors.Is(err, ErrIdentity) {
			t.Fatalf("EnsureIdentity() error = %v", err)
		}
	})
	t.Run("unclean Node state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing", "..", "node")
		if _, err := EnsureIdentity(path); !errors.Is(err, ErrIdentity) {
			t.Fatalf("EnsureIdentity() error = %v", err)
		}
	})
	t.Run("missing Node state", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing")
		if _, err := EnsureIdentity(path); !errors.Is(err, ErrIdentity) || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("EnsureIdentity() error = %v", err)
		}
	})
	t.Run("strict load does not create a missing key", func(t *testing.T) {
		path := newIdentityNodeState(t)
		if _, err := LoadIdentity(path); !errors.Is(err, ErrIdentity) || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("LoadIdentity() error = %v", err)
		}
		entries, err := os.ReadDir(path)
		if err != nil || len(entries) != 0 {
			t.Fatalf("strict load created Node state entries: %v, %v", entries, err)
		}
	})
	t.Run("Node state mode", func(t *testing.T) {
		path := newIdentityNodeState(t)
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(path); !errors.Is(err, ErrIdentity) {
			t.Fatalf("EnsureIdentity() error = %v", err)
		}
	})
	t.Run("Node state symlink", func(t *testing.T) {
		realState := newIdentityNodeState(t)
		link := filepath.Join(t.TempDir(), "node-link")
		if err := os.Symlink(realState, link); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(link); !errors.Is(err, ErrIdentity) {
			t.Fatalf("EnsureIdentity() error = %v", err)
		}
	})
	t.Run("key mode", func(t *testing.T) {
		path := newIdentityNodeState(t)
		writeIdentityTestKey(t, path, validIdentityEncoding(t), 0o644)
		if _, err := LoadIdentity(path); !errors.Is(err, ErrIdentity) {
			t.Fatalf("LoadIdentity() error = %v", err)
		}
	})
	t.Run("key symlink", func(t *testing.T) {
		path := newIdentityNodeState(t)
		target := filepath.Join(t.TempDir(), "outside.key")
		if err := os.WriteFile(target, validIdentityEncoding(t), identityKeyMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(path, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadIdentity(path); !errors.Is(err, ErrIdentity) {
			t.Fatalf("LoadIdentity() error = %v", err)
		}
	})
	t.Run("key is not regular", func(t *testing.T) {
		path := newIdentityNodeState(t)
		if err := os.Mkdir(filepath.Join(path, identityKeyName), identityKeyMode); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadIdentity(path); !errors.Is(err, ErrIdentity) {
			t.Fatalf("LoadIdentity() error = %v", err)
		}
	})
	t.Run("wrong owner metadata", func(t *testing.T) {
		path := newIdentityNodeState(t)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		baseStat := info.Sys().(*syscall.Stat_t)
		wrongUID := uint32(os.Geteuid()) + 1
		fake := identityOwnerTestInfo{FileInfo: info, stat: *baseStat}
		fake.stat.Uid = wrongUID
		if _, err := validateIdentityOwnerPath(fake, identityDirectoryMode, true); err == nil {
			t.Fatal("foreign owner metadata was accepted")
		}
	})
}

func TestIdentityRejectsInvalidOrReplacedKeyMaterialWithoutOverwrite(t *testing.T) {
	nonEdKey, _, err := libp2pcrypto.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nonEdEncoding, err := libp2pcrypto.MarshalPrivateKey(nonEdKey)
	if err != nil {
		t.Fatal(err)
	}
	inconsistentEncoding := inconsistentIdentityEncoding(t)
	validWithTrailingUnknown := append(validIdentityEncoding(t), 0x18, 0x01)
	cases := []struct {
		name    string
		encoded []byte
	}{
		{name: "empty", encoded: nil},
		{name: "corrupt", encoded: []byte("not a private key")},
		{name: "non Ed25519", encoded: nonEdEncoding},
		{name: "inconsistent Ed25519", encoded: inconsistentEncoding},
		{name: "noncanonical encoding", encoded: validWithTrailingUnknown},
		{name: "oversized", encoded: bytes.Repeat([]byte{0x42}, int(maxIdentityKeyBytes+1))},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			nodeState := newIdentityNodeState(t)
			writeIdentityTestKey(t, nodeState, test.encoded, identityKeyMode)
			before, err := os.ReadFile(filepath.Join(nodeState, identityKeyName))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := EnsureIdentity(nodeState); !errors.Is(err, ErrIdentity) {
				t.Fatalf("EnsureIdentity() error = %v", err)
			}
			after, err := os.ReadFile(filepath.Join(nodeState, identityKeyName))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, before) {
				t.Fatal("EnsureIdentity overwrote an existing invalid key")
			}
			assertOnlyIdentityKey(t, nodeState)
		})
	}
}

type identityOwnerTestInfo struct {
	os.FileInfo
	stat syscall.Stat_t
}

func (info identityOwnerTestInfo) Sys() any { return &info.stat }

func newIdentityNodeState(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(path, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	assertIdentityPath(t, info, identityDirectoryMode, true)
	return path
}

func validIdentityEncoding(t *testing.T) []byte {
	t.Helper()
	privateKey, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := libp2pcrypto.MarshalPrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func inconsistentIdentityEncoding(t *testing.T) []byte {
	t.Helper()
	privateKey, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	inconsistent, err := libp2pcrypto.UnmarshalEd25519PrivateKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := libp2pcrypto.MarshalPrivateKey(inconsistent)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func writeIdentityTestKey(t *testing.T, nodeState string, encoded []byte, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(nodeState, identityKeyName)
	if err := os.WriteFile(path, encoded, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func assertOnlyIdentityKey(t *testing.T, nodeState string) {
	t.Helper()
	entries, err := os.ReadDir(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != identityKeyName {
		t.Fatalf("Node state entries = %v, want only %q", entries, identityKeyName)
	}
}

func assertIdentityPath(t *testing.T, info os.FileInfo, mode os.FileMode, directory bool) {
	t.Helper()
	uid, err := validateIdentityOwnerPath(info, mode, directory)
	if err != nil || uid != uint32(os.Geteuid()) {
		t.Fatalf("identity path = (%v, uid %d), %v", info.Mode(), uid, err)
	}
}
