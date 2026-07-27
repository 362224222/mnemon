package store

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestDeriveEffectiveAliasesHandlesDuplicatesAndTeamLabel(t *testing.T) {
	t.Parallel()
	channel := testkit.NewSignedChannel(t, "effective-aliases")
	roster := channel.Roster()
	ownerSigner := enrollmentTestSigner(t, channel.Owner())
	firstIdentity := testkit.NewIdentity(t, "duplicate-one")
	first, roster := appendRosterMemberWithLabel(t, channel.Descriptor(), ownerSigner, roster,
		firstIdentity, "duplicate")
	secondIdentity := testkit.NewIdentity(t, "duplicate-two")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), ownerSigner, roster,
		secondIdentity, "duplicate")
	teamIdentity := testkit.NewIdentity(t, "team-label")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), ownerSigner, roster,
		teamIdentity, "team")
	aliases, _, err := deriveEffectiveAliases(channel.Owner().PeerID(), roster)
	if err != nil {
		t.Fatal(err)
	}
	if aliases[firstIdentity.PeerID()] == first.DisplayLabel() ||
		aliases[secondIdentity.PeerID()] == first.DisplayLabel() ||
		aliases[firstIdentity.PeerID()] == aliases[secondIdentity.PeerID()] ||
		aliases[teamIdentity.PeerID()] != "team" {
		t.Fatalf("derived aliases = %#v", aliases)
	}
}

func TestDeriveEffectiveAliasesSanitizesDisplayWhitespaceBeforeCollisionRules(t *testing.T) {
	t.Parallel()
	channel := testkit.NewSignedChannel(t, "effective-alias-whitespace")
	roster := channel.Roster()
	signer := enrollmentTestSigner(t, channel.Owner())
	first := testkit.NewIdentity(t, "alias-space-one")
	second := testkit.NewIdentity(t, "alias-space-two")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), signer, roster, first, "Alice Smith")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), signer, roster, second, "Alice\u00a0Smith")
	aliases, _, err := deriveEffectiveAliases(channel.Owner().PeerID(), roster)
	if err != nil {
		t.Fatal(err)
	}
	if aliases[first.PeerID()] == "Alice-Smith" || aliases[second.PeerID()] == "Alice-Smith" ||
		aliases[first.PeerID()] == aliases[second.PeerID()] ||
		strings.ContainsAny(aliases[first.PeerID()]+aliases[second.PeerID()], " \u00a0") {
		t.Fatalf("whitespace aliases = %#v", aliases)
	}
}
