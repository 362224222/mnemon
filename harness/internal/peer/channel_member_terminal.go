package peer

import (
	"bytes"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// terminalMemberHelloAcknowledgement is the one narrow exception to current
// membership authorization. A securely authenticated former member may learn
// only the continuous owner-signed suffix through its own terminal record.
// Records after that terminal are deliberately undisclosed.
func terminalMemberHelloAcknowledgement(roster model.VerifiedRoster, hello MemberHello,
	remotePeerID model.PeerID, remotePublicKey []byte,
) (MemberHelloAck, bool, error) {
	current, ok := roster.CurrentMember(remotePeerID)
	if !ok || !current.Status().Terminal() {
		return MemberHelloAck{}, false, nil
	}
	active := hello.ActiveMemberRecord()
	if !validHistoricalTerminalHello(roster, hello, active, current,
		remotePeerID, remotePublicKey) {
		return MemberHelloAck{}, false, fmt.Errorf(
			"%w: terminal hello lacks historical active authority", ErrChannelMemberProtocol)
	}
	members := roster.Members()
	knownRevision := hello.KnownRosterHead().Revision()
	terminalRevision := current.Head().Revision()
	suffix := append([]model.Member(nil), members[knownRevision:terminalRevision]...)
	ack, err := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: hello.ChannelID(),
		MissingRecords: suffix, RosterHead: current.Head()})
	if err != nil {
		return MemberHelloAck{}, false, fmt.Errorf(
			"%w: construct terminal hello acknowledgement: %v", ErrChannelMemberProtocol, err)
	}
	return ack, true, nil
}

func validHistoricalTerminalHello(roster model.VerifiedRoster, hello MemberHello,
	active, terminal model.Member, remotePeerID model.PeerID, remotePublicKey []byte,
) bool {
	members := roster.Members()
	activeRevision := active.Head().Revision()
	knownRevision := hello.KnownRosterHead().Revision()
	terminalRevision := terminal.Head().Revision()
	if roster.IsZero() || hello.ChannelID() != roster.Descriptor().Descriptor().ID() ||
		active.PeerID() != remotePeerID || active.Status() != model.MemberActive ||
		!bytes.Equal(active.PublicKey(), remotePublicKey) || terminal.PeerID() != remotePeerID ||
		terminal.OriginEpoch() != active.OriginEpoch() ||
		!bytes.Equal(terminal.PublicKey(), active.PublicKey()) || activeRevision == 0 ||
		knownRevision == 0 || knownRevision < activeRevision || terminalRevision <= knownRevision ||
		terminalRevision > uint64(len(members)) {
		return false
	}
	return sameMember(members[activeRevision-1], active) &&
		members[knownRevision-1].Head() == hello.KnownRosterHead() &&
		sameMember(members[terminalRevision-1], terminal)
}
