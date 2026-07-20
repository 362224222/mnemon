package peer

import (
	"context"
	"fmt"
	"sort"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

const (
	maxEnrollmentDNSDepth = 4
	maxEnrollmentDNSWork  = model.MaxMemberMultiaddrs * maxEnrollmentDNSDepth
)

type enrollmentMultiaddrResolver interface {
	Resolve(context.Context, ma.Multiaddr) ([]ma.Multiaddr, error)
}

func defaultEnrollmentTransportResolver() enrollmentMultiaddrResolver {
	return madns.DefaultResolver
}

type enrollmentTransportResolution struct {
	ctx       context.Context
	owner     libp2ppeer.ID
	resolver  enrollmentMultiaddrResolver
	remaining int
	frozen    map[string]ma.Multiaddr
}

// resolveEnrollmentTransportAddresses performs one bounded DNS snapshot for a
// permit. The signed names remain part of the permit key, while only the
// concrete, canonical result set reaches Peerstore and the gater. A permit
// never consults DNS again; DNS drift requires a new permit acquisition.
func resolveEnrollmentTransportAddresses(ctx context.Context, owner libp2ppeer.ID,
	signed []ma.Multiaddr, resolver enrollmentMultiaddrResolver,
) ([]ma.Multiaddr, error) {
	if ctx == nil || ctx.Err() != nil || owner == "" || resolver == nil || len(signed) == 0 {
		return nil, fmt.Errorf("%w: live bounded DNS resolution is required",
			ErrEnrollmentTransportPermit)
	}
	resolution := &enrollmentTransportResolution{ctx: ctx, owner: owner, resolver: resolver,
		remaining: maxEnrollmentDNSWork, frozen: make(map[string]ma.Multiaddr)}
	for _, address := range signed {
		if err := resolution.resolve(address, 0); err != nil {
			return nil, err
		}
	}
	if len(resolution.frozen) == 0 {
		return nil, fmt.Errorf("%w: DNS resolved no concrete owner transports",
			ErrEnrollmentTransportPermit)
	}
	addresses := make([]ma.Multiaddr, 0, len(resolution.frozen))
	for _, address := range resolution.frozen {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(left, right int) bool {
		return addresses[left].String() < addresses[right].String()
	})
	return addresses, nil
}

func (resolution *enrollmentTransportResolution) resolve(address ma.Multiaddr, depth int) error {
	if err := resolution.ctx.Err(); err != nil {
		return fmt.Errorf("%w: DNS context ended: %w", ErrEnrollmentTransportPermit, err)
	}
	if address == nil || address.String() == "" {
		return fmt.Errorf("%w: DNS returned an empty transport", ErrEnrollmentTransportPermit)
	}
	if !hasEnrollmentDNSComponent(address) {
		return resolution.freeze(address)
	}
	if depth >= maxEnrollmentDNSDepth || resolution.remaining <= 0 {
		return fmt.Errorf("%w: DNS recursion/work bound exceeded", ErrEnrollmentTransportPermit)
	}
	resolution.remaining--
	resolved, err := resolution.resolver.Resolve(resolution.ctx, address)
	if err != nil {
		return fmt.Errorf("%w: resolve signed owner transport: %w", ErrEnrollmentTransportPermit, err)
	}
	if err := resolution.ctx.Err(); err != nil {
		return fmt.Errorf("%w: DNS context ended: %w", ErrEnrollmentTransportPermit, err)
	}
	if len(resolved) == 0 || len(resolved) > model.MaxMemberMultiaddrs ||
		len(resolved) > resolution.remaining {
		return fmt.Errorf("%w: DNS answer is empty or exceeds the bounded result/work set",
			ErrEnrollmentTransportPermit)
	}
	resolution.remaining -= len(resolved)
	for _, candidate := range resolved {
		if err := resolution.resolve(candidate, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (resolution *enrollmentTransportResolution) freeze(address ma.Multiaddr) error {
	canonical, err := canonicalPeerAddresses(resolution.owner, address.String())
	if err != nil || len(canonical) != 1 || !concreteEnrollmentTransport(canonical[0]) {
		return fmt.Errorf("%w: DNS result is noncanonical, unresolved, or bound to another Peer",
			ErrEnrollmentTransportPermit)
	}
	value := canonical[0].String()
	if _, duplicate := resolution.frozen[value]; duplicate {
		return nil
	}
	if len(resolution.frozen) >= model.MaxMemberMultiaddrs {
		return fmt.Errorf("%w: DNS resolved more than %d distinct transports",
			ErrEnrollmentTransportPermit, model.MaxMemberMultiaddrs)
	}
	resolution.frozen[value] = canonical[0]
	return nil
}

func hasEnrollmentDNSComponent(address ma.Multiaddr) bool {
	matched := false
	ma.ForEach(address, func(component ma.Component) bool {
		switch component.Protocol().Code {
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6, ma.P_DNSADDR:
			matched = true
			return false
		default:
			return true
		}
	})
	return matched
}

func concreteEnrollmentTransport(address ma.Multiaddr) bool {
	if address == nil {
		return false
	}
	concrete := false
	unresolved := false
	ma.ForEach(address, func(component ma.Component) bool {
		switch component.Protocol().Code {
		case ma.P_IP4, ma.P_IP6:
			concrete = true
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6, ma.P_DNSADDR:
			unresolved = true
		}
		return true
	})
	return concrete && !unresolved
}
