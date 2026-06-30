package participant

import (
	"fmt"
	"strings"
)

// NormalizePrincipal returns the stable product-config key for a participant.
func NormalizePrincipal(value string) string {
	return strings.TrimSpace(value)
}

func RequirePrincipal(label, value string) (string, error) {
	principal := NormalizePrincipal(value)
	if principal == "" {
		if strings.TrimSpace(label) == "" {
			label = "participant"
		}
		return "", fmt.Errorf("%s principal is required", strings.TrimSpace(label))
	}
	return principal, nil
}

func ValidateUniquePrincipals[T any](items []T, label string, principalOf func(T) string) error {
	label = strings.TrimSpace(label)
	if label == "" {
		label = "participant"
	}
	seen := map[string]bool{}
	for _, item := range items {
		principal := NormalizePrincipal(principalOf(item))
		if principal == "" {
			return fmt.Errorf("%s principal is required", label)
		}
		if seen[principal] {
			return fmt.Errorf("duplicate %s principal %q", label, principal)
		}
		seen[principal] = true
	}
	return nil
}

func UpsertByPrincipal[T any](items []T, next T, principalOf func(T) string, merge func(existing, next T) T) []T {
	nextPrincipal := NormalizePrincipal(principalOf(next))
	for i := range items {
		if NormalizePrincipal(principalOf(items[i])) != nextPrincipal {
			continue
		}
		if merge == nil {
			items[i] = next
		} else {
			items[i] = merge(items[i], next)
		}
		return items
	}
	return append(items, next)
}
