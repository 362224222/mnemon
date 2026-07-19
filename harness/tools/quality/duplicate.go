package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

const (
	maximumSeedWindows     = 2_000_000
	maximumSeedOccurrences = 256
)

type duplicateMeasurement struct {
	DebtID      string
	Owners      []string
	Fingerprint string
	Tokens      int
}

type tokenOccurrence struct {
	function int
	start    int
}

type duplicateCandidate struct {
	fingerprint string
	tokens      int
	owners      map[string]struct{}
}

func measureDuplicates(functions []functionMeasurement) ([]duplicateMeasurement, error) {
	seeds, err := buildDuplicateSeeds(functions)
	if err != nil {
		return nil, err
	}
	candidates := collectDuplicateCandidates(functions, seeds)
	return finalizeDuplicates(candidates), nil
}

func buildDuplicateSeeds(functions []functionMeasurement) (map[[sha256.Size]byte][]tokenOccurrence, error) {
	seeds := make(map[[sha256.Size]byte][]tokenOccurrence)
	windows := 0
	for functionIndex, function := range functions {
		for start := 0; start+duplicateTokenMinimum <= len(function.Tokens); start++ {
			windows++
			if windows > maximumSeedWindows {
				return nil, fmt.Errorf("duplicate analysis exceeds %d seed windows", maximumSeedWindows)
			}
			fingerprint := tokenFingerprint(function.Tokens[start : start+duplicateTokenMinimum])
			occurrences := seeds[fingerprint]
			if len(occurrences) >= maximumSeedOccurrences {
				return nil, fmt.Errorf("duplicate seed %x exceeds %d occurrences", fingerprint[:8], maximumSeedOccurrences)
			}
			seeds[fingerprint] = append(occurrences, tokenOccurrence{function: functionIndex, start: start})
		}
	}
	return seeds, nil
}

func collectDuplicateCandidates(functions []functionMeasurement, seeds map[[sha256.Size]byte][]tokenOccurrence) map[string]*duplicateCandidate {
	candidates := make(map[string]*duplicateCandidate)
	seenMatches := make(map[string]struct{})
	for _, occurrences := range seeds {
		if len(occurrences) < 2 {
			continue
		}
		collectOccurrencePairs(functions, occurrences, candidates, seenMatches)
	}
	return candidates
}

func collectOccurrencePairs(functions []functionMeasurement, occurrences []tokenOccurrence, candidates map[string]*duplicateCandidate, seen map[string]struct{}) {
	for leftIndex := 0; leftIndex < len(occurrences); leftIndex++ {
		for rightIndex := leftIndex + 1; rightIndex < len(occurrences); rightIndex++ {
			collectOccurrencePair(functions, occurrences[leftIndex], occurrences[rightIndex], candidates, seen)
		}
	}
}

func collectOccurrencePair(functions []functionMeasurement, left, right tokenOccurrence, candidates map[string]*duplicateCandidate, seen map[string]struct{}) {
	leftOwner := functionOwner(functions[left.function])
	rightOwner := functionOwner(functions[right.function])
	if leftOwner == rightOwner {
		return
	}
	leftStart, rightStart, length := extendDuplicate(functions[left.function].Tokens, left.start, functions[right.function].Tokens, right.start)
	matchKey := duplicateMatchKey(leftOwner, leftStart, rightOwner, rightStart, length)
	if _, exists := seen[matchKey]; exists {
		return
	}
	seen[matchKey] = struct{}{}
	fingerprintBytes := tokenFingerprint(functions[left.function].Tokens[leftStart : leftStart+length])
	fingerprint := hex.EncodeToString(fingerprintBytes[:])
	candidate := candidates[fingerprint]
	if candidate == nil {
		candidate = &duplicateCandidate{fingerprint: fingerprint, tokens: length, owners: make(map[string]struct{})}
		candidates[fingerprint] = candidate
	}
	candidate.owners[leftOwner] = struct{}{}
	candidate.owners[rightOwner] = struct{}{}
}

func finalizeDuplicates(candidates map[string]*duplicateCandidate) []duplicateMeasurement {
	measured := make([]duplicateMeasurement, 0, len(candidates))
	for _, candidate := range candidates {
		owners := sortedSet(candidate.owners)
		measured = append(measured, duplicateMeasurement{Owners: owners, Fingerprint: candidate.fingerprint, Tokens: candidate.tokens})
	}
	sort.Slice(measured, func(i, j int) bool {
		leftOwners := strings.Join(measured[i].Owners, "\x00")
		rightOwners := strings.Join(measured[j].Owners, "\x00")
		if leftOwners != rightOwners {
			return leftOwners < rightOwners
		}
		return measured[i].Fingerprint < measured[j].Fingerprint
	})
	for index := range measured {
		measured[index].DebtID = fmt.Sprintf("dup-%04d", index+1)
	}
	return measured
}

func tokenFingerprint(tokens []string) [sha256.Size]byte {
	digest := sha256.New()
	var length [4]byte
	for _, item := range tokens {
		binary.BigEndian.PutUint32(length[:], uint32(len(item)))
		digest.Write(length[:])
		digest.Write([]byte(item))
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func extendDuplicate(left []string, leftStart int, right []string, rightStart int) (int, int, int) {
	for leftStart > 0 && rightStart > 0 && left[leftStart-1] == right[rightStart-1] {
		leftStart--
		rightStart--
	}
	length := duplicateTokenMinimum
	for leftStart+length < len(left) && rightStart+length < len(right) && left[leftStart+length] == right[rightStart+length] {
		length++
	}
	return leftStart, rightStart, length
}

func duplicateMatchKey(leftOwner string, leftStart int, rightOwner string, rightStart int, length int) string {
	if rightOwner < leftOwner {
		leftOwner, rightOwner = rightOwner, leftOwner
		leftStart, rightStart = rightStart, leftStart
	}
	return fmt.Sprintf("%s\x00%d\x00%s\x00%d\x00%d", leftOwner, leftStart, rightOwner, rightStart, length)
}

func functionOwner(function functionMeasurement) string {
	return function.Path + "::" + function.Symbol
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
