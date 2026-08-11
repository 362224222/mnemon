package agencyclient

import "sync"

type processJournalLock struct {
	mu   sync.Mutex
	refs int
}

var processJournalLocks = struct {
	sync.Mutex
	byPath map[string]*processJournalLock
}{byPath: make(map[string]*processJournalLock)}

// acquireProcessJournalLock complements the cross-process flock. BSD flock
// semantics do not uniformly serialize separate descriptors in one process,
// so goroutines need this short, reference-counted owner as well. Entries are
// removed at the last release; the registry cannot grow with inactive paths.
func acquireProcessJournalLock(path string) func() {
	processJournalLocks.Lock()
	entry := processJournalLocks.byPath[path]
	if entry == nil {
		entry = &processJournalLock{}
		processJournalLocks.byPath[path] = entry
	}
	entry.refs++
	processJournalLocks.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		processJournalLocks.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(processJournalLocks.byPath, path)
		}
		processJournalLocks.Unlock()
	}
}
