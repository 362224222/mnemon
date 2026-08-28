//go:build windows

package attach

import "os"

// fsStatOf always reports "not a POSIX stat" on Windows: there is no owner uid
// or hard-link count. The caller's !ok branch keeps the owner checks failing
// closed.
func fsStatOf(_ os.FileInfo) (*fsStat, bool) { return nil, false }
