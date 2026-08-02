//go:build darwin

package agent

import "errors"

// Darwin rejects getpgid(2) and getsid(2) for an unreaped zombie even though
// kern.proc.pid still exposes the exact retained child. An isolated managed
// Runtime was captured only after PID=PGID=SID was proven; two stable kernel
// snapshots preserve that identity until the parent consumes its Wait right.
func darwinRuntimeProcessStableZombie(pid int,
	first darwinRuntimeProcessSnapshot,
) (darwinRuntimeProcessSnapshot, error) {
	second, err := darwinRuntimeProcessKinfo(pid)
	if errors.Is(err, errDarwinRuntimeProcessNotExist) {
		return darwinRuntimeProcessSnapshot{}, errDarwinRuntimeProcessNotExist
	}
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	if first.pid != second.pid || first.startToken != second.startToken ||
		first.uid != second.uid || first.pgid != second.pgid ||
		second.state != darwinRuntimeProcessZombie || second.pgid != pid {
		return darwinRuntimeProcessSnapshot{},
			errors.New("Darwin zombie identity changed during observation")
	}
	second.sid = pid
	return second, nil
}
