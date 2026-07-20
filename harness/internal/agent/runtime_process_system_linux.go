//go:build linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const (
	linuxRuntimeProcessStatMax   = 4096
	linuxRuntimeProcessStatusMax = 64 << 10
	linuxRuntimeProcessDirBatch  = 256
	linuxRuntimeProcessEntryMax  = 131072
)

var (
	errLinuxRuntimeProcessNotExist = errors.New("Linux process does not exist")
	// errLinuxRuntimeProcessForeignIdentity marks stat lines whose identity can
	// never belong to a managed positive-PGID group, such as kernel threads
	// reporting pgid 0 and sid 0 on hosts with an unrestricted /proc view.
	errLinuxRuntimeProcessForeignIdentity = errors.New("Linux process identity is outside any managed group")
)

type systemLinuxRuntimeProcess struct{}

func (systemLinuxRuntimeProcess) BootID() (string, error) {
	raw, err := readBoundedRuntimeProcessFile("/proc/sys/kernel/random/boot_id", 128)
	if err != nil {
		return "", err
	}
	parsed, err := uuid.Parse(strings.TrimSpace(string(raw)))
	if err != nil {
		return "", errors.New("Linux boot ID is invalid")
	}
	return parsed.String(), nil
}

func (systemLinuxRuntimeProcess) EffectiveUID() uint32 {
	return uint32(unix.Geteuid())
}

func (system systemLinuxRuntimeProcess) Snapshot(pid int) (linuxRuntimeProcessSnapshot, error) {
	bootID, err := system.BootID()
	if err != nil {
		return linuxRuntimeProcessSnapshot{}, err
	}
	first, err := readLinuxRuntimeProcessStat(pid, bootID)
	if err != nil {
		return linuxRuntimeProcessSnapshot{}, err
	}
	uid, err := readLinuxRuntimeProcessEffectiveUID(pid)
	if err != nil {
		return linuxRuntimeProcessSnapshot{}, err
	}
	second, err := readLinuxRuntimeProcessStat(pid, bootID)
	if err != nil {
		return linuxRuntimeProcessSnapshot{}, err
	}
	if first.pid != second.pid || first.pgid != second.pgid || first.sid != second.sid ||
		first.startToken != second.startToken {
		return linuxRuntimeProcessSnapshot{}, errors.New("Linux process identity changed during observation")
	}
	second.uid = uid
	return second, nil
}

func readLinuxRuntimeProcessStat(pid int, bootID string) (linuxRuntimeProcessSnapshot, error) {
	raw, err := readBoundedRuntimeProcessFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"),
		linuxRuntimeProcessStatMax)
	if errors.Is(err, os.ErrNotExist) {
		return linuxRuntimeProcessSnapshot{}, errLinuxRuntimeProcessNotExist
	}
	if err != nil {
		return linuxRuntimeProcessSnapshot{}, err
	}
	return parseLinuxRuntimeProcessStat(pid, bootID, raw)
}

func parseLinuxRuntimeProcessStat(pid int, bootID string, raw []byte) (linuxRuntimeProcessSnapshot, error) {
	line := strings.TrimSpace(string(raw))
	open := strings.IndexByte(line, '(')
	close := strings.LastIndex(line, ") ")
	if open <= 0 || close <= open || close+2 >= len(line) {
		return linuxRuntimeProcessSnapshot{}, errors.New("Linux process stat is malformed")
	}
	parsedPID, err := strconv.Atoi(strings.TrimSpace(line[:open]))
	if err != nil || parsedPID != pid {
		return linuxRuntimeProcessSnapshot{}, errors.New("Linux process stat PID differs")
	}
	fields := strings.Fields(line[close+2:])
	if len(fields) <= 19 || len(fields[0]) != 1 {
		return linuxRuntimeProcessSnapshot{}, errors.New("Linux process stat lacks identity fields")
	}
	pgid, pgidErr := strconv.Atoi(fields[2])
	sid, sidErr := strconv.Atoi(fields[3])
	start, startErr := strconv.ParseUint(fields[19], 10, 64)
	if pgidErr != nil || sidErr != nil || startErr != nil || start == 0 {
		return linuxRuntimeProcessSnapshot{}, errors.New("Linux process stat identity is invalid")
	}
	if pgid <= 0 || sid <= 0 {
		return linuxRuntimeProcessSnapshot{}, errLinuxRuntimeProcessForeignIdentity
	}
	return linuxRuntimeProcessSnapshot{pid: pid, pgid: pgid, sid: sid,
		startToken: fmt.Sprintf("linux:%s:%d", bootID, start), state: fields[0][0]}, nil
}

func readLinuxRuntimeProcessEffectiveUID(pid int) (uint32, error) {
	raw, err := readBoundedRuntimeProcessFile(filepath.Join("/proc", strconv.Itoa(pid), "status"),
		linuxRuntimeProcessStatusMax)
	if errors.Is(err, os.ErrNotExist) {
		return 0, errLinuxRuntimeProcessNotExist
	}
	if err != nil {
		return 0, err
	}
	seen := false
	var uid uint64
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "Uid:" {
			continue
		}
		if seen || len(fields) != 5 {
			return 0, errors.New("Linux process status has an invalid UID row")
		}
		value, parseErr := strconv.ParseUint(fields[2], 10, 32)
		if parseErr != nil {
			return 0, errors.New("Linux process effective UID is invalid")
		}
		uid, seen = value, true
	}
	if !seen {
		return 0, errors.New("Linux process status lacks UID")
	}
	return uint32(uid), nil
}

func readBoundedRuntimeProcessFile(path string, maximum int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, errors.New("process identity file exceeds its bound")
	}
	return raw, nil
}

func (systemLinuxRuntimeProcess) GroupExists(pgid int) (bool, error) {
	if pgid <= 0 {
		return false, errors.New("Linux process group is invalid")
	}
	if pgid == 1 {
		bootID, err := (systemLinuxRuntimeProcess{}).BootID()
		if err != nil {
			return false, err
		}
		snapshot, err := readLinuxRuntimeProcessStat(1, bootID)
		switch {
		case errors.Is(err, errLinuxRuntimeProcessNotExist):
			return false, nil
		case err != nil:
			return false, err
		default:
			return snapshot.pgid == 1 && !linuxRuntimeProcessExited(snapshot.state), nil
		}
	}
	err := unix.Kill(-pgid, 0)
	switch {
	case err == nil, errors.Is(err, unix.EPERM):
		return true, nil
	case errors.Is(err, unix.ESRCH):
		return false, nil
	default:
		return false, err
	}
}

func (system systemLinuxRuntimeProcess) GroupHasLiveMembers(ctx context.Context, pgid int) (bool, error) {
	return system.groupHasMatchingMembers(ctx, pgid, func(state byte) bool {
		return !linuxRuntimeProcessExited(state)
	})
}

func (system systemLinuxRuntimeProcess) GroupHasUnstoppedMembers(ctx context.Context,
	pgid int,
) (bool, error) {
	return system.groupHasMatchingMembers(ctx, pgid, func(state byte) bool {
		return !linuxRuntimeProcessExited(state) && state != 'T' && state != 't'
	})
}

func (system systemLinuxRuntimeProcess) groupHasMatchingMembers(ctx context.Context, pgid int,
	matches func(byte) bool,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("Linux group scan context is unavailable")
	}
	bootID, err := system.BootID()
	if err != nil {
		return false, err
	}
	directory, err := os.Open("/proc")
	if err != nil {
		return false, err
	}
	defer directory.Close()
	entries := 0
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		names, err := directory.Readdirnames(linuxRuntimeProcessDirBatch)
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		entries += len(names)
		if entries > linuxRuntimeProcessEntryMax {
			return false, errors.New("Linux process directory exceeds its scan bound")
		}
		for _, name := range names {
			member, memberErr := linuxGroupEntryMatches(name, pgid, bootID, matches)
			if memberErr != nil {
				return false, memberErr
			}
			if member {
				return true, nil
			}
		}
		if errors.Is(err, io.EOF) {
			return false, nil
		}
	}
}

// linuxGroupEntryMatches reports whether one /proc entry is a live member of
// the scanned group. Vanished processes and foreign identities such as kernel
// threads can never belong to a managed positive-PGID group and never fail
// the sweep.
func linuxGroupEntryMatches(name string, pgid int, bootID string,
	matches func(byte) bool,
) (bool, error) {
	pid, parseErr := strconv.Atoi(name)
	if parseErr != nil || pid <= 0 {
		return false, nil
	}
	snapshot, err := readLinuxRuntimeProcessStat(pid, bootID)
	if errors.Is(err, errLinuxRuntimeProcessNotExist) ||
		errors.Is(err, errLinuxRuntimeProcessForeignIdentity) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return snapshot.pgid == pgid && matches(snapshot.state), nil
}

func (systemLinuxRuntimeProcess) SignalGroup(pgid int, signal syscall.Signal) error {
	// kill(-1, signal) broadcasts to nearly every permitted process. Keep this
	// guard at the syscall boundary as defense in depth even though every
	// managed Runtime entry point also requires PID=PGID=SID>1.
	if pgid <= 1 {
		return errors.New("Linux process-group signal target must be greater than one")
	}
	return unix.Kill(-pgid, signal)
}
