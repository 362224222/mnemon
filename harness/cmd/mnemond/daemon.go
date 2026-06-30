package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// daemonPaths resolves the pidfile + logfile under <root>/.mnemon/harness/local (decision E): the
// same local state dir setup writes config.json into, so the daemon's runtime files sit beside its
// configuration. One daemon per project store (the store flock is the real mutex; the pidfile is the
// operator-facing handle).
func daemonPaths(root string) (dir, pidPath, logPath string) {
	dir = filepath.Join(root, ".mnemon", "harness", "local")
	return dir, filepath.Join(dir, "mnemond.pid"), filepath.Join(dir, "mnemond.log")
}

// rootFlag parses --root for the lifecycle verbs that take no serve flags (down/status/logs).
func rootFlag(args []string, errw io.Writer, verb ...string) (string, error) {
	name := "mnemond"
	if len(verb) > 0 && strings.TrimSpace(verb[0]) != "" {
		name = "mnemond " + strings.TrimSpace(verb[0])
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(errw)
	root := fs.String("root", ".", "project root")
	fs.Usage = func() {
		fmt.Fprintf(errw, "Usage of %s:\n", name)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	if *root == "" {
		return ".", nil
	}
	return filepath.Clean(*root), nil
}

// readLivePid reads the pidfile and reports the recorded pid plus whether that process is alive. A
// missing/garbage pidfile returns (0, false); a pidfile naming a dead process returns (pid, false)
// so the caller can clean the stale file and report what it found.
func readLivePid(pidPath string) (int, bool) {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, processAlive(pid)
}

// processAlive probes a pid with signal 0 (no signal delivered, just an existence/permission check).
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// daemonUp starts the foreground serve as a DETACHED background child (its own session via Setsid,
// stdout/stderr to the logfile), records its pid, and confirms it began listening. It PRE-FLIGHTS the
// boot in the foreground (parseServe resolves setup + T1), so a misconfigured project reports the
// error directly here instead of silently in the log. Refuses to start a second daemon over a live one.
func daemonUp(args []string, out, errw io.Writer) error {
	cfg, err := parseServe(args, errw)
	if err != nil {
		return err
	}
	dir, pidPath, logPath := daemonPaths(cfg.projectRoot)
	if pid, alive := readLivePid(pidPath); alive {
		return fmt.Errorf("already running (pid %d); run `mnemond down` first", pid)
	}
	if err := ensureListenAddrAvailable(cfg.listenAddr); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(exe, append([]string{"serve"}, args...)...)
	child.Stdout = logf
	child.Stderr = logf
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	pid := child.Process.Pid
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		_ = child.Process.Kill()
		return err
	}
	if err := waitListening(pid, cfg.listenAddr); err != nil {
		_ = os.Remove(pidPath)
		if tail := tailFile(logPath, 10); tail != "" {
			return fmt.Errorf("%w; recent log:\n%s", err, tail)
		}
		return err
	}
	_ = child.Process.Release()
	fmt.Fprintf(out, "mnemond: started (pid %d) on %s\nlogs: %s\n", pid, cfg.listenAddr, logPath)
	return nil
}

func ensureListenAddrAvailable(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen address %s is unavailable: %w", addr, err)
	}
	return ln.Close()
}

// waitListening confirms the detached child came up: it polls for the child to accept a TCP
// connection on its listen address (a strong readiness signal that also catches a bind failure),
// failing fast if the child exits during startup.
func waitListening(pid int, addr string) error {
	for i := 0; i < 30; i++ {
		if !processAlive(pid) {
			return fmt.Errorf("daemon exited during startup")
		}
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("daemon did not start listening on %s within 3s", addr)
}

// daemonDown signals the recorded daemon to stop (SIGTERM, the same signal the foreground serve
// traps for graceful shutdown), waits for it to exit, and removes the pidfile. A stale or absent
// pidfile is reported, not an error — `down` is idempotent.
func daemonDown(args []string, out, errw io.Writer) error {
	root, err := rootFlag(args, errw, "down")
	if err != nil {
		return err
	}
	_, pidPath, _ := daemonPaths(root)
	pid, alive := readLivePid(pidPath)
	if pid == 0 {
		fmt.Fprintln(out, "mnemond: not running")
		return nil
	}
	if !alive {
		_ = os.Remove(pidPath)
		fmt.Fprintf(out, "mnemond: not running (removed stale pidfile for pid %d)\n", pid)
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	for i := 0; i < 50; i++ {
		if !processAlive(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if processAlive(pid) {
		return fmt.Errorf("daemon (pid %d) did not stop within 5s", pid)
	}
	_ = os.Remove(pidPath)
	fmt.Fprintf(out, "mnemond: stopped (pid %d)\n", pid)
	return nil
}

// daemonReload restarts the daemon so it RE-ASSEMBLES the catalog, including any external
// capability packages under .mnemon/loops. It is a single verb (stop the recorded pid, wait, then
// `up` with the same flags), NOT a watch and NOT two shelled commands. Pre-flighting the boot (via
// daemonUp) keeps a misconfigured project from leaving the daemon down.
func daemonReload(args []string, out, errw io.Writer) error {
	cfg, err := parseServe(args, errw)
	if err != nil {
		return err
	}
	_, pidPath, _ := daemonPaths(cfg.projectRoot)
	if pid, alive := readLivePid(pidPath); alive {
		if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("signal pid %d: %w", pid, err)
		}
		for i := 0; i < 50; i++ {
			if !processAlive(pid) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if processAlive(pid) {
			return fmt.Errorf("daemon (pid %d) did not stop for reload within 5s", pid)
		}
		_ = os.Remove(pidPath)
		fmt.Fprintf(out, "mnemond: stopped (pid %d) for reload\n", pid)
	}
	// up re-reads the catalog before serving again.
	return daemonUp(args, out, errw)
}

// daemonStatus reports whether the recorded daemon is alive.
func daemonStatus(args []string, out, errw io.Writer) error {
	root, err := rootFlag(args, errw, "status")
	if err != nil {
		return err
	}
	_, pidPath, _ := daemonPaths(root)
	if pid, alive := readLivePid(pidPath); alive {
		fmt.Fprintf(out, "mnemond: running (pid %d)\n", pid)
	} else {
		fmt.Fprintln(out, "mnemond: stopped")
	}
	return nil
}

// daemonLogs prints the daemon's captured stdout/stderr.
func daemonLogs(args []string, out, errw io.Writer) error {
	root, err := rootFlag(args, errw, "logs")
	if err != nil {
		return err
	}
	_, _, logPath := daemonPaths(root)
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(out, "mnemond: no log yet")
			return nil
		}
		return err
	}
	_, err = out.Write(data)
	return err
}

// tailFile returns the last n lines of a file (best-effort; "" on any read error).
func tailFile(path string, n int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
