package setup

import "os"

func writeExecutableFile(path string, content []byte) error {
	if err := os.WriteFile(path, content, 0o755); err != nil {
		return err
	}
	return os.Chmod(path, 0o755)
}
