//go:build (linux || darwin || freebsd || openbsd) && !js

package task

import (
	"fmt"
	"syscall"
)

func GetAvailableSpace(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}
