//go:build linux

package downloader

import (
	"os"
	"syscall"
)

func syscallFallocate(file *os.File, size int64) error {
	return syscall.Fallocate(int(file.Fd()), 0, 0, size)
}
