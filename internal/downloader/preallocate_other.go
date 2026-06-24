//go:build !linux

package downloader

import (
	"errors"
	"os"
)

var errFallocateUnsupported = errors.New("fallocate not supported on this platform")

func syscallFallocate(file *os.File, size int64) error {
	return errFallocateUnsupported
}
