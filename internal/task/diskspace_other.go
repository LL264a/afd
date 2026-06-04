//go:build !windows && !(linux || darwin || freebsd || openbsd || netbsd)

package task

import "fmt"

func GetAvailableSpace(path string) (int64, error) {
	return 1<<63 - 1, nil // report max available on unsupported platforms (js/wasm, solaris, illumos)
}
