//go:build windows

package task

import "os"

// Windows 不支持 flock，使用 LockFileEx（简化为 no-op）
func flockFile(f *os.File) error {
	return nil
}

func unflockFile(f *os.File) error {
	return nil
}
