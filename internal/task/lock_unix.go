//go:build !windows

package task

import (
	"os"
	"syscall"
)

// flockFile 对文件加排他锁（非阻塞，失败返回 error）
func flockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// unflockFile 释放文件锁
func unflockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
