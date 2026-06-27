//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// setSysProcAttr 在 Unix 上为新进程创建新会话（setsid），脱离控制终端，
// 避免终端关闭时子进程收到 SIGHUP 被杀死。
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
