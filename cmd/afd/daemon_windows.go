//go:build windows

package main

import "os/exec"

// setSysProcAttr 在 Windows 上是 no-op，因为 Windows 不支持 setsid。
// daemonize 在 Windows 上会提前返回，不会真正调用此函数。
func setSysProcAttr(cmd *exec.Cmd) {
	// Windows 不支持 setsid
}
