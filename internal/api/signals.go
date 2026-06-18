package api

import (
	"os"
	"sync/atomic"
	"syscall"
)

var gracefulShutdownHandler atomic.Value // 存储 func(syscall.Signal) error

// RegisterGracefulShutdownHandler lets the CLI install a callback that
// performs a clean shutdown (flush logs, close connections, persist
// task state). RPC shutdown / forceShutdown invoke this with the
// appropriate signal so the rest of the process can run its deferred
// cleanup instead of being terminated by os.Exit.
func RegisterGracefulShutdownHandler(fn func(sig syscall.Signal) error) {
	gracefulShutdownHandler.Store(fn)
}

// requestGracefulShutdown asks the registered handler (if any) to
// perform a graceful shutdown. When no handler is registered it falls
// back to delivering SIGTERM to the current process so the main
// signal.Notify handler can drive the cleanup. Callers should treat
// this as best-effort and not panic on error.
func requestGracefulShutdown() error {
	sig := syscall.SIGTERM
	if fn, ok := gracefulShutdownHandler.Load().(func(syscall.Signal) error); ok && fn != nil {
		return fn(sig)
	}
	return procSignal(sig)
}

// procSignal sends a signal to the current process so the process's own
// signal.Notify handler can drive the shutdown. Returns an error if
// the signal cannot be delivered (e.g. in tests where the handler is
// not installed).
func procSignal(sig syscall.Signal) error {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	// Windows 上 SIGTERM 不被支持，使用 os.Interrupt 替代
	if err := p.Signal(sig); err != nil {
		// 回退到 os.Interrupt
		return p.Signal(os.Interrupt)
	}
	return nil
}
