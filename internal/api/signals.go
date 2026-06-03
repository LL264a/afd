package api

import (
	"os"
	"syscall"
)

var gracefulShutdownHandler func(sig syscall.Signal) error

// RegisterGracefulShutdownHandler lets the CLI install a callback that
// performs a clean shutdown (flush logs, close connections, persist
// task state). RPC shutdown / forceShutdown invoke this with the
// appropriate signal so the rest of the process can run its deferred
// cleanup instead of being terminated by os.Exit.
func RegisterGracefulShutdownHandler(fn func(sig syscall.Signal) error) {
	gracefulShutdownHandler = fn
}

// requestGracefulShutdown asks the registered handler (if any) to
// perform a graceful shutdown. When no handler is registered it falls
// back to delivering SIGTERM to the current process so the main
// signal.Notify handler can drive the cleanup. Callers should treat
// this as best-effort and not panic on error.
func requestGracefulShutdown() error {
	const sig = syscall.SIGTERM
	if gracefulShutdownHandler != nil {
		return gracefulShutdownHandler(sig)
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
	return p.Signal(sig)
}
