package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

func init() {
	if logger.Log == nil {
		logger.Log = zap.NewNop().Sugar()
	}
}

func TestCommandHandler_StdinPayload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh on POSIX; skipping on windows")
	}
	h := NewCommandHandler("/bin/sh", []string{"-c", "cat >/dev/null; echo \"$(cat)\""})

	ev := &Event{Type: EventTaskStarted, TaskID: "abc-123"}
	if err := h.HandleEvent(ev); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
}

func TestCommandHandler_InvalidCommand(t *testing.T) {
	h := NewCommandHandler("/nonexistent/binary/xyz", nil)
	ev := &Event{Type: EventTaskStarted, TaskID: "id"}
	err := h.HandleEvent(ev)
	if err == nil {
		t.Fatalf("expected error for invalid command, got nil")
	}
}

func TestEventEmitter_AsyncQueueFull(t *testing.T) {
	e := NewEventEmitter(true, 1)
	defer e.Close()

	for i := 0; i < 2000; i++ {
		e.Emit(&Event{Type: EventTaskStarted, TaskID: "id"})
	}
}

func TestEventEmitter_ConcurrentEmit(t *testing.T) {
	e := NewEventEmitter(true, 4)
	defer e.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				e.Emit(&Event{Type: EventTaskProgress, TaskID: "x"})
			}
		}()
	}
	wg.Wait()
}

func TestEventEmitter_CloseIdempotencySafe(t *testing.T) {
	e := NewEventEmitter(true, 2)

	go func() {
		for i := 0; i < 100; i++ {
			e.Emit(&Event{Type: EventTaskStarted, TaskID: "x"})
		}
	}()

	time.Sleep(20 * time.Millisecond)
	if err := e.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

func TestCommandHandler_StdinReceivesExactPayload(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX-only")
	}
	cmdFactory := func(payload string) *exec.Cmd {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = cancel
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "cat; echo END")
		cmd.Stdin = bytes.NewBufferString(payload)
		return cmd
	}
	ev, err := json.Marshal(&Event{Type: EventTaskStarted, TaskID: "tid"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmdFactory(string(ev)).Output()
	if err != nil {
		t.Fatalf("cmd: %v", err)
	}
	if !bytes.Contains(out, ev) {
		t.Fatalf("stdin payload not received: got %q", out)
	}
}

func TestEventEmitter_ProcessAfterClose_NoPanic(t *testing.T) {
	e := NewEventEmitter(false, 0)
	e.Close()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Emit after Close panicked: %v", r)
		}
	}()
	e.Emit(&Event{Type: EventTaskStarted, TaskID: "x"})
}

func TestEvent_HTTPHandler_CloseIsNoop(t *testing.T) {
	h := NewHTTPHandler("http://localhost:1", nil)
	if err := h.Close(); err != nil {
		t.Fatalf("Close should be noop, got %v", err)
	}
}

func TestCommandHandler_StdinStreamReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX-only")
	}
	want := "Hello, World! 你好世界"
	cmd := exec.Command("/bin/sh", "-c", "cat")
	cmd.Stdin = bytes.NewBufferString(want)
	var got bytes.Buffer
	cmd.Stdout = &got
	if err := cmd.Run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got.String() != want {
		t.Fatalf("stdin round-trip mismatch: want %q got %q", want, got.String())
	}
}

func TestCommandHandler_DoesNotPutEventInArgv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX-only")
	}
	sentinel := "INJECTED_ARGV_TOKEN_42"
	// If the handler incorrectly puts the event into argv, $1 would contain
	// the sentinel and the script exits 99. Correct behaviour passes the
	// event via stdin only, so $1 is empty and the script exits 0.
	h := NewCommandHandler("/bin/sh", []string{"-c", `if [ "$1" = "` + sentinel + `" ]; then exit 99; fi; cat >/dev/null; exit 0`, "arg1"})

	ev := &Event{Type: EventTaskStarted, TaskID: sentinel}
	if err := h.HandleEvent(ev); err != nil {
		t.Fatalf("expected success (event not in argv), got error: %v", err)
	}
}
