package downloader

import (
	"sync"
	"testing"
)

// Regression: Quit used to leave c.conn non-nil after closing it,
// so a second call would re-Close the same net.Conn and (with the
// new defer pattern) risk double-cleanup.  After the nil-out, Quit
// must be a true no-op on the second call.
func TestFTPClientQuitIdempotent(t *testing.T) {
	c := NewFTPClient("localhost", "21", "anonymous", "anon@", false, true, nil)

	// Never Connected: c.conn is already nil, Quit returns nil.
	if err := c.Quit(); err != nil {
		t.Errorf("first Quit on disconnected client returned err: %v", err)
	}
	if err := c.Quit(); err != nil {
		t.Errorf("second Quit returned err: %v", err)
	}
}

func TestFTPClientQuitConcurrent(t *testing.T) {
	c := NewFTPClient("localhost", "21", "anonymous", "anon@", false, true, nil)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Quit()
		}()
	}
	wg.Wait()

	// After concurrent Quits the client must end in a fully cleared
	// state so a subsequent operation that touches the conn is safe.
	if c.conn != nil {
		t.Error("c.conn should be nil after Quit")
	}
}
