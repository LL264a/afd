package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

func init() {
	if logger.Log == nil {
		logger.Log = zap.NewNop().Sugar()
	}
}

func newFuzzJSONRPC(t testing.TB) *JSONRPCServer {
	t.Helper()
	dir := t.TempDir()
	tq := task.NewTaskQueue(8)
	cfg := config.DefaultConfig()
	return NewJSONRPCServer(tq, task.NewTaskStore(dir), cfg, "test")
}

func FuzzJSONRPCServe(f *testing.F) {
	s := newFuzzJSONRPC(f)

	f.Add([]byte(`{"jsonrpc":"2.0","id":"1","method":"getVersion","params":[]}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"2","method":"tellActive","params":[]}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"3","method":"multicall","params":[[]]}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{`))
	f.Add([]byte(``))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"jsonrpc":"1.0","method":"foo"}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":123,"params":"oops"}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":[1,2],"method":"getVersion","params":[null]}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":null,"method":"addUri","params":[["://no-scheme"],{}]}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"shutdown","params":[]}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"\u0000\u0001","params":[]}`))

	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest(http.MethodPost, "/jsonrpc", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)

		if rec.Code == 0 {
			t.Errorf("status code not set")
		}
		if rec.Code >= 500 {
			t.Errorf("server error on input %q: status %d body %q", body, rec.Code, rec.Body.String())
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q, want application/json", ct)
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Errorf("body is not valid JSON for input %q: %v body=%q", body, err, rec.Body.String())
		}
	})
}

func FuzzJSONRPCMulticallParams(f *testing.F) {
	s := newFuzzJSONRPC(f)

	f.Add([]byte(`[[]]`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(`[{},[]]`))
	f.Add([]byte(`[[{"methodName":"getVersion","params":[]}]]`))
	f.Add([]byte(`[[{"methodName":"getVersion","params":[]},{"methodName":"getVersion","params":[]}]]`))
	f.Add([]byte(`[[]]`))

	f.Fuzz(func(t *testing.T, paramsJSON []byte) {
		body := []byte(`{"jsonrpc":"2.0","id":1,"method":"multicall","params":`)
		body = append(body, paramsJSON...)
		body = append(body, '}')

		req := httptest.NewRequest(http.MethodPost, "/jsonrpc", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("multicall panicked on %q: %v", body, r)
			}
		}()
		s.ServeHTTP(rec, req)

		if rec.Code >= 500 {
			t.Errorf("multicall 5xx on %q: status %d body %q", body, rec.Code, rec.Body.String())
		}
	})
}
