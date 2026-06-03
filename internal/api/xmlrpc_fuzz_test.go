package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

func init() {
	if logger.Log == nil {
		logger.Log = zap.NewNop().Sugar()
	}
}

func newFuzzXMLRPC(t testing.TB) *XMLRPCServer {
	t.Helper()
	tq := task.NewTaskQueue(8)
	return NewXMLRPCServer(tq)
}

func FuzzXMLRPCServe(f *testing.F) {
	s := newFuzzXMLRPC(f)

	f.Add([]byte(`<?xml version="1.0"?><methodCall><methodName>system.listMethods</methodName><params></params></methodCall>`))
	f.Add([]byte(`<?xml version="1.0"?><methodCall><methodName>aria2.getVersion</methodName></methodCall>`))
	f.Add([]byte(`<methodCall><methodName>aria2.tellActive</methodName><params></params></methodCall>`))
	f.Add([]byte(`not xml`))
	f.Add([]byte(``))
	f.Add([]byte(`<?xml version="1.0"?><methodCall><methodName></methodName></methodCall>`))
	f.Add([]byte(`<?xml version="1.0"?><methodCall><methodName>unknown.method</methodName><params></params></methodCall>`))
	f.Add([]byte(`<?xml version="1.0"?><methodResponse><fault><value><struct><member><name>faultCode</name><value><int>1</int></value></member></struct></value></fault></methodResponse>`))
	f.Add([]byte(`<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY lol "lol">]><methodCall><methodName>&lol;</methodName></methodCall>`))

	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<20 {
			return
		}
		req := httptest.NewRequest(http.MethodPost, "/xmlrpc", bytes.NewReader(body))
		req.Header.Set("Content-Type", "text/xml")
		rec := httptest.NewRecorder()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("xmlrpc panicked on %q: %v", body, r)
			}
		}()
		s.ServeHTTP(rec, req)

		if rec.Code >= 500 {
			t.Errorf("xmlrpc 5xx on %q: status %d body %q", body, rec.Code, rec.Body.String())
		}
		if strings.TrimSpace(rec.Body.String()) == "" {
			t.Errorf("xmlrpc empty body on %q: status %d", body, rec.Code)
		}
	})
}
