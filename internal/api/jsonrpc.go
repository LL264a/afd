package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/config"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

type JSONRPCServer struct {
	taskQueue *task.TaskQueue
	taskStore *task.TaskStore
	config    *config.Config
	version   string
	logger    *zap.SugaredLogger
	mu        sync.RWMutex
	startedAt time.Time
}

func NewJSONRPCServer(taskQueue *task.TaskQueue, taskStore *task.TaskStore, cfg *config.Config, ver string) *JSONRPCServer {
	if ver == "" {
		ver = "1.0.0"
	}
	return &JSONRPCServer{
		taskQueue: taskQueue,
		taskStore: taskStore,
		config:    cfg,
		version:   ver,
		logger:    logger.Log.Named("jsonrpc"),
		startedAt: time.Now(),
	}
}

func (s *JSONRPCServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendJSONRPCError(w, nil, http.StatusMethodNotAllowed, -32600, "Invalid request: only POST is allowed")
		return
	}

	const maxBodySize = 10 * 1024 * 1024
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	r.Body.Close()
	if err != nil {
		s.logger.Errorw("Failed to read request body", "error", err)
		sendJSONRPCError(w, nil, http.StatusBadRequest, -32700, "Failed to read request body")
		return
	}
	if len(bodyBytes) == maxBodySize {
		s.logger.Warnw("Request body hit size limit", "max", maxBodySize)
	}

	var req JSONRPCRequest
	if len(bodyBytes) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(bodyBytes)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			s.logger.Errorw("Failed to parse JSON-RPC request", "error", err)
			sendJSONRPCError(w, nil, http.StatusBadRequest, -32700, "Parse error")
			return
		}
	}

	if req.JSONRPC != "" && req.JSONRPC != "2.0" {
		sendJSONRPCError(w, req.ID, http.StatusBadRequest, -32600, "Invalid JSON-RPC version")
		return
	}

	var params []interface{}
	if len(req.Params) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(req.Params)))
		if err := dec.Decode(&params); err != nil {
			paramMap := map[string]interface{}{}
			dec2 := json.NewDecoder(strings.NewReader(string(req.Params)))
			if err2 := dec2.Decode(&paramMap); err2 != nil {
				sendJSONRPCError(w, req.ID, http.StatusBadRequest, -32602, "Invalid params")
				return
			}
			params = []interface{}{paramMap}
		}
	}

	s.logger.Debugw("Received JSON-RPC request", "method", req.Method)

	result, err := s.handleMethod(req.Method, params)
	if err != nil {
		var code int
		if e, ok := err.(*jsonRPCError); ok {
			code = e.code
			err = e.err
		} else {
			code = -32500
		}
		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			Error:   &JSONRPCError{Code: code, Message: err.Error()},
			ID:      req.ID,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		Result:  result,
		ID:      req.ID,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

type jsonRPCError struct {
	code int
	err  error
}

func (e *jsonRPCError) Error() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *jsonRPCError) Unwrap() error {
	return e.err
}

func newJSONRPCError(code int, err error) *jsonRPCError {
	return &jsonRPCError{code: code, err: err}
}

func sendJSONRPCError(w http.ResponseWriter, id json.RawMessage, httpCode, rpcCode int, message string) {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		Error:   &JSONRPCError{Code: rpcCode, Message: message},
		ID:      id,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	json.NewEncoder(w).Encode(resp)
}

func (s *JSONRPCServer) handleMethod(method string, params []interface{}) (interface{}, error) {
	switch method {
	case "aria2.addUri":
		return s.addUri(params)
	case "aria2.addTorrent":
		return s.addTorrent(params)
	case "aria2.addMetalink":
		return s.addMetalink(params)
	case "aria2.remove":
		return s.remove(params)
	case "aria2.forceRemove":
		return s.remove(params)
	case "aria2.pause":
		return s.pause(params)
	case "aria2.pauseAll":
		return s.pauseAll(params)
	case "aria2.forcePause":
		return s.pause(params)
	case "aria2.forcePauseAll":
		return s.pauseAll(params)
	case "aria2.unpause":
		return s.unpause(params)
	case "aria2.unpauseAll":
		return s.unpauseAll(params)
	case "aria2.tellStatus":
		return s.tellStatus(params)
	case "aria2.tellActive":
		return s.tellActive(params)
	case "aria2.tellWaiting":
		return s.tellWaiting(params)
	case "aria2.tellStopped":
		return s.tellStopped(params)
	case "aria2.getFiles":
		return s.getFiles(params)
	case "aria2.getPeers":
		return s.getPeers(params)
	case "aria2.getServers":
		return s.getServers(params)
	case "aria2.getUris":
		return s.getUris(params)
	case "aria2.changeGlobalOption":
		return s.changeGlobalOption(params)
	case "aria2.changeOption":
		return s.changeOption(params)
	case "aria2.getGlobalOption":
		return s.getGlobalOption(params)
	case "aria2.getOption":
		return s.getOption(params)
	case "aria2.getGlobalStat":
		return s.getGlobalStat(params)
	case "aria2.changePosition":
		return s.changePosition(params)
	case "aria2.changeUri":
		return s.changeUri(params)
	case "aria2.saveSession":
		return s.saveSession(params)
	case "aria2.shutdown":
		return s.shutdown()
	case "aria2.forceShutdown":
		return s.forceShutdown()
	case "aria2.getVersion":
		return s.getVersion()
	case "aria2.getSessionInfo":
		return s.getSessionInfo()
	case "aria2.purgeDownloadResult":
		return s.purgeDownloadResult(params)
	case "aria2.removeDownloadResult":
		return s.removeDownloadResult(params)
	case "system.multicall":
		return s.multicall(params)
	case "system.listMethods":
		return s.listMethods()
	case "system.listNotifications":
		return []string{}, nil
	default:
		return nil, newJSONRPCError(-32601, fmt.Errorf("Method not found: %s", method))
	}
}

func paramString(params []interface{}, idx int) string {
	if len(params) <= idx {
		return ""
	}
	if v, ok := params[idx].(string); ok {
		return v
	}
	return ""
}

func paramMap(params []interface{}, idx int) map[string]interface{} {
	if len(params) <= idx {
		return nil
	}
	if m, ok := params[idx].(map[string]interface{}); ok {
		return m
	}
	return nil
}

func paramInt(params []interface{}, idx int) int {
	if len(params) <= idx {
		return 0
	}
	switch v := params[idx].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		i, _ := strconv.Atoi(v)
		return i
	}
	return 0
}

func paramStringSlice(params []interface{}, idx int) []string {
	if len(params) <= idx {
		return nil
	}
	arr, ok := params[idx].([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if str, ok := v.(string); ok {
			out = append(out, str)
		}
	}
	return out
}

func paramKeys(params []interface{}, idx int) []string {
	return paramStringSlice(params, idx)
}

func (s *JSONRPCServer) addUri(params []interface{}) (interface{}, error) {
	uris := paramStringSlice(params, 0)
	if len(uris) == 0 {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing URIs parameter"))
	}
	options := paramMap(params, 1)
	position := paramString(params, 2)

	gids := make([]string, 0, len(uris))
	for _, uri := range uris {
		if !isValidURL(uri) {
			return nil, newJSONRPCError(-1, fmt.Errorf("Invalid URI: %s", uri))
		}
		t := task.NewTask(uri, s.outputDir(options))
		s.applyOptions(t, options)
		if err := s.taskQueue.Add(t); err != nil {
			return nil, newJSONRPCError(-1, err)
		}
		s.persistTask(t)
		gids = append(gids, t.ID)
		_ = position
	}
	return gids, nil
}

func (s *JSONRPCServer) addTorrent(params []interface{}) (interface{}, error) {
	if len(params) < 1 {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing torrent parameter"))
	}
	torrentField := params[0]
	torrentB64, ok := torrentField.(string)
	if !ok {
		return nil, newJSONRPCError(-1, fmt.Errorf("Invalid torrent parameter"))
	}
	options := paramMap(params, 1)
	_ = options

	if torrentB64 == "" {
		return nil, newJSONRPCError(-1, fmt.Errorf("Empty torrent"))
	}

	t := task.NewTask(torrentB64, s.outputDir(options))
	if err := s.applyOptions(t, options); err != nil {
		return nil, newJSONRPCError(-1, err)
	}
	if err := s.taskQueue.Add(t); err != nil {
		return nil, newJSONRPCError(-1, err)
	}
	s.persistTask(t)
	return t.ID, nil
}

func (s *JSONRPCServer) addMetalink(params []interface{}) (interface{}, error) {
	if len(params) < 1 {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing metalink parameter"))
	}
	metalinkField, ok := params[0].(string)
	if !ok {
		return nil, newJSONRPCError(-1, fmt.Errorf("Invalid metalink parameter"))
	}
	if metalinkField == "" {
		return nil, newJSONRPCError(-1, fmt.Errorf("Empty metalink"))
	}

	gids := []string{}
	t := task.NewTask(metalinkField, s.outputDir(nil))
	if err := s.taskQueue.Add(t); err != nil {
		return nil, newJSONRPCError(-1, err)
	}
	s.persistTask(t)
	gids = append(gids, t.ID)
	return gids, nil
}

func (s *JSONRPCServer) remove(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	if err := s.taskQueue.Remove(gid); err != nil {
		return nil, newJSONRPCError(-1, err)
	}
	_ = s.taskStore.Delete(gid)
	return gid, nil
}

func (s *JSONRPCServer) pause(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	if err := s.taskQueue.Pause(gid); err != nil {
		return nil, newJSONRPCError(-1, err)
	}
	return gid, nil
}

func (s *JSONRPCServer) pauseAll(_ []interface{}) (interface{}, error) {
	tasks := s.taskQueue.List()
	for _, t := range tasks {
		if t.GetStatus() == task.StatusDownloading {
			_ = s.taskQueue.Pause(t.ID)
		}
	}
	return "OK", nil
}

func (s *JSONRPCServer) unpause(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	if err := s.taskQueue.Resume(gid); err != nil {
		return nil, newJSONRPCError(-1, err)
	}
	return gid, nil
}

func (s *JSONRPCServer) unpauseAll(_ []interface{}) (interface{}, error) {
	tasks := s.taskQueue.List()
	for _, t := range tasks {
		if t.GetStatus() == task.StatusPaused {
			_ = s.taskQueue.Resume(t.ID)
		}
	}
	return "OK", nil
}

func (s *JSONRPCServer) tellStatus(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	t, err := s.taskQueue.Get(gid)
	if err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	keys := paramKeys(params, 1)
	return s.taskToStatus(t, keys), nil
}

func (s *JSONRPCServer) tellActive(params []interface{}) (interface{}, error) {
	keys := paramKeys(params, 0)
	tasks := s.taskQueue.List()
	out := []interface{}{}
	for _, t := range tasks {
		st := t.GetStatus()
		if st == task.StatusDownloading || st == task.StatusPending {
			out = append(out, s.taskToStatus(t, keys))
		}
	}
	return out, nil
}

func (s *JSONRPCServer) tellWaiting(params []interface{}) (interface{}, error) {
	offset := paramInt(params, 0)
	num := paramInt(params, 1)
	keys := paramKeys(params, 2)
	tasks := s.taskQueue.List()
	waiting := []*task.Task{}
	for _, t := range tasks {
		if t.GetStatus() == task.StatusPending {
			waiting = append(waiting, t)
		}
	}
	end := offset + num
	if end > len(waiting) {
		end = len(waiting)
	}
	if offset > len(waiting) {
		offset = len(waiting)
	}
	if offset < 0 {
		offset = 0
	}
	if num < 0 {
		num = 0
	}
	out := []interface{}{}
	for _, t := range waiting[offset:end] {
		out = append(out, s.taskToStatus(t, keys))
	}
	return out, nil
}

func (s *JSONRPCServer) tellStopped(params []interface{}) (interface{}, error) {
	offset := paramInt(params, 0)
	num := paramInt(params, 1)
	keys := paramKeys(params, 2)
	tasks := s.taskQueue.List()
	stopped := []*task.Task{}
	for _, t := range tasks {
		st := t.GetStatus()
		if st == task.StatusDone || st == task.StatusFailed || st == task.StatusCancelled {
			stopped = append(stopped, t)
		}
	}
	end := offset + num
	if end > len(stopped) {
		end = len(stopped)
	}
	if offset > len(stopped) {
		offset = len(stopped)
	}
	if offset < 0 {
		offset = 0
	}
	if num < 0 {
		num = 0
	}
	out := []interface{}{}
	for _, t := range stopped[offset:end] {
		out = append(out, s.taskToStatus(t, keys))
	}
	return out, nil
}

func (s *JSONRPCServer) getFiles(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	t, err := s.taskQueue.Get(gid)
	if err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	safe := t.GetSafe()
	files := []map[string]interface{}{}
	if len(safe.Chunks) > 0 {
		files = append(files, map[string]interface{}{
			"index":           "1",
			"path":            filepath.Base(safe.OutputPath),
			"length":          fmt.Sprintf("%d", safe.TotalSize),
			"completedLength": fmt.Sprintf("%d", safe.DownloadedSize),
			"selected":        "true",
			"uris":            []map[string]string{{"uri": safe.URL, "status": "used"}},
		})
	}
	return files, nil
}

func (s *JSONRPCServer) getPeers(_ []interface{}) (interface{}, error) {
	return []map[string]interface{}{}, nil
}

func (s *JSONRPCServer) getServers(params []interface{}) (interface{}, error) {
	_ = paramString(params, 0)
	return []map[string]string{}, nil
}

func (s *JSONRPCServer) getUris(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	t, err := s.taskQueue.Get(gid)
	if err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	safe := t.GetSafe()
	return []map[string]interface{}{
		{"uri": safe.URL, "status": "used"},
	}, nil
}

func (s *JSONRPCServer) changeGlobalOption(params []interface{}) (interface{}, error) {
	options := paramMap(params, 0)
	if options == nil {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing options parameter"))
	}
	s.logger.Infow("changeGlobalOption", "options", options)
	return "OK", nil
}

func (s *JSONRPCServer) changeOption(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	options := paramMap(params, 1)
	if options == nil {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing options parameter"))
	}
	if _, err := s.taskQueue.Get(gid); err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	s.logger.Infow("changeOption", "gid", gid, "options", options)
	return "OK", nil
}

func (s *JSONRPCServer) getGlobalOption(_ []interface{}) (interface{}, error) {
	return map[string]string{}, nil
}

func (s *JSONRPCServer) getOption(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	if _, err := s.taskQueue.Get(gid); err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	return map[string]string{}, nil
}

func (s *JSONRPCServer) getGlobalStat(_ []interface{}) (interface{}, error) {
	tasks := s.taskQueue.List()
	var active, waiting, stopped int
	var speed int64
	for _, t := range tasks {
		safe := t.GetSafe()
		switch safe.Status {
		case task.StatusDownloading, task.StatusPending:
			active++
		case task.StatusPaused:
			waiting++
		default:
			stopped++
		}
		speed += safe.Speed
	}
	return map[string]interface{}{
		"downloadSpeed":   fmt.Sprintf("%d", speed),
		"uploadSpeed":     "0",
		"numActive":       fmt.Sprintf("%d", active),
		"numWaiting":      fmt.Sprintf("%d", waiting),
		"numStopped":      fmt.Sprintf("%d", stopped),
		"numStoppedTotal": fmt.Sprintf("%d", stopped),
	}, nil
}

func (s *JSONRPCServer) changePosition(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	pos := paramInt(params, 1)
	how := paramString(params, 2)
	if _, err := s.taskQueue.Get(gid); err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	s.logger.Debugw("changePosition", "gid", gid, "pos", pos, "how", how)
	return "OK", nil
}

func (s *JSONRPCServer) changeUri(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	uris := paramStringSlice(params, 1)
	pos := paramInt(params, 2)
	if _, err := s.taskQueue.Get(gid); err != nil {
		return nil, newJSONRPCError(-1, fmt.Errorf("Task not found: %s", gid))
	}
	for _, u := range uris {
		if !isValidURL(u) {
			return nil, newJSONRPCError(-1, fmt.Errorf("Invalid URI: %s", u))
		}
	}
	s.logger.Debugw("changeUri", "gid", gid, "uris", uris, "pos", pos)
	return []string{gid}, nil
}

func (s *JSONRPCServer) saveSession(_ []interface{}) (interface{}, error) {
	tasks := s.taskQueue.List()
	for _, t := range tasks {
		if err := s.taskStore.Save(t); err != nil {
			s.logger.Warnw("saveSession persist failed", "gid", t.ID, "err", err)
		}
	}
	return "OK", nil
}

func (s *JSONRPCServer) shutdown() (interface{}, error) {
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := requestGracefulShutdown(); err != nil {
			s.logger.Warnw("Graceful shutdown request failed; forcing exit", "err", err)
			os.Exit(1)
		}
	}()
	return "OK", nil
}

func (s *JSONRPCServer) forceShutdown() (interface{}, error) {
	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := requestGracefulShutdown(); err != nil {
			s.logger.Warnw("Force shutdown request failed; falling back to os.Exit", "err", err)
		}
	}()
	return "OK", nil
}

func (s *JSONRPCServer) getVersion() (interface{}, error) {
	return map[string][]string{
		"version": {s.version},
	}, nil
}

func (s *JSONRPCServer) getSessionInfo() (interface{}, error) {
	return map[string]string{
		"sessionId": "nexus-dl-" + s.startedAt.Format("20060102-150405"),
	}, nil
}

func (s *JSONRPCServer) purgeDownloadResult(_ []interface{}) (interface{}, error) {
	return "OK", nil
}

func (s *JSONRPCServer) removeDownloadResult(params []interface{}) (interface{}, error) {
	gid := paramString(params, 0)
	if gid == "" {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing gid parameter"))
	}
	return "OK", nil
}

func (s *JSONRPCServer) multicall(params []interface{}) (interface{}, error) {
	if len(params) == 0 {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Missing multicall params"))
	}
	calls, ok := params[0].([]interface{})
	if !ok {
		return nil, newJSONRPCError(-32602, fmt.Errorf("Invalid multicall params"))
	}
	// 限制调用数量，防止通过大量调用消耗资源
	if len(calls) > 32 {
		return nil, newJSONRPCError(-32603, fmt.Errorf("Too many multicall requests (max 32)"))
	}
	results := make([]interface{}, 0, len(calls))
	for _, c := range calls {
		callMap, ok := c.(map[string]interface{})
		if !ok {
			results = append(results, map[string]interface{}{"error": map[string]interface{}{"code": -32602, "message": "Invalid call object"}})
			continue
		}
		methodName, _ := callMap["methodName"].(string)
		// 拒绝嵌套 multicall，防止递归导致的栈溢出 DoS
		if methodName == "system.multicall" {
			results = append(results, map[string]interface{}{"error": map[string]interface{}{"code": -32600, "message": "Nested multicall is not allowed"}})
			continue
		}
		callParamsRaw, _ := callMap["params"].([]interface{})
		res, err := s.handleMethod(methodName, callParamsRaw)
		entry := map[string]interface{}{}
		if err != nil {
			code := -32500
			if e, ok := err.(*jsonRPCError); ok {
				code = e.code
				err = e.err
			}
			entry["error"] = map[string]interface{}{"code": code, "message": errString(err)}
			entry["code"] = code
			entry["message"] = errString(err)
		} else {
			entry["result"] = res
		}
		results = append(results, entry)
	}
	return []interface{}{results}, nil
}

func (s *JSONRPCServer) listMethods() (interface{}, error) {
	return []string{
		"aria2.addUri", "aria2.addTorrent", "aria2.addMetalink",
		"aria2.remove", "aria2.forceRemove",
		"aria2.pause", "aria2.pauseAll", "aria2.forcePause", "aria2.forcePauseAll",
		"aria2.unpause", "aria2.unpauseAll",
		"aria2.tellStatus", "aria2.tellActive", "aria2.tellWaiting", "aria2.tellStopped",
		"aria2.getFiles", "aria2.getPeers", "aria2.getServers", "aria2.getUris",
		"aria2.changeGlobalOption", "aria2.changeOption",
		"aria2.getGlobalOption", "aria2.getOption", "aria2.getGlobalStat",
		"aria2.changePosition", "aria2.changeUri",
		"aria2.saveSession", "aria2.shutdown", "aria2.forceShutdown",
		"aria2.getVersion", "aria2.getSessionInfo",
		"aria2.purgeDownloadResult", "aria2.removeDownloadResult",
		"system.multicall", "system.listMethods", "system.listNotifications",
	}, nil
}

func (s *JSONRPCServer) taskToStatus(t *task.Task, keys []string) map[string]interface{} {
	safe := t.GetSafe()
	all := map[string]interface{}{
		"gid":             safe.ID,
		"status":          string(safe.Status),
		"totalLength":     fmt.Sprintf("%d", safe.TotalSize),
		"completedLength": fmt.Sprintf("%d", safe.DownloadedSize),
		"uploadLength":    "0",
		"downloadSpeed":   fmt.Sprintf("%d", safe.Speed),
		"uploadSpeed":     "0",
		"errorMessage":    safe.Error,
		"dir":             safe.OutputPath,
		"files": []map[string]interface{}{
			{
				"index":           "1",
				"path":            filepath.Base(safe.OutputPath),
				"length":          fmt.Sprintf("%d", safe.TotalSize),
				"completedLength": fmt.Sprintf("%d", safe.DownloadedSize),
				"selected":        "true",
				"uris":            []map[string]string{{"uri": safe.URL, "status": "used"}},
			},
		},
		"bittorrent": map[string]interface{}{
			"info": map[string]string{
				"name": filepath.Base(safe.OutputPath),
			},
		},
		"followedBy":  []string{},
		"following":   "",
		"belongsTo":   "",
		"pieceLength": "0",
		"numPieces":   "0",
		"connections": "0",
		"seeder":      "false",
		"leecher":     "false",
	}
	if len(keys) == 0 {
		return all
	}
	filtered := make(map[string]interface{}, len(keys))
	for _, k := range keys {
		if v, ok := all[k]; ok {
			filtered[k] = v
		}
	}
	return filtered
}

func (s *JSONRPCServer) applyOptions(t *task.Task, options map[string]interface{}) error {
	if options == nil {
		return nil
	}
	if v, ok := options["dir"].(string); ok && v != "" {
		if isSafePath(v) {
			t.OutputPath = v
		}
	}
	if v, ok := options["out"].(string); ok && v != "" {
		if t.Metadata == nil {
			t.Metadata = make(map[string]string)
		}
		t.Metadata["filename"] = v
	}
	if v, ok := options["priority"]; ok {
		switch n := v.(type) {
		case float64:
			priority := int(n)
			if priority < 0 || priority > 10 {
				return fmt.Errorf("Priority must be between 0 and 10")
			}
			t.Priority = priority
		case int:
			if n < 0 || n > 10 {
				return fmt.Errorf("Priority must be between 0 and 10")
			}
			t.Priority = n
		case string:
			if i, err := strconv.Atoi(n); err == nil {
				if i < 0 || i > 10 {
					return fmt.Errorf("Priority must be between 0 and 10")
				}
				t.Priority = i
			}
		}
	}
	return nil
}

func (s *JSONRPCServer) outputDir(options map[string]interface{}) string {
	if options != nil {
		if v, ok := options["dir"].(string); ok && v != "" {
			if isSafePath(v) {
				return v
			}
			s.logger.Warnw("Rejected unsafe dir option, falling back to default", "dir", v)
		}
	}
	if s.config != nil && s.config.Node.DataDir != "" {
		return filepath.Join(s.config.Node.DataDir, "downloads")
	}
	return "downloads"
}

func (s *JSONRPCServer) persistTask(t *task.Task) {
	if err := s.taskStore.Save(t); err != nil {
		s.logger.Warnw("Failed to persist task", "gid", t.ID, "err", err)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
