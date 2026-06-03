package api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/nexus-dl/afd/internal/task"
	"github.com/nexus-dl/afd/pkg/logger"
	"go.uber.org/zap"
)

type XMLRPCRequest struct {
	XMLName    xml.Name `xml:"methodCall"`
	MethodName string   `xml:"methodName"`
	Params     struct {
		Param []struct {
			Value struct {
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
				Array  struct {
					Data struct {
						Value []struct {
							String string `xml:"string,omitempty"`
						} `xml:"value"`
					} `xml:"data"`
				} `xml:"array,omitempty"`
			} `xml:"value"`
		} `xml:"param"`
	} `xml:"params"`
}

type XMLRPCMember struct {
	Name  string `xml:"name"`
	Value struct {
		String string `xml:"string,omitempty"`
		Int    int64  `xml:"int,omitempty"`
		I4     int64  `xml:"i4,omitempty"`
	} `xml:"value"`
}

type XMLRPCResponse struct {
	XMLName xml.Name `xml:"methodResponse"`
	Params  struct {
		Param struct {
			Value struct {
				Struct struct {
					Member []XMLRPCMember `xml:"member"`
				} `xml:"struct,omitempty"`
				Array struct {
					Data struct {
						Value []struct {
							String string `xml:"string,omitempty"`
							Int    int64  `xml:"int,omitempty"`
						} `xml:"value"`
					} `xml:"data"`
				} `xml:"array,omitempty"`
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
			} `xml:"value"`
		} `xml:"param"`
	} `xml:"params"`
	Fault *struct {
		Value struct {
			Struct struct {
				Member []XMLRPCMember `xml:"member"`
			} `xml:"struct"`
		} `xml:"value"`
	} `xml:"fault,omitempty"`
}

type XMLRPCServer struct {
	taskQueue *task.TaskQueue
	logger    *zap.SugaredLogger
	mu        sync.Mutex
}

func NewXMLRPCServer(taskQueue *task.TaskQueue) *XMLRPCServer {
	return &XMLRPCServer{
		taskQueue: taskQueue,
		logger:    logger.Log.Named("xmlrpc"),
	}
}

func (s *XMLRPCServer) handleAddURI(req *XMLRPCRequest) (*XMLRPCResponse, error) {
	var uri string
	if len(req.Params.Param) >= 1 {
		if req.Params.Param[0].Value.Array.Data.Value != nil && len(req.Params.Param[0].Value.Array.Data.Value) > 0 {
			uri = req.Params.Param[0].Value.Array.Data.Value[0].String
		} else {
			uri = req.Params.Param[0].Value.String
		}
	}

	if uri == "" {
		return s.makeError(-32602, "Missing URI parameter"), nil
	}

	newTask := task.NewTask(uri, "")
	if err := s.taskQueue.Add(newTask); err != nil {
		return s.makeError(-1, "Failed to add task"), nil
	}
	taskID := newTask.ID

	resp := &XMLRPCResponse{}
	resp.Params.Param.Value.String = taskID
	return resp, nil
}

func (s *XMLRPCServer) handleTellStatus(req *XMLRPCRequest) (*XMLRPCResponse, error) {
	var gid string
	if len(req.Params.Param) >= 1 {
		gid = req.Params.Param[0].Value.String
	}

	t, err := s.taskQueue.Get(gid)
	if err != nil {
		return s.makeError(-1, "Task not found"), nil
	}

	resp := &XMLRPCResponse{}
	safe := t.GetSafe()

	resp.Params.Param.Value.Struct.Member = []XMLRPCMember{
		{
			Name: "gid",
			Value: struct {
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
			}{String: gid},
		},
		{
			Name: "status",
			Value: struct {
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
			}{String: string(safe.Status)},
		},
		{
			Name: "totalLength",
			Value: struct {
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
			}{I4: safe.TotalSize},
		},
		{
			Name: "completedLength",
			Value: struct {
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
			}{I4: safe.DownloadedSize},
		},
		{
			Name: "downloadSpeed",
			Value: struct {
				String string `xml:"string,omitempty"`
				Int    int64  `xml:"int,omitempty"`
				I4     int64  `xml:"i4,omitempty"`
			}{I4: safe.Speed},
		},
	}

	return resp, nil
}

func (s *XMLRPCServer) handleRemove(req *XMLRPCRequest) (*XMLRPCResponse, error) {
	var gid string
	if len(req.Params.Param) >= 1 {
		gid = req.Params.Param[0].Value.String
	}

	err := s.taskQueue.Remove(gid)
	if err != nil {
		return s.makeError(-1, "Failed to remove task"), nil
	}

	resp := &XMLRPCResponse{}
	resp.Params.Param.Value.String = gid
	return resp, nil
}

func (s *XMLRPCServer) handlePause(req *XMLRPCRequest) (*XMLRPCResponse, error) {
	var gid string
	if len(req.Params.Param) >= 1 {
		gid = req.Params.Param[0].Value.String
	}

	err := s.taskQueue.Pause(gid)
	if err != nil {
		return s.makeError(-1, "Failed to pause task"), nil
	}

	resp := &XMLRPCResponse{}
	resp.Params.Param.Value.String = gid
	return resp, nil
}

func (s *XMLRPCServer) handleResume(req *XMLRPCRequest) (*XMLRPCResponse, error) {
	var gid string
	if len(req.Params.Param) >= 1 {
		gid = req.Params.Param[0].Value.String
	}

	err := s.taskQueue.Resume(gid)
	if err != nil {
		return s.makeError(-1, "Failed to resume task"), nil
	}

	resp := &XMLRPCResponse{}
	resp.Params.Param.Value.String = gid
	return resp, nil
}

func (s *XMLRPCServer) makeError(code int, message string) *XMLRPCResponse {
	return &XMLRPCResponse{
		Fault: &struct {
			Value struct {
				Struct struct {
					Member []XMLRPCMember `xml:"member"`
				} `xml:"struct"`
			} `xml:"value"`
		}{
			Value: struct {
				Struct struct {
					Member []XMLRPCMember `xml:"member"`
				} `xml:"struct"`
			}{
				Struct: struct {
					Member []XMLRPCMember `xml:"member"`
				}{
					Member: []XMLRPCMember{
						{
							Name: "faultCode",
							Value: struct {
								String string `xml:"string,omitempty"`
								Int    int64  `xml:"int,omitempty"`
								I4     int64  `xml:"i4,omitempty"`
							}{Int: int64(code)},
						},
						{
							Name: "faultString",
							Value: struct {
								String string `xml:"string,omitempty"`
								Int    int64  `xml:"int,omitempty"`
								I4     int64  `xml:"i4,omitempty"`
							}{String: message},
						},
					},
				},
			},
		},
	}
}

func (s *XMLRPCServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	const maxBodySize = 10 * 1024 * 1024
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	r.Body.Close()
	if err != nil {
		s.logger.Errorw("Failed to read request body", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if len(bodyBytes) == maxBodySize {
		s.logger.Warnw("Request body hit size limit", "max", maxBodySize)
	}

	var req XMLRPCRequest
	if err = xml.Unmarshal(bodyBytes, &req); err != nil {
		s.logger.Errorw("Failed to parse XML-RPC request", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	s.logger.Debugw("Received XML-RPC request", "method", req.MethodName)

	var resp *XMLRPCResponse

	switch req.MethodName {
	case "aria2.addUri":
		resp, err = s.handleAddURI(&req)
	case "aria2.tellStatus":
		resp, err = s.handleTellStatus(&req)
	case "aria2.remove":
		resp, err = s.handleRemove(&req)
	case "aria2.pause":
		resp, err = s.handlePause(&req)
	case "aria2.unpause":
		resp, err = s.handleResume(&req)
	default:
		resp = s.makeError(-32601, fmt.Sprintf("Method not found: %s", req.MethodName))
	}

	if err != nil {
		resp = s.makeError(-32500, err.Error())
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)

	if err = xml.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Errorw("Failed to write XML-RPC response", "error", err)
	}
}

func (s *XMLRPCServer) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/xmlrpc", s)
	s.logger.Infow("XML-RPC server registered", "path", "/xmlrpc")
}
