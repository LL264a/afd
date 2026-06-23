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

// XMLRPCValue is a generic XML-RPC value that can represent strings, ints,
// arrays, and structs. It is used for both request parsing and response
// building.
type XMLRPCValue struct {
	String string        `xml:"string,omitempty"`
	Int    int64         `xml:"int,omitempty"`
	I4     int64         `xml:"i4,omitempty"`
	Array  *XMLRPCArray  `xml:"array,omitempty"`
	Struct *XMLRPCStruct `xml:"struct,omitempty"`
}

// XMLRPCArray represents an XML-RPC <array><data>…</data></array> value.
type XMLRPCArray struct {
	Data struct {
		Value []XMLRPCValue `xml:"value"`
	} `xml:"data"`
}

// XMLRPCStruct represents an XML-RPC <struct>…</struct> value.
type XMLRPCStruct struct {
	Member []XMLRPCMember `xml:"member"`
}

// XMLRPCMember represents a <member> inside an XML-RPC struct.
type XMLRPCMember struct {
	Name  string      `xml:"name"`
	Value XMLRPCValue `xml:"value"`
}

// XMLRPCRequest is the parsed XML-RPC <methodCall> request. The param value
// only needs to cover the scalar/array shapes actually sent by clients
// (strings, ints, arrays of strings).
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
							Int    int64  `xml:"int,omitempty"`
							I4     int64  `xml:"i4,omitempty"`
						} `xml:"value"`
					} `xml:"data"`
				} `xml:"array,omitempty"`
			} `xml:"value"`
		} `xml:"param"`
	} `xml:"params"`
}

// XMLRPCResponse is the XML-RPC <methodResponse> document.
type XMLRPCResponse struct {
	XMLName xml.Name `xml:"methodResponse"`
	Params  struct {
		Param struct {
			Value XMLRPCValue `xml:"value"`
		} `xml:"param"`
	} `xml:"params"`
	Fault *struct {
		Value XMLRPCValue `xml:"value"`
	} `xml:"fault,omitempty"`
}

// XMLRPCServer serves XML-RPC requests by delegating to the JSON-RPC server,
// which owns the canonical method implementations. This keeps the two
// transports in sync without duplicating business logic.
type XMLRPCServer struct {
	taskQueue *task.TaskQueue
	jsonrpc   *JSONRPCServer
	logger    *zap.SugaredLogger
	mu        sync.Mutex
}

// NewXMLRPCServer creates an XML-RPC server that delegates method handling to
// the provided JSONRPCServer. Passing a nil jsonrpc server is treated as a
// programmer error; every method call will return an internal error.
func NewXMLRPCServer(taskQueue *task.TaskQueue, jsonrpcServer *JSONRPCServer) *XMLRPCServer {
	return &XMLRPCServer{
		taskQueue: taskQueue,
		jsonrpc:   jsonrpcServer,
		logger:    logger.Log.Named("xmlrpc"),
	}
}

// paramsToInterface converts the parsed XML-RPC params into the
// []interface{} shape expected by JSONRPCServer.handleMethod.
func (s *XMLRPCServer) paramsToInterface(req *XMLRPCRequest) []interface{} {
	params := make([]interface{}, 0, len(req.Params.Param))
	for i := range req.Params.Param {
		pv := &req.Params.Param[i].Value
		switch {
		case pv.String != "":
			params = append(params, pv.String)
		case len(pv.Array.Data.Value) > 0:
			arr := make([]interface{}, 0, len(pv.Array.Data.Value))
			for j := range pv.Array.Data.Value {
				av := &pv.Array.Data.Value[j]
				switch {
				case av.String != "":
					arr = append(arr, av.String)
				case av.Int != 0 || av.I4 != 0:
					arr = append(arr, int(av.Int+av.I4))
				default:
					arr = append(arr, "")
				}
			}
			params = append(params, arr)
		case pv.Int != 0 || pv.I4 != 0:
			params = append(params, int(pv.Int+pv.I4))
		default:
			params = append(params, "")
		}
	}
	return params
}

// interfaceToValue converts a Go value (as returned by JSONRPCServer) into an
// XMLRPCValue suitable for marshalling.
func interfaceToValue(val interface{}) XMLRPCValue {
	if val == nil {
		return XMLRPCValue{String: ""}
	}
	switch x := val.(type) {
	case string:
		return XMLRPCValue{String: x}
	case int:
		return XMLRPCValue{I4: int64(x)}
	case int64:
		return XMLRPCValue{I4: x}
	case float64:
		return XMLRPCValue{I4: int64(x)}
	case bool:
		if x {
			return XMLRPCValue{String: "true"}
		}
		return XMLRPCValue{String: "false"}
	case map[string]interface{}:
		st := &XMLRPCStruct{}
		for k, v := range x {
			st.Member = append(st.Member, XMLRPCMember{
				Name:  k,
				Value: interfaceToValue(v),
			})
		}
		return XMLRPCValue{Struct: st}
	case []interface{}:
		arr := &XMLRPCArray{}
		for _, item := range x {
			arr.Data.Value = append(arr.Data.Value, interfaceToValue(item))
		}
		return XMLRPCValue{Array: arr}
	case []string:
		arr := &XMLRPCArray{}
		for _, item := range x {
			arr.Data.Value = append(arr.Data.Value, XMLRPCValue{String: item})
		}
		return XMLRPCValue{Array: arr}
	case []map[string]interface{}:
		arr := &XMLRPCArray{}
		for _, item := range x {
			arr.Data.Value = append(arr.Data.Value, interfaceToValue(item))
		}
		return XMLRPCValue{Array: arr}
	default:
		return XMLRPCValue{String: fmt.Sprintf("%v", x)}
	}
}

func (s *XMLRPCServer) makeError(code int, message string) *XMLRPCResponse {
	resp := &XMLRPCResponse{
		Fault: &struct {
			Value XMLRPCValue `xml:"value"`
		}{
			Value: XMLRPCValue{
				Struct: &XMLRPCStruct{
					Member: []XMLRPCMember{
						{Name: "faultCode", Value: XMLRPCValue{Int: int64(code)}},
						{Name: "faultString", Value: XMLRPCValue{String: message}},
					},
				},
			},
		},
	}
	return resp
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

	if s.jsonrpc == nil {
		resp = s.makeError(-32500, "XML-RPC server is not wired to a JSON-RPC backend")
	} else {
		params := s.paramsToInterface(&req)
		result, callErr := s.jsonrpc.handleMethod(req.MethodName, params)
		if callErr != nil {
			code := -32500
			if e, ok := callErr.(*jsonRPCError); ok {
				code = e.code
				if e.err != nil {
					callErr = e.err
				}
			}
			resp = s.makeError(code, callErr.Error())
		} else {
			resp = &XMLRPCResponse{}
			resp.Params.Param.Value = interfaceToValue(result)
		}
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
