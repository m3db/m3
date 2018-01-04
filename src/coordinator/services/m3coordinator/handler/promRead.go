package handler

import (
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler() http.Handler {
	return &PromReadHandler{}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	// TODO (nikunj): Actual read instead of logging
	logging.WithContext(r.Context()).Info("Read request", zap.Any("req", req))

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		Error(w, err, http.StatusInternalServerError)
		return
	}
}
func (h *PromReadHandler) parseRequest(r *http.Request) (*prompb.ReadRequest, *ParseError) {
	reqBuf, err := ParsePromRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}
