package handler

import (
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// PromWriteURL is the url for prom write handler
	PromWriteURL = "/api/v1/prom/write"
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler() http.Handler {
	return &PromWriteHandler{}
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := h.parseRequest(r)
	if err != nil {
		Error(w, err.Error(), err.Code())
		return
	}

	// TODO (nikunj): Actual write instead of logging
	logging.WithContext(r.Context()).Info("Write request", zap.Any("req", req))
}
func (h *PromWriteHandler) parseRequest(r *http.Request) (*prompb.WriteRequest, *ParseError) {
	reqBuf, err := ParsePromRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return &req, nil
}
