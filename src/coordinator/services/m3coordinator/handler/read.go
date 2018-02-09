package handler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for prom read handler
	PromReadURL = "/api/v1/prom/read"
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
	store storage.Storage
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(store storage.Storage) http.Handler {
	return &PromReadHandler{store: store}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	result, err := h.read(r.Context(), req)
	if err != nil {
		logging.WithContext(r.Context()).Error("Read error", zap.Any("err", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &prompb.ReadResponse{
		Results: result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		logging.WithContext(r.Context()).Error("Read error", zap.Any("err", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
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

func (h *PromReadHandler) read(ctx context.Context, r *prompb.ReadRequest) ([]*prompb.QueryResult, error) {
	// TODO: Handle multi query use case
	if len(r.Queries) != 1 {
		return nil, fmt.Errorf("prometheus read endpoint currently only supports one query at a time")
	}

	promQuery := r.Queries[0]
	query, err := storage.PromReadQueryToM3(promQuery)
	if err != nil {
		return nil, err
	}

	result, err := h.store.Fetch(ctx, query)
	if err != nil {
		return nil, err
	}

	return storage.FetchResultToPromResults(result)
}
