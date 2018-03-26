package handler

import (
	"context"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/prompb"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/execution"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// PromWriteURL is the url for the prom write handler
	PromWriteURL = "/api/v1/prom/write"
)

// PromWriteHandler represents a handler for prometheus write endpoint.
type PromWriteHandler struct {
	store storage.Storage
}

// NewPromWriteHandler returns a new instance of handler.
func NewPromWriteHandler(store storage.Storage) http.Handler {
	return &PromWriteHandler{
		store: store,
	}
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}
	if err := h.write(r.Context(), req); err != nil {
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}
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

func (h *PromWriteHandler) write(ctx context.Context, r *prompb.WriteRequest) error {
	requests := make([]execution.Request, len(r.Timeseries))
	for idx, t := range r.Timeseries {
		requests[idx] = newLocalWriteRequest(storage.PromWriteTSToM3(t), h.store)
	}
	return execution.ExecuteParallel(ctx, requests)
}

func (w *localWriteRequest) Process(ctx context.Context) error {
	return w.store.Write(ctx, w.writeQuery)
}

type localWriteRequest struct {
	store      storage.Storage
	writeQuery *storage.WriteQuery
}

func newLocalWriteRequest(writeQuery *storage.WriteQuery, store storage.Storage) execution.Request {
	return &localWriteRequest{
		store:      store,
		writeQuery: writeQuery,
	}
}
