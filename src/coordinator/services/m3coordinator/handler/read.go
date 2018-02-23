package handler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3coordinator/executor"
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
	engine *executor.Engine
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(engine *executor.Engine) http.Handler {
	return &PromReadHandler{engine: engine}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params, err := ParseRequestParams(r)
	logger := logging.WithContext(r.Context())
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return
	}

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	result, err := h.read(r.Context(), w, req, params)
	if err != nil {
		logger.Error("unable to fetch data", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &prompb.ReadResponse{
		Results: result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("unable to marshal read results to protobuf", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logger.Error("unable to encode read results to snappy", zap.Any("err", err))
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

func (h *PromReadHandler) read(reqCtx context.Context, w http.ResponseWriter, r *prompb.ReadRequest, params *RequestParams) ([]*prompb.QueryResult, error) {
	// TODO: Handle multi query use case
	if len(r.Queries) != 1 {
		return nil, fmt.Errorf("prometheus read endpoint currently only supports one query at a time")
	}

	ctx, cancel := context.WithTimeout(reqCtx, params.Timeout)
	defer cancel()
	promQuery := r.Queries[0]
	query, err := storage.PromReadQueryToM3(promQuery)
	if err != nil {
		return nil, err
	}

	// Results is closed by execute
	results := make(chan *storage.QueryResult)

	opts := &executor.EngineOptions{}
	// Detect clients closing connections
	abortCh, closingCh := CloseWatcher(ctx, w)
	opts.AbortCh = abortCh

	go h.engine.Execute(ctx, query, opts, closingCh, results)

	promResults := make([]*prompb.QueryResult, 0, 1)
	for result := range results {
		if result.Err != nil {
			return nil, result.Err
		}

		promRes := storage.FetchResultToPromResult(result.FetchResult)
		promResults = append(promResults, promRes)
	}

	return promResults, nil
}

// CloseWatcher watches for CloseNotify and context timeout. It is best effort and may sometimes not close the channel relying on gc
func CloseWatcher(ctx context.Context, w http.ResponseWriter) (<-chan bool, <-chan bool) {
	closing := make(chan bool)
	logger := logging.WithContext(ctx)
	var doneChan <-chan bool
	if notifier, ok := w.(http.CloseNotifier); ok {
		done := make(chan bool)

		notify := notifier.CloseNotify()
		go func() {
			// Wait for either the request to finish
			// or for the client to disconnect
			select {
			case <-done:
			case <-notify:
				logger.Warn("connection closed by client")
				close(closing)
			case <-ctx.Done():
				// We only care about the time out case and not other cancellations
				if ctx.Err() == context.DeadlineExceeded {
					logger.Warn("request timed out")
				}
				close(closing)
			}
			close(done)
		}()
		doneChan = done
	}

	return doneChan, closing
}
