// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package placement

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
)

const (
	// DeprecatedM3DBDeleteAllURL is the old url for the handler to delete all placements, maintained
	// for backwards compatibility.
	DeprecatedM3DBDeleteAllURL = handler.RoutePrefixV1 + "/placement"

	// M3DBDeleteAllURL is the url for the handler to delete all placements (with the DELETE method)
	// for the M3DB service.
	M3DBDeleteAllURL = handler.RoutePrefixV1 + "/services/m3db/placement"

	// M3AggDeleteAllURL is the url for the handler to delete all placements (with the DELETE method)
	// for the M3Agg service.
	M3AggDeleteAllURL = handler.RoutePrefixV1 + "/services/m3agg/placement"

	// DeleteAllHTTPMethod is the HTTP method used with this resource.
	DeleteAllHTTPMethod = http.MethodDelete
)

// DeleteAllHandler is the handler to delete all placements.
type DeleteAllHandler Handler

// NewDeleteAllHandler returns a new instance of DeleteAllHandler.
func NewDeleteAllHandler(opts HandlerOptions) *DeleteAllHandler {
	return &DeleteAllHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *DeleteAllHandler) ServeHTTP(serviceName string, w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx)
		opts   = NewServiceOptions(
			serviceName, r.Header, h.M3AggServiceOptions)
	)

	service, err := Service(h.ClusterClient, opts, h.nowFn())
	if err != nil {
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	if err := service.Delete(); err != nil {
		logger.Error("unable to delete placement", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(struct {
		Deleted bool `json:"deleted"`
	}{
		Deleted: true,
	})
}
