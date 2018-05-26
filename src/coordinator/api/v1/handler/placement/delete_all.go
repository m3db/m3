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

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"go.uber.org/zap"
)

const (
	// DeleteAllURL is the url for the handler to delete all placements (with the DELETE method).
	DeleteAllURL = handler.RoutePrefixV1 + "/placement"

	// DeleteAllHTTPMethod is the HTTP method used with this resource.
	DeleteAllHTTPMethod = "DELETE"
)

// DeleteAllHandler is the handler to delete all placements.
type DeleteAllHandler Handler

// NewDeleteAllHandler returns a new instance of DeleteAllHandler.
func NewDeleteAllHandler(client clusterclient.Client, cfg config.Configuration) *DeleteAllHandler {
	return &DeleteAllHandler{client: client, cfg: cfg}
}

func (h *DeleteAllHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	service, err := Service(h.client, h.cfg)
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
