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
	"errors"
	"fmt"
	"net/http"

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	placementIDVar = "id"

	// DeleteHTTPMethod is the HTTP method used with this resource.
	DeleteHTTPMethod = http.MethodDelete
)

var (
	// DeleteURL is the url for the placement delete handler.
	DeleteURL = fmt.Sprintf("%s/placement/{%s}", handler.RoutePrefixV1, placementIDVar)

	errEmptyID = errors.New("must specify placement ID to delete")
)

// DeleteHandler is the handler for placement deletes.
type DeleteHandler Handler

// NewDeleteHandler returns a new instance of DeleteHandler.
func NewDeleteHandler(client clusterclient.Client, cfg config.Configuration) *DeleteHandler {
	return &DeleteHandler{client: client, cfg: cfg}
}

func (h *DeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)
	id := mux.Vars(r)[placementIDVar]
	if id == "" {
		logger.Error("no placement ID provided to delete", zap.Any("error", errEmptyID))
		handler.Error(w, errEmptyID, http.StatusBadRequest)
		return
	}

	service, err := Service(h.client, r.Header)
	if err != nil {
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	placement, err := service.RemoveInstances([]string{id})
	if err != nil {
		logger.Error("unable to delete placement", zap.Any("error", err))
		handler.Error(w, err, http.StatusNotFound)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
	}

	handler.WriteProtoMsgJSONResponse(w, resp, logger)
}
