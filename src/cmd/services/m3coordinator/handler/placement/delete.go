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
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	placementIDVar = "id"
)

var (
	// DeleteURL is the url for the placement delete handler (with the DELETE method).
	DeleteURL = fmt.Sprintf("/placement/{%s}", placementIDVar)

	errEmptyID = errors.New("must specify placement ID to delete")
)

type deleteHandler Handler

// NewDeleteHandler returns a new instance of a placement delete handler.
func NewDeleteHandler(client clusterclient.Client, cfg config.Configuration) http.Handler {
	return &deleteHandler{client: client, cfg: cfg}
}

func (h *deleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)
	id := mux.Vars(r)[placementIDVar]
	if id == "" {
		logger.Error("no placement ID provided to delete", zap.Any("error", errEmptyID))
		handler.Error(w, errEmptyID, http.StatusBadRequest)
		return
	}

	service, err := Service(h.client, h.cfg)
	if err != nil {
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	placement, err := service.RemoveInstances([]string{id})
	if err != nil {
		logger.Error("unable to delete placement", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
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
