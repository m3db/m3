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
	"net/http"
	"path"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// SetHTTPMethod is the HTTP method for the the upsert endpoint.
	SetHTTPMethod = http.MethodPost

	setPathName = "set"
)

var (
	// M3DBSetURL is the url for the m3db replace handler (method POST).
	M3DBSetURL = path.Join(handler.RoutePrefixV1,
		M3DBServicePlacementPathName, setPathName)

	// M3AggSetURL is the url for the m3aggregator replace handler (method
	// POST).
	M3AggSetURL = path.Join(handler.RoutePrefixV1,
		M3AggServicePlacementPathName, setPathName)

	// M3CoordinatorSetURL is the url for the m3coordinator replace handler
	// (method POST).
	M3CoordinatorSetURL = path.Join(handler.RoutePrefixV1,
		M3CoordinatorServicePlacementPathName, setPathName)
)

// SetHandler is the type for placement replaces.
type SetHandler Handler

// NewSetHandler returns a new SetHandler.
func NewSetHandler(opts HandlerOptions) *SetHandler {
	return &SetHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *SetHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOptions)

	req, pErr := h.parseRequest(r)
	if pErr != nil {
		xhttp.Error(w, pErr.Inner(), pErr.Code())
		return
	}

	serviceOpts := handleroptions.NewServiceOptions(svc,
		r.Header, h.m3AggServiceOptions)
	service, _, err := ServiceWithAlgo(h.clusterClient,
		serviceOpts, h.nowFn(), nil)
	if err != nil {
		logger.Error("unable to create placement service", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	curPlacement, err := service.Placement()
	if err == kv.ErrNotFound {
		logger.Error("placement not found", zap.Any("req", req), zap.Error(err))
		xhttp.Error(w, err, http.StatusNotFound)
		return
	}
	if err != nil {
		logger.Error("unable to get current placement", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	var (
		placementProto   = req.Placement
		placementVersion int
	)
	newPlacement, err := placement.NewPlacementFromProto(req.Placement)
	if err != nil {
		logger.Error("unable to create new placement from proto", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	dryRun := !req.Confirm
	if dryRun {
		logger.Info("performing dry run for set placement, not confirmed")
		placementVersion = curPlacement.Version() + 1
	} else {
		logger.Info("performing live run for set placement, confirmed")
		// Ensure the placement we're updating is still the one on which we validated
		// all shards are available.
		updatedPlacement, err := service.CheckAndSet(newPlacement,
			curPlacement.Version())
		if err != nil {
			logger.Error("unable to update placement", zap.Error(err))
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}

		placementVersion = updatedPlacement.Version()
	}

	resp := &admin.PlacementSetResponse{
		Placement: placementProto,
		Version:   int32(placementVersion),
		DryRun:    dryRun,
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *SetHandler) parseRequest(r *http.Request) (*admin.PlacementSetRequest, *xhttp.ParseError) {
	defer r.Body.Close()

	req := &admin.PlacementSetRequest{}
	if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return req, nil
}
