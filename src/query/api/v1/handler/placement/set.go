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
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
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

// SetHandler is the type for manually setting a placement value. If none
// currently exists, this will set the initial placement. Otherwise it will
// override the existing placement.
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

	req, err := h.parseRequest(r)
	if err != nil {
		logger.Error("unable to parse request", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	serviceOpts := handleroptions.NewServiceOptions(svc,
		r.Header, h.m3AggServiceOptions)
	service, _, err := ServiceWithAlgo(h.clusterClient,
		serviceOpts, h.nowFn(), nil)
	if err != nil {
		logger.Error("unable to create placement service", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	var isNewPlacement bool
	curPlacement, err := service.Placement()
	if err != nil {
		if err != kv.ErrNotFound {
			logger.Error("unable to get current placement", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}

		isNewPlacement = true
		logger.Info("no placement found, creating new placement")
	}

	newPlacement, err := placement.NewPlacementFromProto(req.Placement)
	if err != nil {
		logger.Error("unable to create new placement from proto", zap.Error(err))
		xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
		return
	}

	if err := placement.Validate(newPlacement); err != nil {
		if !req.Force {
			logger.Error("unable to validate new placement", zap.Error(err))
			xhttp.WriteError(w,
				xerrors.NewRenamedError(err, fmt.Errorf("unable to validate new placement: %w", err)))
			return
		}
		logger.Warn("unable to validate new placement, continuing with force", zap.Error(err))
	}

	var (
		placementProto = req.Placement
		dryRun         = !req.Confirm

		updatedPlacement placement.Placement
		placementVersion int
	)

	if dryRun {
		logger.Info("performing dry run for set placement, not confirmed")
		if isNewPlacement {
			placementVersion = 0
		} else {
			placementVersion = curPlacement.Version() + 1
		}
	} else {
		logger.Info("performing live run for set placement, confirmed")

		if isNewPlacement {
			updatedPlacement, err = service.SetIfNotExist(newPlacement)
		} else {
			// Ensure the placement we're updating is still the one on which we validated
			// all shards are available.
			updatedPlacement, err = service.CheckAndSet(newPlacement,
				curPlacement.Version())
		}

		if err != nil {
			logger.Error("unable to update placement", zap.Error(err), zap.Bool("isNewPlacement", isNewPlacement))
			xhttp.WriteError(w, err)
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

func (h *SetHandler) parseRequest(r *http.Request) (*admin.PlacementSetRequest, error) {
	defer r.Body.Close()

	req := &admin.PlacementSetRequest{}
	if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return req, nil
}
