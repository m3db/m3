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

package placementhandler

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	placementIDVar    = "id"
	placementForceVar = "force"

	// DeleteHTTPMethod is the HTTP method used with this resource.
	DeleteHTTPMethod = http.MethodDelete
)

var (
	placementIDPath = fmt.Sprintf("{%s}", placementIDVar)

	// M3DBDeleteURL is the url for the placement delete handler for the M3DB service.
	M3DBDeleteURL = path.Join(route.Prefix, M3DBServicePlacementPathName, placementIDPath)

	// M3AggDeleteURL is the url for the placement delete handler for the M3Agg service.
	M3AggDeleteURL = path.Join(route.Prefix, M3AggServicePlacementPathName, placementIDPath)

	// M3CoordinatorDeleteURL is the url for the placement delete handler for the M3Coordinator service.
	M3CoordinatorDeleteURL = path.Join(route.Prefix, M3CoordinatorServicePlacementPathName, placementIDPath)

	errEmptyID = xerrors.NewInvalidParamsError(errors.New("must specify placement ID to delete"))
)

// DeleteHandler is the handler for placement deletes.
type DeleteHandler Handler

// NewDeleteHandler returns a new instance of DeleteHandler.
func NewDeleteHandler(opts HandlerOptions) *DeleteHandler {
	return &DeleteHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *DeleteHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx, h.instrumentOptions)
		id     = mux.Vars(r)[placementIDVar]
	)
	if id == "" {
		logger.Error("no placement ID provided to delete", zap.Error(errEmptyID))
		xhttp.WriteError(w, errEmptyID)
		return
	}

	var (
		force = r.FormValue(placementForceVar) == "true"
		opts  = handleroptions.NewServiceOptions(svc, r.Header, h.m3AggServiceOptions)
	)

	service, algo, err := ServiceWithAlgo(
		h.clusterClient,
		opts,
		Handler(*h).PlacementConfig(),
		h.nowFn(),
		nil,
	)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	curPlacement, err := service.Placement()
	if err != nil {
		logger.Error("unable to fetch placement", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	instance, ok := curPlacement.Instance(id)
	if !ok {
		err = fmt.Errorf("instance not found: %s", id)
		xhttp.WriteError(w, xhttp.NewError(err, http.StatusNotFound))
		return
	}

	toRemove := []string{id}

	// There are no unsafe placement changes because M3Coordinator is stateless
	if isStateless(svc.ServiceName) {
		force = true
	}

	var newPlacement placement.Placement
	if force {
		newPlacement, err = service.RemoveInstances(toRemove)
		if err != nil {
			logger.Error("unable to delete instances", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
	} else {
		if err := validateAllAvailable(curPlacement); err != nil {
			logger.Warn("unable to remove instance, some shards not available", zap.Error(err), zap.String("instance", id))
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
			return
		}

		_, ok := curPlacement.Instance(id)
		if !ok {
			logger.Error("instance not found in placement", zap.String("instance", id))
			err := fmt.Errorf("instance %s not found in placement", id)
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusNotFound))
			return
		}

		newPlacement, err = algo.RemoveInstances(curPlacement, toRemove)
		if err != nil {
			logger.Error("unable to generate placement with instances removed", zap.String("instance", id), zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}

		newPlacement, err = service.CheckAndSet(newPlacement, curPlacement.Version())
		if err != nil {
			logger.Error("unable to remove instance from placement", zap.String("instance", id), zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
	}

	// Now need to delete aggregator related keys (e.g. for shardsets) if required.
	if svc.ServiceName == handleroptions.M3AggregatorServiceName {
		shardSetID := instance.ShardSetID()
		anyExistingShardSetIDs := false
		for _, elem := range curPlacement.Instances() {
			if elem.ID() == instance.ID() {
				continue
			}
			if elem.ShardSetID() == shardSetID {
				anyExistingShardSetIDs = true
				break
			}
		}

		// Only delete the related shardset keys if no other shardsets left.
		if !anyExistingShardSetIDs {
			err := deleteAggregatorShardSetIDRelatedKeys(svc, opts,
				h.clusterClient, []uint32{shardSetID})
			if err != nil {
				logger.Error("error removing aggregator keys for instances",
					zap.Error(err))
				xhttp.WriteError(w, err)
				return
			}
		}
	}

	placementProto, err := newPlacement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
		Version:   int32(newPlacement.Version()),
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}
