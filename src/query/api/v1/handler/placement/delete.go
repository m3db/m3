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
	"path"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
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

	// DeprecatedM3DBDeleteURL is the old url for the placement delete handler, maintained
	// for backwards compatibility.
	DeprecatedM3DBDeleteURL = path.Join(handler.RoutePrefixV1, PlacementPathName, placementIDPath)

	// M3DBDeleteURL is the url for the placement delete handler for the M3DB service.
	M3DBDeleteURL = path.Join(handler.RoutePrefixV1, M3DBServicePlacementPathName, placementIDPath)

	// M3AggDeleteURL is the url for the placement delete handler for the M3Agg service.
	M3AggDeleteURL = path.Join(handler.RoutePrefixV1, M3AggServicePlacementPathName, placementIDPath)

	// M3CoordinatorDeleteURL is the url for the placement delete handler for the M3Coordinator service.
	M3CoordinatorDeleteURL = path.Join(handler.RoutePrefixV1, M3CoordinatorServicePlacementPathName, placementIDPath)

	errEmptyID = errors.New("must specify placement ID to delete")
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
		xhttp.Error(w, errEmptyID, http.StatusBadRequest)
		return
	}

	var (
		force = r.FormValue(placementForceVar) == "true"
		opts  = handleroptions.NewServiceOptions(svc, r.Header, h.m3AggServiceOptions)
	)

	service, algo, err := ServiceWithAlgo(h.clusterClient, opts, h.nowFn(), nil)
	if err != nil {
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	curPlacement, err := service.Placement()
	if err != nil {
		logger.Error("unable to fetch placement", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	instance, ok := curPlacement.Instance(id)
	if !ok {
		err = fmt.Errorf("instance not found: %s", id)
		xhttp.Error(w, err, http.StatusNotFound)
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
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}
	} else {
		if err := validateAllAvailable(curPlacement); err != nil {
			logger.Warn("unable to remove instance, some shards not available", zap.Error(err), zap.String("instance", id))
			xhttp.Error(w, err, http.StatusBadRequest)
			return
		}

		_, ok := curPlacement.Instance(id)
		if !ok {
			logger.Error("instance not found in placement", zap.String("instance", id))
			err := fmt.Errorf("instance %s not found in placement", id)
			xhttp.Error(w, err, http.StatusNotFound)
			return
		}

		newPlacement, err = algo.RemoveInstances(curPlacement, toRemove)
		if err != nil {
			logger.Error("unable to generate placement with instances removed", zap.String("instance", id), zap.Error(err))
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}

		newPlacement, err = service.CheckAndSet(newPlacement, curPlacement.Version())
		if err != nil {
			logger.Error("unable to remove instance from placement", zap.String("instance", id), zap.Error(err))
			xhttp.Error(w, err, http.StatusInternalServerError)
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
				xhttp.Error(w, err, http.StatusInternalServerError)
				return
			}
		}
	}

	placementProto, err := newPlacement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
		Version:   int32(newPlacement.Version()),
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}
