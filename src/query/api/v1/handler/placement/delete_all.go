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
	"path"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// DeleteAllHTTPMethod is the HTTP method used with this resource.
	DeleteAllHTTPMethod = http.MethodDelete
)

var (
	// M3DBDeleteAllURL is the url for the handler to delete all placements (with the DELETE method)
	// for the M3DB service.
	M3DBDeleteAllURL = path.Join(handler.RoutePrefixV1, M3DBServicePlacementPathName)

	// M3AggDeleteAllURL is the url for the handler to delete all placements (with the DELETE method)
	// for the M3Agg service.
	M3AggDeleteAllURL = path.Join(handler.RoutePrefixV1, M3AggServicePlacementPathName)

	// M3CoordinatorDeleteAllURL is the url for the handler to delete all placements (with the DELETE method)
	// for the M3Coordinator service.
	M3CoordinatorDeleteAllURL = path.Join(handler.RoutePrefixV1, M3CoordinatorServicePlacementPathName)
)

// DeleteAllHandler is the handler to delete all placements.
type DeleteAllHandler Handler

// NewDeleteAllHandler returns a new instance of DeleteAllHandler.
func NewDeleteAllHandler(opts HandlerOptions) *DeleteAllHandler {
	return &DeleteAllHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *DeleteAllHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx, h.instrumentOptions)
		opts   = handleroptions.NewServiceOptions(svc, r.Header, h.m3AggServiceOptions)
	)

	service, err := Service(h.clusterClient, opts,
		h.config.ClusterManagement.Placement, h.nowFn(), nil)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	curPlacement, err := service.Placement()
	if err != nil {
		if err == kv.ErrNotFound {
			logger.Info("cannot delete placement",
				zap.String("service", svc.ServiceName),
				zap.Error(err))
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusNotFound))
			return
		}

		logger.Error("unable to fetch placement", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	if err := service.Delete(); err != nil {
		logger.Error("error deleting placement", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	// Now need to delete aggregator related keys (e.g. for shardsets) if required.
	if svc.ServiceName == handleroptions.M3AggregatorServiceName {
		instances := curPlacement.Instances()
		shardSetIDs := make([]uint32, 0, len(instances))
		for _, elem := range instances {
			value := elem.ShardSetID()
			found := false
			for _, existing := range shardSetIDs {
				if existing == value {
					found = true
					break
				}
			}
			if !found {
				shardSetIDs = append(shardSetIDs, value)
			}
		}

		err := deleteAggregatorShardSetIDRelatedKeys(svc, opts,
			h.clusterClient, shardSetIDs)
		if err != nil {
			logger.Error("error removing aggregator keys for instances",
				zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
	}

	json.NewEncoder(w).Encode(struct {
		Deleted bool `json:"deleted"`
	}{
		Deleted: true,
	})
}
