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

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// ReplaceHTTPMethod is the HTTP method for the the replace endpoint.
	ReplaceHTTPMethod = http.MethodPost

	replacePathName = "replace"
)

var (
	// M3DBReplaceURL is the url for the m3db replace handler (method POST).
	M3DBReplaceURL = path.Join(handler.RoutePrefixV1, M3DBServicePlacementPathName, replacePathName)

	// M3AggReplaceURL is the url for the m3aggregator replace handler (method
	// POST).
	M3AggReplaceURL = path.Join(handler.RoutePrefixV1, M3AggregatorServiceName, replacePathName)

	// M3CoordinatorReplaceURL is the url for the m3coordinator replace handler
	// (method POST).
	M3CoordinatorReplaceURL = path.Join(handler.RoutePrefixV1, M3CoordinatorServiceName, replacePathName)
)

// ReplaceHandler is the type for placement replaces.
type ReplaceHandler Handler

// NewReplaceHandler returns a new ReplaceHandler.
func NewReplaceHandler(opts HandlerOptions) *ReplaceHandler {
	return &ReplaceHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *ReplaceHandler) ServeHTTP(serviceName string, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, pErr := h.parseRequest(r)
	if pErr != nil {
		xhttp.Error(w, pErr.Inner(), pErr.Code())
		return
	}

	placement, err := h.Replace(serviceName, r, req)
	if err != nil {
		status := http.StatusInternalServerError
		if _, ok := err.(unsafeAddError); ok {
			status = http.StatusBadRequest
		}
		logger.Error("unable to replace instance", zap.Error(err))
		xhttp.Error(w, err, status)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
		Version:   int32(placement.GetVersion()),
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *ReplaceHandler) parseRequest(r *http.Request) (*admin.PlacementReplaceRequest, *xhttp.ParseError) {
	defer r.Body.Close()

	req := &admin.PlacementReplaceRequest{}
	if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return req, nil
}

// Replace replaces instances.
func (h *ReplaceHandler) Replace(
	serviceName string,
	httpReq *http.Request,
	req *admin.PlacementReplaceRequest,
) (placement.Placement, error) {
	candidates, err := ConvertInstancesProto(req.Candidates)
	if err != nil {
		return nil, err
	}

	serviceOpts := NewServiceOptions(serviceName, httpReq.Header, h.M3AggServiceOptions)
	service, algo, err := ServiceWithAlgo(h.ClusterClient, serviceOpts, h.nowFn())
	if err != nil {
		return nil, err
	}

	if req.Force {
		newPlacement, _, err := service.ReplaceInstances(req.LeavingInstanceIDs, candidates)
		return newPlacement, err
	}

	curPlacement, version, err := service.Placement()
	if err != nil {
		return nil, err
	}

	// M3Coordinator isn't sharded, can't check if its shards are available.
	if !isStateless(serviceName) {
		if err := validateAllAvailable(curPlacement); err != nil {
			return nil, err
		}
	}

	// We use the algorithm directly so that we can CheckAndSet on the placement
	// to make "atomic" forward progress.
	newPlacement, err := algo.ReplaceInstances(curPlacement, req.LeavingInstanceIDs, candidates)
	if err != nil {
		return nil, err
	}

	// Ensure the placement we're updating is still the one on which we validated
	// all shards are available.
	version, err = service.CheckAndSet(newPlacement, version)
	if err != nil {
		return nil, err
	}

	return newPlacement.SetVersion(version), nil
}
