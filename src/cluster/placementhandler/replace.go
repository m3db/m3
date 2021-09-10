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
	"net/http"
	"path"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	// ReplaceHTTPMethod is the HTTP method for the the replace endpoint.
	ReplaceHTTPMethod = http.MethodPost

	replacePathName = "replace"
)

var (
	// M3DBReplaceURL is the url for the m3db replace handler (method POST).
	M3DBReplaceURL = path.Join(route.Prefix,
		M3DBServicePlacementPathName, replacePathName)

	// M3AggReplaceURL is the url for the m3aggregator replace handler (method
	// POST).
	M3AggReplaceURL = path.Join(route.Prefix,
		M3AggServicePlacementPathName, replacePathName)

	// M3CoordinatorReplaceURL is the url for the m3coordinator replace handler
	// (method POST).
	M3CoordinatorReplaceURL = path.Join(route.Prefix,
		M3CoordinatorServicePlacementPathName, replacePathName)
)

// ReplaceHandler is the type for placement replaces.
type ReplaceHandler Handler

// NewReplaceHandler returns a new ReplaceHandler.
func NewReplaceHandler(opts HandlerOptions) *ReplaceHandler {
	return &ReplaceHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *ReplaceHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOptions)

	req, pErr := h.parseRequest(r)
	if pErr != nil {
		xhttp.WriteError(w, pErr)
		return
	}

	placement, err := h.Replace(svc, r, req)
	if err != nil {
		logger.Error("unable to replace instance", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
		Version:   int32(placement.Version()),
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *ReplaceHandler) parseRequest(r *http.Request) (*admin.PlacementReplaceRequest, error) {
	defer r.Body.Close()

	req := &admin.PlacementReplaceRequest{}
	if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return req, nil
}

// Replace replaces instances.
func (h *ReplaceHandler) Replace(
	svc handleroptions.ServiceNameAndDefaults,
	httpReq *http.Request,
	req *admin.PlacementReplaceRequest,
) (placement.Placement, error) {
	candidates, err := ConvertInstancesProto(req.Candidates)
	if err != nil {
		return nil, err
	}

	serviceOpts := handleroptions.NewServiceOptions(svc,
		httpReq.Header, h.m3AggServiceOptions)

	pcfg, err := Handler(*h).PlacementConfigCopy()
	if err != nil {
		return nil, err
	}
	service, algo, err := ServiceWithAlgo(h.clusterClient,
		serviceOpts, pcfg.ApplyOverride(req.OptionOverride), h.nowFn(), nil)
	if err != nil {
		return nil, err
	}

	if req.Force {
		newPlacement, _, err := service.ReplaceInstances(req.LeavingInstanceIDs, candidates)
		return newPlacement, err
	}

	curPlacement, err := service.Placement()
	if err != nil {
		return nil, err
	}

	// M3Coordinator isn't sharded, can't check if its shards are available.
	if !isStateless(svc.ServiceName) {
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
	return service.CheckAndSet(newPlacement, curPlacement.Version())
}
