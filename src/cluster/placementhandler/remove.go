// Copyright (c) 2021 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// RemoveHTTPMethod is the HTTP method used with this resource.
	RemoveHTTPMethod = http.MethodPost
)

var (
	// M3DBRemoveURL is the url for the placement Remove handler (with the POST method)
	// for the M3DB service.
	M3DBRemoveURL = path.Join(route.Prefix, M3DBServicePlacementPathName)

	// M3AggRemoveURL is the url for the placement Remove handler (with the POST method)
	// for the M3Agg service.
	M3AggRemoveURL = path.Join(route.Prefix, M3AggServicePlacementPathName)

	// M3CoordinatorRemoveURL is the url for the placement Remove handler (with the POST method)
	// for the M3Coordinator service.
	M3CoordinatorRemoveURL = path.Join(route.Prefix, M3CoordinatorServicePlacementPathName)
)

// RemoveHandler is the handler for placement removes.
type RemoveHandler Handler

// NewRemoveHandler returns a new instance of RemoveHandler.
func NewRemoveHandler(opts HandlerOptions) *RemoveHandler {
	return &RemoveHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *RemoveHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOptions)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	placement, err := h.Remove(svc, r, req)
	if err != nil {
		logger.Error("unable to Remove placement", zap.Error(err))
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

func (h *RemoveHandler) parseRequest(r *http.Request) (*admin.PlacementRemoveRequest, error) {
	defer r.Body.Close()

	removeReq := new(admin.PlacementRemoveRequest)
	if err := jsonpb.Unmarshal(r.Body, removeReq); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return removeReq, nil
}

// Remove removes an instance.
func (h *RemoveHandler) Remove(
	svc handleroptions.ServiceNameAndDefaults,
	httpReq *http.Request,
	req *admin.PlacementRemoveRequest,
) (placement.Placement, error) {
	serviceOpts := handleroptions.NewServiceOptions(svc, httpReq.Header,
		h.m3AggServiceOptions)
	var validateFn placement.ValidateFn
	if !req.Force {
		validateFn = validateAllAvailable
	}

	pcfg, err := Handler(*h).PlacementConfigCopy()
	if err != nil {
		return nil, err
	}
	service, _, err := ServiceWithAlgo(
		h.clusterClient,
		serviceOpts,
		pcfg.ApplyOverride(req.OptionOverride),
		h.nowFn(),
		validateFn,
	)
	if err != nil {
		return nil, err
	}
	newPlacement, err := service.RemoveInstances(req.InstanceIds)
	if err != nil {
		return nil, err
	}

	return newPlacement, nil
}
