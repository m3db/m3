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
	initPathName = "init"
)

var (
	// DeprecatedM3DBInitURL is the old url for the placement init handler, maintained for backwards
	// compatibility. (with the POST method).
	DeprecatedM3DBInitURL = path.Join(handler.RoutePrefixV1, PlacementPathName, initPathName)

	// M3DBInitURL is the url for the placement init handler, (with the POST method).
	M3DBInitURL = path.Join(handler.RoutePrefixV1, M3DBServicePlacementPathName, initPathName)

	// M3AggInitURL is the url for the m3agg placement init handler (with the POST method).
	M3AggInitURL = path.Join(handler.RoutePrefixV1, M3AggServicePlacementPathName, initPathName)

	// M3CoordinatorInitURL is the url for the m3agg placement init handler (with the POST method).
	M3CoordinatorInitURL = path.Join(handler.RoutePrefixV1, M3CoordinatorServicePlacementPathName, initPathName)

	// InitHTTPMethod is the HTTP method used with this resource.
	InitHTTPMethod = http.MethodPost
)

// InitHandler is the handler for placement inits.
type InitHandler Handler

// NewInitHandler returns a new instance of InitHandler.
func NewInitHandler(opts HandlerOptions) *InitHandler {
	return &InitHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *InitHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOptions)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	placement, err := h.Init(svc, r, req)
	if err != nil {
		if err == kv.ErrAlreadyExists {
			logger.Error("placement already exists", zap.Error(err))
			xhttp.Error(w, err, http.StatusConflict)
			return
		}
		logger.Error("unable to initialize placement", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
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
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *InitHandler) parseRequest(r *http.Request) (*admin.PlacementInitRequest, *xhttp.ParseError) {
	defer r.Body.Close()
	initReq := new(admin.PlacementInitRequest)
	if err := jsonpb.Unmarshal(r.Body, initReq); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return initReq, nil
}

// Init initializes a placement.
func (h *InitHandler) Init(
	svc handleroptions.ServiceNameAndDefaults,
	httpReq *http.Request,
	req *admin.PlacementInitRequest,
) (placement.Placement, error) {
	instances, err := ConvertInstancesProto(req.Instances)
	if err != nil {
		return nil, err
	}

	serviceOpts := handleroptions.NewServiceOptions(svc, httpReq.Header,
		h.m3AggServiceOptions)
	service, err := Service(h.clusterClient, serviceOpts, h.nowFn(), nil)
	if err != nil {
		return nil, err
	}

	replicationFactor := int(req.ReplicationFactor)
	switch svc.ServiceName {
	case handleroptions.M3CoordinatorServiceName:
		// M3Coordinator placements are stateless
		replicationFactor = 1
	}

	placement, err := service.BuildInitialPlacement(instances,
		int(req.NumShards), replicationFactor)
	if err != nil {
		return nil, err
	}

	return placement, nil
}
