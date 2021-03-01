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
	"strconv"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// GetHTTPMethod is the HTTP method used with this resource.
	GetHTTPMethod = http.MethodGet
)

var (
	// M3DBGetURL is the url for the placement get handler (with the GET method)
	// for the M3DB service.
	M3DBGetURL = path.Join(handler.RoutePrefixV1, M3DBServicePlacementPathName)

	// M3AggGetURL is the url for the placement get handler (with the GET method)
	// for the M3Agg service.
	M3AggGetURL = path.Join(handler.RoutePrefixV1, M3AggServicePlacementPathName)

	// M3CoordinatorGetURL is the url for the placement get handler (with the GET method)
	// for the M3Coordinator service.
	M3CoordinatorGetURL = path.Join(handler.RoutePrefixV1, M3CoordinatorServicePlacementPathName)

	errPlacementDoesNotExist = xhttp.NewError(errors.New("placement does not exist"), http.StatusNotFound)
)

// GetHandler is the handler for placement gets.
type GetHandler Handler

// NewGetHandler returns a new instance of GetHandler.
func NewGetHandler(opts HandlerOptions) *GetHandler {
	return &GetHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *GetHandler) ServeHTTP(
	service handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx, h.instrumentOptions)
	)

	placement, err := h.Get(service, r)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}
	if placement == nil {
		xhttp.WriteError(w, errPlacementDoesNotExist)
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

// Get gets a placement.
func (h *GetHandler) Get(
	svc handleroptions.ServiceNameAndDefaults,
	httpReq *http.Request,
) (placement placement.Placement, err error) {
	var headers http.Header
	if httpReq != nil {
		headers = httpReq.Header
	}

	opts := handleroptions.NewServiceOptions(svc, headers, h.m3AggServiceOptions)
	service, err := Service(h.clusterClient, opts,
		h.config.ClusterManagement.Placement, h.nowFn(), nil)
	if err != nil {
		return nil, err
	}

	if httpReq != nil && httpReq.FormValue("version") != "" {
		version, err := strconv.Atoi(httpReq.FormValue("version"))
		if err != nil {
			return nil, xerrors.NewInvalidParamsError(fmt.Errorf("could not parse version: %v", err))
		}

		placement, err = service.PlacementForVersion(version)
		if err == kv.ErrNotFound {
			// TODO(rartoul): This should probably be handled at the service
			// level but that would be a large refactor.
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		return placement, nil
	}

	placement, err = service.Placement()
	if err == kv.ErrNotFound {
		// TODO(rartoul): This should probably be handled at the service
		// level but that would be a large refactor.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return placement, nil
}
