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
	"strconv"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// GetHTTPMethod is the HTTP method used with this resource.
	GetHTTPMethod = http.MethodGet
)

var (
	// DeprecatedM3DBGetURL is the old url for the placement get handler, maintained for
	// backwards compatibility.
	DeprecatedM3DBGetURL = path.Join(handler.RoutePrefixV1, PlacementPathName)

	// M3DBGetURL is the url for the placement get handler (with the GET method)
	// for the M3DB service.
	M3DBGetURL = path.Join(handler.RoutePrefixV1, M3DBServicePlacementPathName)

	// M3AggGetURL is the url for the placement get handler (with the GET method)
	// for the M3Agg service.
	M3AggGetURL = path.Join(handler.RoutePrefixV1, M3AggServicePlacementPathName)

	// M3CoordinatorGetURL is the url for the placement get handler (with the GET method)
	// for the M3Coordinator service.
	M3CoordinatorGetURL = path.Join(handler.RoutePrefixV1, M3CoordinatorServicePlacementPathName)
)

// GetHandler is the handler for placement gets.
type GetHandler Handler

// NewGetHandler returns a new instance of GetHandler.
func NewGetHandler(opts HandlerOptions) *GetHandler {
	return &GetHandler{HandlerOptions: opts, nowFn: time.Now}
}

func (h *GetHandler) ServeHTTP(serviceName string, w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx)
		opts   = NewServiceOptions(
			serviceName, r.Header, h.M3AggServiceOptions)
	)

	service, err := Service(h.ClusterClient, opts, h.nowFn())
	if err != nil {
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	var placement placement.Placement
	var version int
	status := http.StatusNotFound
	if vs := r.FormValue("version"); vs != "" {
		version, err = strconv.Atoi(vs)
		if err == nil {
			placement, err = service.PlacementForVersion(version)
		} else {
			status = http.StatusBadRequest
		}
	} else {
		placement, version, err = service.Placement()
	}

	if err != nil {
		xhttp.Error(w, err, status)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
		Version:   int32(version),
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}
