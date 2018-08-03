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

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3db/src/cmd/services/m3query/config"
	"github.com/m3db/m3db/src/query/api/v1/handler"
	"github.com/m3db/m3db/src/query/generated/proto/admin"
	"github.com/m3db/m3db/src/query/util/logging"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// AddURL is the url for the placement add handler (with the POST method).
	AddURL = handler.RoutePrefixV1 + "/placement"

	// AddHTTPMethod is the HTTP method used with this resource.
	AddHTTPMethod = http.MethodPost
)

// AddHandler is the handler for placement adds.
type AddHandler Handler

// NewAddHandler returns a new instance of AddHandler.
func NewAddHandler(client clusterclient.Client, cfg config.Configuration) *AddHandler {
	return &AddHandler{client: client, cfg: cfg}
}

func (h *AddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		handler.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	placement, err := h.Add(r, req)
	if err != nil {
		logger.Error("unable to add placement", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
	}

	handler.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *AddHandler) parseRequest(r *http.Request) (*admin.PlacementAddRequest, *handler.ParseError) {
	defer r.Body.Close()
	addReq := new(admin.PlacementAddRequest)
	if err := jsonpb.Unmarshal(r.Body, addReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return addReq, nil
}

// Add adds a placement.
func (h *AddHandler) Add(
	httpReq *http.Request,
	req *admin.PlacementAddRequest,
) (placement.Placement, error) {
	instances, err := ConvertInstancesProto(req.Instances)
	if err != nil {
		return nil, err
	}

	service, err := Service(h.client, httpReq.Header)
	if err != nil {
		return nil, err
	}

	newPlacement, _, err := service.AddInstances(instances)
	if err != nil {
		return nil, err
	}

	return newPlacement, nil
}
