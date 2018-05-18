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
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/m3db/m3cluster/placement"

	"go.uber.org/zap"
)

const (
	// AddURL is the url for the placement add handler (with the POST method).
	AddURL = "/placement/add"
)

type addHandler Handler

// NewAddHandler returns a new instance of a placement add handler.
func NewAddHandler(service placement.Service) http.Handler {
	return &addHandler{service: service}
}

func (h *addHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	placement, err := h.add(req)
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

func (h *addHandler) parseRequest(r *http.Request) (*admin.PlacementAddRequest, *handler.ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	defer r.Body.Close()

	addReq := new(admin.PlacementAddRequest)
	if err := json.Unmarshal(body, addReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return addReq, nil
}

func (h *addHandler) add(r *admin.PlacementAddRequest) (placement.Placement, error) {
	instances, err := ConvertInstancesProto(r.Instances)
	if err != nil {
		return nil, err
	}

	newPlacement, _, err := h.service.AddInstances(instances)
	if err != nil {
		return nil, err
	}

	return newPlacement, nil
}
