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

package handler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"

	"go.uber.org/zap"
)

const (
	// PlacementInitURL is the url for the placement init handler (with the POST method).
	PlacementInitURL = "/placement/init"
)

// PlacementInitHandler represents a handler for placement init endpoint.
type PlacementInitHandler AdminHandler

// NewPlacementInitHandler returns a new instance of handler.
func NewPlacementInitHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &PlacementInitHandler{
		clusterClient: clusterClient,
	}
}

func (h *PlacementInitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	placement, err := h.placementInit(ctx, req)
	if err != nil {
		logger.Error("unable to initialize placement", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := placement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
	}

	WriteJSONResponse(w, resp, logger)
}

func (h *PlacementInitHandler) parseRequest(r *http.Request) (*admin.PlacementInitRequest, *ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	initReq := new(admin.PlacementInitRequest)
	if err := json.Unmarshal(body, initReq); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return initReq, nil
}

func (h *PlacementInitHandler) placementInit(ctx context.Context, r *admin.PlacementInitRequest) (placement.Placement, error) {
	ps, err := PlacementService(h.clusterClient)
	if err != nil {
		return nil, err
	}

	instances, err := ConvertInstancesProto(r.Instances)
	if err != nil {
		return nil, err
	}

	placement, err := ps.BuildInitialPlacement(instances, int(r.NumShards), int(r.ReplicationFactor))
	if err != nil {
		return nil, err
	}

	return placement, nil
}
