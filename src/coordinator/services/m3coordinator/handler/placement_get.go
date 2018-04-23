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
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"

	"go.uber.org/zap"
)

const (
	// PlacementGetURL is the url for the placement get handler (with the GET method).
	PlacementGetURL = "/placement/get"

	// PlacementGetHTTPMethodURL is the url for the placement get handler (with the GET method).
	PlacementGetHTTPMethodURL = "/placement"
)

// PlacementGetHandler represents a handler for placement get endpoint.
type PlacementGetHandler AdminHandler

// NewPlacementGetHandler returns a new instance of handler.
func NewPlacementGetHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &PlacementGetHandler{
		clusterClient: clusterClient,
	}
}

func (h *PlacementGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	placement, version, err := h.placementGet(ctx)
	if err != nil {
		logger.Error("unable to get placement", zap.Any("error", err))
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
		Version:   int32(version),
	}

	WriteJSONResponse(w, resp, logger)
}

func (h *PlacementGetHandler) placementGet(ctx context.Context) (placement.Placement, int, error) {
	ps, err := PlacementService(h.clusterClient)
	if err != nil {
		return nil, 0, err
	}

	placement, version, err := ps.Placement()
	if err != nil {
		return nil, 0, err
	}

	return placement, version, nil
}
