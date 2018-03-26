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
