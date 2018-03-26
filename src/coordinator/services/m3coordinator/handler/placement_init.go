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
