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

package database

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	clusterplacement "github.com/m3db/m3/src/cluster/placement"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	dbnamespace "github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/placement"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// CreateURL is the URL for the database create handler.
	CreateURL = handler.RoutePrefixV1 + "/database/create"

	// CreateNamespaceURL is the URL for the database namespace create handler.
	CreateNamespaceURL = handler.RoutePrefixV1 + "/database/namespace/create"

	// CreateHTTPMethod is the HTTP method used with the create database resource.
	CreateHTTPMethod = http.MethodPost

	// CreateNamespaceHTTPMethod is the HTTP method used with the create database namespace resource.
	CreateNamespaceHTTPMethod = http.MethodPost

	// DefaultLocalHostID is the default local host ID when creating a database.
	DefaultLocalHostID = "m3db_local"

	// DefaultLocalIsolationGroup is the default isolation group when creating a
	// local database.
	DefaultLocalIsolationGroup = "local"

	// DefaultLocalZone is the default zone when creating a local database.
	DefaultLocalZone = "embedded"

	idealDatapointsPerBlock           = 720
	blockSizeFromExpectedSeriesScalar = idealDatapointsPerBlock * int64(time.Hour)
	shardMultiplier                   = 64

	dbTypeLocal   dbType = "local"
	dbTypeCluster dbType = "cluster"

	defaultIsolationGroup = "local"
	defaultZone           = "local"

	defaultLocalRetentionPeriod = 24 * time.Hour

	minRecommendCalculateBlockSize = 30 * time.Minute
	maxRecommendCalculateBlockSize = 24 * time.Hour
)

type recommendedBlockSize struct {
	forRetentionLessThanOrEqual time.Duration
	blockSize                   time.Duration
}

var recommendedBlockSizesByRetentionAsc = []recommendedBlockSize{
	{
		forRetentionLessThanOrEqual: 12 * time.Hour,
		blockSize:                   30 * time.Minute,
	},
	{
		forRetentionLessThanOrEqual: 24 * time.Hour,
		blockSize:                   time.Hour,
	},
	{
		forRetentionLessThanOrEqual: 7 * 24 * time.Hour,
		blockSize:                   2 * time.Hour,
	},
	{
		forRetentionLessThanOrEqual: 30 * 24 * time.Hour,
		blockSize:                   12 * time.Hour,
	},
	{
		forRetentionLessThanOrEqual: 365 * 24 * time.Hour,
		blockSize:                   24 * time.Hour,
	},
}

var (
	errMissingRequiredField    = errors.New("missing required field")
	errInvalidDBType           = errors.New("invalid database type")
	errMissingEmbeddedDBPort   = errors.New("unable to get port from embedded database listen address")
	errMissingEmbeddedDBConfig = errors.New("unable to find local embedded database config")
	errMissingHostID           = errors.New("missing host ID")

	errClusteredPlacementAlreadyExists        = errors.New("cannot use database create API to modify clustered placements after they are instantiated. Use the placement APIs directly to make placement changes, or remove the list of hosts from the request to add a namespace without modifying the placement")
	errCantReplaceLocalPlacementWithClustered = errors.New("cannot replace existing local placement with a clustered placement. Use the placement APIs directly to make placement changes, or remove the `type` field from the  request to add a namespace without modifying the existing local placement")
)

type dbType string

type createHandler struct {
	placementInitHandler   *placement.InitHandler
	placementGetHandler    *placement.GetHandler
	namespaceAddHandler    *namespace.AddHandler
	namespaceGetHandler    *namespace.GetHandler
	namespaceDeleteHandler *namespace.DeleteHandler
	embeddedDbCfg          *dbconfig.DBConfiguration
}

// NewCreateHandler returns a new instance of a database create handler.
func NewCreateHandler(
	client clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
) http.Handler {
	placementHandlerOptions := placement.HandlerOptions{
		ClusterClient: client,
		Config:        cfg,
	}
	return &createHandler{
		placementInitHandler:   placement.NewInitHandler(placementHandlerOptions),
		placementGetHandler:    placement.NewGetHandler(placementHandlerOptions),
		namespaceAddHandler:    namespace.NewAddHandler(client),
		namespaceGetHandler:    namespace.NewGetHandler(client),
		namespaceDeleteHandler: namespace.NewDeleteHandler(client),
		embeddedDbCfg:          embeddedDbCfg,
	}
}

func (h *createHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx)
	)

	currPlacement, _, err := h.placementGetHandler.Get(placement.M3DBServiceName, nil)
	if err != nil {
		logger.Error("unable to get placement", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	parsedReq, namespaceRequest, placementRequest, rErr := h.parseAndValidateRequest(r, currPlacement)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	currPlacement, badRequest, err := h.maybeInitPlacement(currPlacement, parsedReq, placementRequest, r)
	if err != nil {
		logger.Error("unable to initialize placement", zap.Error(err))
		status := http.StatusBadRequest
		if !badRequest {
			status = http.StatusInternalServerError
		}
		xhttp.Error(w, err, status)
		return
	}

	nsRegistry, err := h.namespaceGetHandler.Get()
	if err != nil {
		logger.Error("unable to retrieve existing namespaces", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	// TODO(rartoul): Add test for NS exists.
	_, nsExists := nsRegistry.Namespaces[namespaceRequest.Name]
	if nsExists {
		err := fmt.Errorf(
			"unable to create namespace: %s because it already exists",
			namespaceRequest.Name)
		logger.Error("unable to create namespace", zap.Error(err))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	nsRegistry, err = h.namespaceAddHandler.Add(namespaceRequest)
	if err != nil {
		logger.Error("unable to add namespace", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := currPlacement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.DatabaseCreateResponse{
		Namespace: &admin.NamespaceGetResponse{
			Registry: &nsRegistry,
		},
		Placement: &admin.PlacementGetResponse{
			Placement: placementProto,
		},
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *createHandler) maybeInitPlacement(
	currPlacement clusterplacement.Placement,
	parsedReq *admin.DatabaseCreateRequest,
	placementRequest *admin.PlacementInitRequest,
	r *http.Request,
) (clusterplacement.Placement, bool, error) {
	if currPlacement == nil {
		// If we're here then there is no existing placement, so just create it. This is safe because in
		// the case where a placement did not already exist, the parse function above validated that we
		// have all the required information to create a placement.
		newPlacement, err := h.placementInitHandler.Init(placement.M3DBServiceName, r, placementRequest)
		if err != nil {
			return nil, false, err
		}

		return newPlacement, false, nil
	}

	// NB(rartoul): Pardon the branchiness, making sure every permutation is "reasoned" through for
	// the scenario where the placement already exists.
	switch dbType(parsedReq.Type) {
	case dbTypeCluster:
		if placementRequest != nil {
			// If the caller has specified a desired clustered placement and a placement already exists,
			// throw an error because the create database API should not be used for modifying clustered
			// placements. Instead, they should use the placement APIs.
			return nil, true, errClusteredPlacementAlreadyExists
		}

		if placementIsLocal(currPlacement) {
			// If the caller has specified that they desire a clustered placement (without specifying hosts)
			// and a local placement already exists then throw an error because we can't ignore their request
			// and we also can't convert a local placement to a clustered one.
			return nil, true, errCantReplaceLocalPlacementWithClustered
		}

		// This is fine because we'll just assume they want to keep the same clustered placement
		// that they already have because they didn't specify any hosts.
		return currPlacement, false, nil
	case dbTypeLocal:
		if !placementIsLocal(currPlacement) {
			// If the caller has specified that they desire a local placement and a clustered placement
			// already exists then throw an error because we can't ignore their request and we also can't
			// convert a clustered placement to a local one.
			return nil, true, errCantReplaceLocalPlacementWithClustered
		}

		// This is fine because we'll just assume they want to keep the same local placement
		// that they already have.
		return currPlacement, false, nil
	case "":
		// This is fine because we'll just assume they want to keep the same placement that they already
		// have.
		return currPlacement, false, nil
	default:
		// Invalid dbType.
		return nil, true, fmt.Errorf("unknown database type: %s", parsedReq.Type)
	}
}

func (h *createHandler) parseAndValidateRequest(
	r *http.Request,
	existingPlacement clusterplacement.Placement,
) (*admin.DatabaseCreateRequest, *admin.NamespaceAddRequest, *admin.PlacementInitRequest, *xhttp.ParseError) {
	requirePlacement := existingPlacement == nil

	defer r.Body.Close()
	rBody, err := xhttp.DurationToNanosBytes(r.Body)
	if err != nil {
		wrapped := fmt.Errorf("error converting duration to nano bytes: %s", err.Error())
		return nil, nil, nil, xhttp.NewParseError(wrapped, http.StatusBadRequest)
	}

	dbCreateReq := new(admin.DatabaseCreateRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), dbCreateReq); err != nil {
		return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	if dbCreateReq.NamespaceName == "" {
		err := fmt.Errorf("%s: namespaceName", errMissingRequiredField)
		return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	requestedDBType := dbType(dbCreateReq.Type)
	if requirePlacement &&
		requestedDBType == dbTypeCluster &&
		len(dbCreateReq.Hosts) == 0 {
		return nil, nil, nil, xhttp.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	namespaceAddRequest, err := defaultedNamespaceAddRequest(dbCreateReq, existingPlacement)
	if err != nil {
		return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	var placementInitRequest *admin.PlacementInitRequest
	if (requestedDBType == dbTypeCluster && len(dbCreateReq.Hosts) > 0) ||
		requestedDBType == dbTypeLocal {
		placementInitRequest, err = defaultedPlacementInitRequest(dbCreateReq, h.embeddedDbCfg)
		if err != nil {
			return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	return dbCreateReq, namespaceAddRequest, placementInitRequest, nil
}

func defaultedNamespaceAddRequest(
	r *admin.DatabaseCreateRequest,
	existingPlacement clusterplacement.Placement,
) (*admin.NamespaceAddRequest, error) {
	var (
		opts   = dbnamespace.NewOptions()
		dbType = dbType(r.Type)
	)
	if dbType == "" && existingPlacement != nil {
		// If they didn't provide a database type, infer it from the
		// existing placement.
		if placementIsLocal(existingPlacement) {
			dbType = dbTypeLocal
		} else {
			dbType = dbTypeCluster
		}
	}

	switch dbType {
	case dbTypeLocal, dbTypeCluster:
		opts = opts.SetRepairEnabled(false)
		retentionOpts := opts.RetentionOptions()

		if r.RetentionTime == "" {
			retentionOpts = retentionOpts.SetRetentionPeriod(defaultLocalRetentionPeriod)
		} else {
			value, err := time.ParseDuration(r.RetentionTime)
			if err != nil {
				return nil, fmt.Errorf("invalid retention time: %v", err)
			}
			retentionOpts = retentionOpts.SetRetentionPeriod(value)
		}

		retentionPeriod := retentionOpts.RetentionPeriod()

		var blockSize time.Duration
		switch {
		case r.BlockSize != nil && r.BlockSize.Time != "":
			value, err := time.ParseDuration(r.BlockSize.Time)
			if err != nil {
				return nil, fmt.Errorf("invalid block size time: %v", err)
			}
			blockSize = value

		case r.BlockSize != nil && r.BlockSize.ExpectedSeriesDatapointsPerHour > 0:
			value := r.BlockSize.ExpectedSeriesDatapointsPerHour
			blockSize = time.Duration(blockSizeFromExpectedSeriesScalar / value)
			// Snap to the nearest 5mins
			blockSizeCeil := blockSize.Truncate(5*time.Minute) + 5*time.Minute
			blockSizeFloor := blockSize.Truncate(5 * time.Minute)
			if blockSizeFloor%time.Hour == 0 ||
				blockSizeFloor%30*time.Minute == 0 ||
				blockSizeFloor%15*time.Minute == 0 ||
				blockSizeFloor%10*time.Minute == 0 {
				// Try snap to hour or 30min or 15min or 10min boundary if possible
				blockSize = blockSizeFloor
			} else {
				blockSize = blockSizeCeil
			}

			if blockSize < minRecommendCalculateBlockSize {
				blockSize = minRecommendCalculateBlockSize
			} else if blockSize > maxRecommendCalculateBlockSize {
				blockSize = maxRecommendCalculateBlockSize
			}

		default:
			// Use the maximum block size if we don't find a way to
			// recommended one based on request parameters
			max := recommendedBlockSizesByRetentionAsc[len(recommendedBlockSizesByRetentionAsc)-1]
			blockSize = max.blockSize
			for _, elem := range recommendedBlockSizesByRetentionAsc {
				if retentionPeriod <= elem.forRetentionLessThanOrEqual {
					blockSize = elem.blockSize
					break
				}
			}

		}

		retentionOpts = retentionOpts.SetBlockSize(blockSize)

		indexOpts := opts.IndexOptions().
			SetEnabled(true).
			SetBlockSize(blockSize)

		opts = opts.SetRetentionOptions(retentionOpts).
			SetIndexOptions(indexOpts)
	default:
		return nil, errInvalidDBType
	}

	return &admin.NamespaceAddRequest{
		Name:    r.NamespaceName,
		Options: dbnamespace.OptionsToProto(opts),
	}, nil
}

func defaultedPlacementInitRequest(
	r *admin.DatabaseCreateRequest,
	embeddedDbCfg *dbconfig.DBConfiguration,
) (*admin.PlacementInitRequest, error) {
	var (
		numShards         int32
		replicationFactor int32
		instances         []*placementpb.Instance
	)
	switch dbType(r.Type) {
	case dbTypeLocal:
		if embeddedDbCfg == nil {
			return nil, errMissingEmbeddedDBConfig
		}

		addr := embeddedDbCfg.ListenAddress
		port, err := portFromEmbeddedDBConfigListenAddress(addr)
		if err != nil {
			return nil, err
		}

		numShards = shardMultiplier
		replicationFactor = 1
		instances = []*placementpb.Instance{
			&placementpb.Instance{
				Id:             DefaultLocalHostID,
				IsolationGroup: DefaultLocalIsolationGroup,
				Zone:           DefaultLocalZone,
				Weight:         1,
				Endpoint:       fmt.Sprintf("127.0.0.1:%d", port),
				Hostname:       "localhost",
				Port:           uint32(port),
			},
		}
	case dbTypeCluster:

		numHosts := len(r.Hosts)
		numShards = int32(math.Min(math.MaxInt32, powerOfTwoAtLeast(float64(numHosts*shardMultiplier))))
		replicationFactor = r.ReplicationFactor
		if replicationFactor == 0 {
			replicationFactor = 3
		}

		instances = make([]*placementpb.Instance, 0, numHosts)

		for _, host := range r.Hosts {
			id := strings.TrimSpace(host.Id)
			if id == "" {
				return nil, errMissingHostID
			}

			isolationGroup := strings.TrimSpace(host.IsolationGroup)
			if isolationGroup == "" {
				isolationGroup = defaultIsolationGroup
			}

			zone := strings.TrimSpace(host.Zone)
			if zone == "" {
				zone = defaultZone
			}

			weight := host.Weight
			if weight == 0 {
				weight = 1
			}

			instances = append(instances, &placementpb.Instance{
				Id:             id,
				IsolationGroup: isolationGroup,
				Zone:           zone,
				Weight:         weight,
				Endpoint:       fmt.Sprintf("%s:%d", host.Address, host.Port),
				Hostname:       host.Address,
				Port:           host.Port,
			})
		}
	default:
		return nil, errInvalidDBType
	}

	numShardsInput := r.GetNumShards()
	if numShardsInput > 0 {
		numShards = numShardsInput
	}

	return &admin.PlacementInitRequest{
		NumShards:         numShards,
		ReplicationFactor: replicationFactor,
		Instances:         instances,
	}, nil
}

func placementIsLocal(p clusterplacement.Placement) bool {
	existingInstances := p.Instances()
	return len(existingInstances) == 1 &&
		existingInstances[0].ID() == DefaultLocalHostID &&
		existingInstances[0].IsolationGroup() == DefaultLocalIsolationGroup &&
		existingInstances[0].Zone() == DefaultLocalZone
}

func portFromEmbeddedDBConfigListenAddress(address string) (int, error) {
	colonIdx := strings.LastIndex(address, ":")
	if colonIdx == -1 || colonIdx == len(address)-1 {
		return 0, errMissingEmbeddedDBPort
	}

	return strconv.Atoi(address[colonIdx+1:])
}

func powerOfTwoAtLeast(num float64) float64 {
	return math.Pow(2, math.Ceil(math.Log2(num)))
}
