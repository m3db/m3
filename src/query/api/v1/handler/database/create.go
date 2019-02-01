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
	"sort"
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
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// CreateURL is the url for the database create handler.
	CreateURL = handler.RoutePrefixV1 + "/database/create"

	// CreateHTTPMethod is the HTTP method used with this resource.
	CreateHTTPMethod = http.MethodPost

	// DefaultLocalHostID is the default local host ID when creating a database.
	DefaultLocalHostID = "m3db_local"

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

	errClusteredPlacementAlreadyExists = errors.New("cannot use database create API to modify clustered placements are they are instantiated. Use the placement APIs directly to make placement changes, or remove the list of hosts from the request to add a namespace without modifying the placement")
)

type dbType string

type createHandler struct {
	placementInitHandler   *placement.InitHandler
	placementGetHandler    *placement.GetHandler
	namespaceAddHandler    *namespace.AddHandler
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
		namespaceDeleteHandler: namespace.NewDeleteHandler(client),
		embeddedDbCfg:          embeddedDbCfg,
	}
}

func (h *createHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx)
	)

	currPlacement, _, err := h.placementGetHandler.Get(placement.M3DBServiceName, r)
	if err != nil {
		logger.Error("unable to get  placement", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
	}

	requirePlacementRequest := currPlacement == nil
	parsedReq, namespaceRequest, placementRequest, rErr := h.parseRequest(r, requirePlacementRequest)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	if currPlacement != nil &&
		dbType(parsedReq.Type) == dbTypeCluster &&
		placementRequest != nil {
		// If an existing clustered placement exists AND the caller has provided a desired
		// placement, then we need to compare the requested placement and the existing one.
		err := comparePlacements(placementRequest, currPlacement)
		if err != nil {
			// If the requested placement differs from the existing one for
			// a clustered placement throw an error so the callers knows they
			// can't use this API to make clustered placement changes.
			wrappedErr := fmt.Errorf(
				"%s, specific error: %s", errClusteredPlacementAlreadyExists, err.Error())
			logger.Error("unable to create database", zap.Error(wrappedErr))
			xhttp.Error(w, wrappedErr, http.StatusBadRequest)
			return
		}

		// If the requested placement is the same as the existing one, let it slide.
	}

	// If we've made it this far than we're in one of the following situations, all of which
	// are valid:
	//
	//     1. Clustered placement requested and no placement exists.
	//     2. Clustered placement request and placement requested, but requested placement is
	//        exactly the same as the existing placement so no changes are required.
	//     3. Local placement requested and no placement exists.
	//     4. Local placement requested and placement exists, but it doesn't matter because all
	//        local placements are the same  so we just won't make any changes.
	if currPlacement == nil {
		// Create the requested placement if we don't have one already. This is safe because in the case
		// where a placement did not already exist, the parse function above validated that we have all
		// the required information to create a placement.
		currPlacement, err = h.placementInitHandler.Init(placement.M3DBServiceName, r, placementRequest)
		if err != nil {
			logger.Error("unable to initialize placement", zap.Error(err))
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}
	}

	nsRegistry, err := h.namespaceAddHandler.Add(namespaceRequest)
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

func (h *createHandler) parseRequest(r *http.Request, requirePlacement bool) (*admin.DatabaseCreateRequest, *admin.NamespaceAddRequest, *admin.PlacementInitRequest, *xhttp.ParseError) {
	defer r.Body.Close()
	rBody, err := xhttp.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	dbCreateReq := new(admin.DatabaseCreateRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), dbCreateReq); err != nil {
		return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	// Required fields
	if util.HasEmptyString(dbCreateReq.NamespaceName, dbCreateReq.Type) {
		return nil, nil, nil, xhttp.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	if requirePlacement &&
		dbType(dbCreateReq.Type) == dbTypeCluster &&
		len(dbCreateReq.Hosts) == 0 {
		return nil, nil, nil, xhttp.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	namespaceAddRequest, err := defaultedNamespaceAddRequest(dbCreateReq)
	if err != nil {
		return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	var placementInitRequest *admin.PlacementInitRequest
	if len(dbCreateReq.Hosts) > 0 {
		placementInitRequest, err = defaultedPlacementInitRequest(dbCreateReq, h.embeddedDbCfg)
		if err != nil {
			return nil, nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	return dbCreateReq, namespaceAddRequest, placementInitRequest, nil
}

func defaultedNamespaceAddRequest(r *admin.DatabaseCreateRequest) (*admin.NamespaceAddRequest, error) {
	opts := dbnamespace.NewOptions()

	switch dbType(r.Type) {
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
				IsolationGroup: "local",
				Zone:           "embedded",
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

func comparePlacements(requested *admin.PlacementInitRequest, existing clusterplacement.Placement) error {
	if requested.NumShards != 0 && int(requested.NumShards) != existing.NumShards() {
		return fmt.Errorf(
			"requested num shards: %d does not match existing: %d",
			requested.NumShards, existing.NumShards())
	}

	if requested.ReplicationFactor != 0 && int(requested.ReplicationFactor) != existing.ReplicaFactor() {
		return fmt.Errorf(
			"requested replication factor: %d does not matching existing:  %d",
			requested.ReplicationFactor, existing.ReplicaFactor())
	}

	if len(requested.Instances) > 0 && len(requested.Instances) != existing.NumInstances() {
		return fmt.Errorf(
			"requested num hosts: %d does not matching existing:  %d",
			len(requested.Instances), existing.NumInstances())
	}

	var (
		requestedInstances = requested.Instances
		existingInstances  = existing.Instances()
	)
	sort.Slice(requestedInstances, func(i, j int) bool {
		var (
			leftID  = requestedInstances[i].Id
			rightID = requestedInstances[j].Id
		)
		return strings.Compare(leftID, rightID) == -1
	})
	sort.Slice(existingInstances, func(i, j int) bool {
		var (
			leftID  = existingInstances[i].ID()
			rightID = existingInstances[j].ID()
		)
		return strings.Compare(leftID, rightID) == -1
	})

	for i := 0; i < len(requestedInstances); i++ {
		var (
			requested = requestedInstances[i]
			existing  = existingInstances[i]
		)

		if requested.Id != existing.ID() {
			return fmt.Errorf(
				"requested ID: %s does not match existing: %s",
				requested.Id, existing.ID())
		}

		if requested.IsolationGroup != existing.IsolationGroup() {
			return fmt.Errorf(
				"requested isolation group: %s does not match existing: %s for host: %s",
				requested.IsolationGroup, existing.IsolationGroup(), requested.Id)
		}

		if requested.Endpoint != existing.Endpoint() {
			return fmt.Errorf(
				"requested endpoint: %s does not match existing: %s for host: %s",
				requested.Endpoint, existing.Endpoint(), requested.Id)
		}

		if requested.Port != existing.Port() {
			return fmt.Errorf(
				"requested port: %d does not match existing: %d for host: %s",
				requested.Port, existing.Port(), requested.Id)
		}

		if requested.Weight != existing.Weight() {
			return fmt.Errorf(
				"requested weight: %d does not match existing: %d for host: %s",
				requested.Weight, existing.Weight(), requested.Id)
		}

		if requested.Zone != "" && requested.Zone != existing.Zone() {
			return fmt.Errorf(
				"requested zone: %s does not match existing: %s for host: %s",
				requested.Zone, existing.Zone(), requested.Id)
		}

		if requested.Hostname != "" && requested.Hostname != existing.Hostname() {
			return fmt.Errorf(
				"requested weight: %s does not match existing: %s for host: %s",
				requested.Hostname, existing.Hostname(), requested.Id)
		}

		if len(requested.Shards) > 0 {
			// Not supposed to provide shards in this API anyways.
			return fmt.Errorf(
				"cannot override number of shards on host: %s using database create API", requested.Id)
		}

		if requested.ShardSetId != 0 {
			// Not supposed to provide shard set ID in this API anyways.
			return fmt.Errorf(
				"cannot override shard set ID on host: %s using database create API", requested.Id)
		}
	}

	return nil
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
