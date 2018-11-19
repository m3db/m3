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
)

type dbType string

type createHandler struct {
	placementInitHandler   *placement.InitHandler
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
	return &createHandler{
		placementInitHandler: placement.NewInitHandler(
			placement.HandlerOptions{ClusterClient: client, Config: cfg}),
		namespaceAddHandler:    namespace.NewAddHandler(client),
		namespaceDeleteHandler: namespace.NewDeleteHandler(client),
		embeddedDbCfg:          embeddedDbCfg,
	}
}

func (h *createHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	namespaceRequest, placementRequest, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	nsRegistry, err := h.namespaceAddHandler.Add(namespaceRequest)
	if err != nil {
		logger.Error("unable to add namespace", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	initPlacement, err := h.placementInitHandler.Init(placement.M3DBServiceName, r, placementRequest)
	if err != nil {
		// Attempt to delete the namespace that was just created to maintain idempotency.
		nsDeleteErr := h.namespaceDeleteHandler.Delete(namespaceRequest.Name)
		if nsDeleteErr != nil {
			logger.Error(
				"unable to delete namespace we just added",
				zap.Any("originalError", err),
				zap.Any("namespaceDeleteError", nsDeleteErr))
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}

		logger.Error("unable to initialize placement", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := initPlacement.Proto()
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

func (h *createHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, *admin.PlacementInitRequest, *xhttp.ParseError) {
	defer r.Body.Close()
	rBody, err := xhttp.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	dbCreateReq := new(admin.DatabaseCreateRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), dbCreateReq); err != nil {
		return nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	// Required fields
	if util.HasEmptyString(dbCreateReq.NamespaceName, dbCreateReq.Type) {
		return nil, nil, xhttp.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	if dbType(dbCreateReq.Type) == dbTypeCluster && len(dbCreateReq.Hosts) == 0 {
		return nil, nil, xhttp.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	namespaceAddRequest, err := defaultedNamespaceAddRequest(dbCreateReq)
	if err != nil {
		return nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}
	placementInitRequest, err := defaultedPlacementInitRequest(dbCreateReq, h.embeddedDbCfg)
	if err != nil {
		return nil, nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return namespaceAddRequest, placementInitRequest, nil
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

	return &admin.PlacementInitRequest{
		NumShards:         numShards,
		ReplicationFactor: replicationFactor,
		Instances:         instances,
	}, nil
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
