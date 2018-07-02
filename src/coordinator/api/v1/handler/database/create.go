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

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	dbconfig "github.com/m3db/m3db/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/namespace"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/placement"
	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/coordinator/util"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	dbnamespace "github.com/m3db/m3db/src/dbnode/storage/namespace"

	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// CreateURL is the url for the database create handler.
	CreateURL = handler.RoutePrefixV1 + "/database/create"

	// CreateHTTPMethod is the HTTP method used with this resource.
	CreateHTTPMethod = http.MethodPost

	idealDatapointsPerBlock           = 720
	blockSizeFromExpectedSeriesScalar = idealDatapointsPerBlock * int64(time.Hour)
	shardMultiplier                   = 64

	dbTypeLocal                 dbType = "local"
	dbTypeCluster               dbType = "cluster"
	defaultLocalRetentionPeriod        = 24 * time.Hour
	minBlockSize                       = 30 * time.Minute
	maxBlockSize                       = 2 * time.Hour
)

var (
	errMultipleHostTypes    = errors.New("must only specify one type of host")
	errMissingRequiredField = errors.New("missing required field")
	errInvalidDBType        = errors.New("invalid database type")
	errMissingPort          = errors.New("unable to get port from address")
)

type dbType string

type createHandler struct {
	placementInitHandler   *placement.InitHandler
	namespaceAddHandler    *namespace.AddHandler
	namespaceDeleteHandler *namespace.DeleteHandler
	dbCfg                  dbconfig.DBConfiguration
}

// NewCreateHandler returns a new instance of a database create handler.
func NewCreateHandler(client clusterclient.Client, cfg config.Configuration, dbCfg dbconfig.DBConfiguration) http.Handler {
	return &createHandler{
		placementInitHandler:   placement.NewInitHandler(client, cfg),
		namespaceAddHandler:    namespace.NewAddHandler(client),
		namespaceDeleteHandler: namespace.NewDeleteHandler(client),
		dbCfg: dbCfg,
	}
}

func (h *createHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	namespaceRequest, placementRequest, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		handler.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	nsRegistry, err := h.namespaceAddHandler.Add(namespaceRequest)
	if err != nil {
		logger.Error("unable to add namespace", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	initPlacement, err := h.placementInitHandler.Init(placementRequest)
	if err != nil {
		// Attempt to delete the namespace that was just created to maintain idempotency
		err = h.namespaceDeleteHandler.Delete(namespaceRequest.Name)
		if err != nil {
			logger.Error("unable to delete namespace we just added", zap.Any("error", err))
			handler.Error(w, err, http.StatusInternalServerError)
			return
		}

		logger.Error("unable to add namespace", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	placementProto, err := initPlacement.Proto()
	if err != nil {
		logger.Error("unable to get placement protobuf", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
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

	handler.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *createHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, *admin.PlacementInitRequest, *handler.ParseError) {
	defer r.Body.Close()
	rBody, err := handler.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	dbCreateReq := new(admin.DatabaseCreateRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), dbCreateReq); err != nil {
		return nil, nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	// Required fields
	if util.HasEmptyString(dbCreateReq.NamespaceName, dbCreateReq.Type) {
		return nil, nil, handler.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	if dbType(dbCreateReq.Type) == dbTypeCluster {
		// One (and only one) hostname type required
		if len(dbCreateReq.Hostnames) == 0 && len(dbCreateReq.HostnameGroups) == 0 {
			return nil, nil, handler.NewParseError(errMissingRequiredField, http.StatusBadRequest)
		}

		if len(dbCreateReq.Hostnames) > 0 && len(dbCreateReq.HostnameGroups) > 0 {
			return nil, nil, handler.NewParseError(errMultipleHostTypes, http.StatusBadRequest)
		}
	}

	namespaceAddRequest, err := defaultedNamespaceAddRequest(dbCreateReq)
	if err != nil {
		return nil, nil, handler.NewParseError(err, http.StatusBadRequest)
	}
	placementInitRequest, err := defaultedPlacementInitRequest(dbCreateReq, h.dbCfg)
	if err != nil {
		return nil, nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return namespaceAddRequest, placementInitRequest, nil
}

func defaultedNamespaceAddRequest(r *admin.DatabaseCreateRequest) (*admin.NamespaceAddRequest, error) {
	options := dbnamespace.NewOptions()

	switch dbType(r.Type) {
	case dbTypeLocal, dbTypeCluster:
		options.SetRepairEnabled(false)

		if r.RetentionPeriodNanos <= 0 {
			options.RetentionOptions().SetRetentionPeriod(defaultLocalRetentionPeriod)
		} else {
			options.RetentionOptions().SetRetentionPeriod(time.Duration(r.RetentionPeriodNanos))
		}

		retentionPeriod := options.RetentionOptions().RetentionPeriod()
		if r.ExpectedSeriesDatapointsPerHour > 0 {
			blockSize := time.Duration(blockSizeFromExpectedSeriesScalar / r.ExpectedSeriesDatapointsPerHour)
			if blockSize < minBlockSize {
				blockSize = minBlockSize
			} else if blockSize > maxBlockSize {
				blockSize = maxBlockSize
			}
			options.RetentionOptions().SetBlockSize(blockSize)
		} else if retentionPeriod <= 12*time.Hour {
			options.RetentionOptions().SetBlockSize(30 * time.Minute)
		} else if retentionPeriod <= 24*time.Hour {
			options.RetentionOptions().SetBlockSize(1 * time.Hour)
		} else {
			options.RetentionOptions().SetBlockSize(2 * time.Hour)
		}

		options.IndexOptions().SetEnabled(true)
		options.IndexOptions().SetBlockSize(options.RetentionOptions().BlockSize())
	default:
		return nil, errInvalidDBType
	}

	return &admin.NamespaceAddRequest{
		Name:    r.NamespaceName,
		Options: dbnamespace.OptionsToProto(options),
	}, nil
}

func defaultedPlacementInitRequest(r *admin.DatabaseCreateRequest, dbCfg dbconfig.DBConfiguration) (*admin.PlacementInitRequest, error) {
	var (
		numShards         int32
		replicationFactor int32
		instances         []*placementpb.Instance
	)

	port, err := portFromAddress(dbCfg.ListenAddress)
	if err != nil {
		return nil, err
	}

	switch dbType(r.Type) {
	case dbTypeLocal:
		numShards = shardMultiplier
		replicationFactor = 1
		instances = []*placementpb.Instance{
			&placementpb.Instance{
				Id:             "localhost",
				IsolationGroup: "local",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       fmt.Sprintf("http://localhost:%d", port),
				Hostname:       "localhost",
				Port:           uint32(port),
			},
		}
	case dbTypeCluster:
		// This function assumes only one of Hostnames/HostnameGroups have been set
		numHosts := len(r.Hostnames) + len(r.HostnameGroups)
		numShards = int32(math.Min(math.MaxInt32, powerOfTwoAtLeast(float64(numHosts*shardMultiplier))))
		replicationFactor = r.ReplicationFactor
		if replicationFactor == 0 {
			replicationFactor = 3
		}
		instances = make([]*placementpb.Instance, 0, numHosts)

		for _, hostname := range r.Hostnames {
			instances = append(instances, &placementpb.Instance{
				Id:             hostname,
				IsolationGroup: "cluster",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       fmt.Sprintf("http://%s:%d", hostname, port),
				Hostname:       hostname,
				Port:           uint32(port),
			})
		}

		for _, hostnameGroup := range r.HostnameGroups {
			instances = append(instances, &placementpb.Instance{
				Id:             hostnameGroup.Hostname,
				IsolationGroup: hostnameGroup.IsolationGroup,
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       fmt.Sprintf("http://%s:%d", hostnameGroup.Hostname, port),
				Hostname:       hostnameGroup.Hostname,
				Port:           uint32(port),
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

func portFromAddress(address string) (int, error) {
	colonIdx := strings.LastIndex(address, ":")
	if colonIdx == -1 || colonIdx == len(address)-1 {
		return 0, errMissingPort
	}

	return strconv.Atoi(address[colonIdx+1:])
}

func powerOfTwoAtLeast(num float64) float64 {
	return math.Pow(2, math.Ceil(math.Log2(num)))
}
