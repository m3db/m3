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
	"strings"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/util/logging"
	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"

	"github.com/gorilla/mux"
)

const (
	// DefaultServiceName is the default service ID name
	DefaultServiceName = "m3db"
	// DefaultServiceEnvironment is the default service ID environment
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone
	DefaultServiceZone = "embedded"
	// HeaderClusterServiceName is the header used to specify the service name.
	HeaderClusterServiceName = "Cluster-Service-Name"
	// HeaderClusterEnvironmentName is the header used to specify the environment name.
	HeaderClusterEnvironmentName = "Cluster-Environment-Name"
	// HeaderClusterZoneName is the header used to specify the zone name.
	HeaderClusterZoneName = "Cluster-Zone-Name"
)

// Handler represents a generic handler for placement endpoints.
type Handler struct {
	// This is used by other placement Handlers
	// nolint: structcheck
	client clusterclient.Client
	cfg    config.Configuration
}

// Service gets a placement service from m3cluster client
func Service(clusterClient clusterclient.Client, headers http.Header) (placement.Service, error) {
	cs, err := clusterClient.Services(services.NewOverrideOptions())
	if err != nil {
		return nil, err
	}

	serviceName := DefaultServiceName
	if v := strings.TrimSpace(headers.Get(HeaderClusterServiceName)); v != "" {
		serviceName = v
	}

	serviceEnvironment := DefaultServiceEnvironment
	if v := strings.TrimSpace(headers.Get(HeaderClusterEnvironmentName)); v != "" {
		serviceEnvironment = v
	}

	serviceZone := DefaultServiceZone
	if v := strings.TrimSpace(headers.Get(HeaderClusterZoneName)); v != "" {
		serviceZone = v
	}

	sid := services.NewServiceID().
		SetName(serviceName).
		SetEnvironment(serviceEnvironment).
		SetZone(serviceZone)

	ps, err := cs.PlacementService(sid, placement.NewOptions().SetValidZone(serviceZone))
	if err != nil {
		return nil, err
	}

	return ps, nil
}

// ConvertInstancesProto converts a slice of protobuf `Instance`s to `placement.Instance`s
func ConvertInstancesProto(instancesProto []*placementpb.Instance) ([]placement.Instance, error) {
	res := make([]placement.Instance, 0, len(instancesProto))

	for _, instanceProto := range instancesProto {
		shards, err := shard.NewShardsFromProto(instanceProto.Shards)
		if err != nil {
			return nil, err
		}

		instance := placement.NewInstance().
			SetEndpoint(instanceProto.Endpoint).
			SetHostname(instanceProto.Hostname).
			SetID(instanceProto.Id).
			SetPort(instanceProto.Port).
			SetIsolationGroup(instanceProto.IsolationGroup).
			SetShards(shards).
			SetShardSetID(instanceProto.ShardSetId).
			SetWeight(instanceProto.Weight).
			SetZone(instanceProto.Zone)

		res = append(res, instance)
	}

	return res, nil
}

// RegisterRoutes registers the placement routes
func RegisterRoutes(r *mux.Router, client clusterclient.Client, cfg config.Configuration) {
	logged := logging.WithResponseTimeLogging

	r.HandleFunc(InitURL, logged(NewInitHandler(client, cfg)).ServeHTTP).Methods(InitHTTPMethod)
	r.HandleFunc(GetURL, logged(NewGetHandler(client, cfg)).ServeHTTP).Methods(GetHTTPMethod)
	r.HandleFunc(DeleteAllURL, logged(NewDeleteAllHandler(client, cfg)).ServeHTTP).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(AddURL, logged(NewAddHandler(client, cfg)).ServeHTTP).Methods(AddHTTPMethod)
	r.HandleFunc(DeleteURL, logged(NewDeleteHandler(client, cfg)).ServeHTTP).Methods(DeleteHTTPMethod)
}
