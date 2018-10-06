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
	"errors"
	"strings"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/util/logging"
	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"

	"github.com/gorilla/mux"
)

const (
	// M3DBServiceName is the service name for M3DB
	M3DBServiceName = "m3db"
	// M3AggServiceName is the service name for M3Agg
	M3AggServiceName = "m3agg"
	// TODO: Delete me?
	// DefaultServiceName is the default service ID name
	DefaultServiceName = M3DBServiceName
	// DefaultServiceEnvironment is the default service ID environment
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone
	DefaultServiceZone = "embedded"
)

var (
	errServiceNameIsRequired        = errors.New("service name is required")
	errServiceEnvironmentIsRequired = errors.New("service environment is required")
	errServiceZoneIsRequired        = errors.New("service zone is required")
)

// Handler represents a generic handler for placement endpoints.
type Handler struct {
	// This is used by other placement Handlers
	// nolint: structcheck
	client clusterclient.Client
	cfg    config.Configuration
}

// ServiceOptions are the options for Service.
type ServiceOptions struct {
	ServiceName        string
	ServiceEnvironment string
	ServiceZone        string
}

// NewServiceOptions returns a ServiceOptions with default options.
func NewServiceOptions(serviceName string) ServiceOptions {
	return ServiceOptions{
		ServiceName:        serviceName,
		ServiceEnvironment: DefaultServiceEnvironment,
		ServiceZone:        DefaultServiceZone,
	}
}

// Service gets a placement service from m3cluster client
func Service(clusterClient clusterclient.Client, opts ServiceOptions) (placement.Service, error) {
	ps, _, err := ServiceWithAlgo(clusterClient, opts)
	return ps, err
}

// ServiceWithAlgo gets a placement service from m3cluster client and
// additionally returns an algorithm instance for callers that need fine-grained
// control over placement updates.
func ServiceWithAlgo(clusterClient clusterclient.Client, opts ServiceOptions) (placement.Service, placement.Algorithm, error) {
	cs, err := clusterClient.Services(services.NewOverrideOptions())
	if err != nil {
		return nil, nil, err
	}

	if opts.ServiceName == "" {
		return nil, nil, errServiceNameIsRequired
	}
	if opts.ServiceEnvironment == "" {
		return nil, nil, errServiceEnvironmentIsRequired
	}
	if opts.ServiceZone == "" {
		return nil, nil, errServiceZoneIsRequired
	}

	sid := services.NewServiceID().
		SetName(opts.ServiceName).
		SetEnvironment(opts.ServiceEnvironment).
		SetZone(opts.ServiceZone)

	pOpts := placement.NewOptions().SetValidZone(opts.ServiceZone)

	ps, err := cs.PlacementService(sid, pOpts)
	if err != nil {
		return nil, nil, err
	}

	alg := algo.NewAlgorithm(pOpts)

	return ps, alg, nil
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

func validateAllAvailable(p placement.Placement) error {
	badInsts := []string{}
	for _, inst := range p.Instances() {
		if !inst.IsAvailable() {
			badInsts = append(badInsts, inst.ID())
		}
	}
	if len(badInsts) > 0 {
		return unsafeAddError{
			hosts: strings.Join(badInsts, ","),
		}
	}
	return nil
}
