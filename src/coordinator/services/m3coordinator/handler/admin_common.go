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
	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
)

const (
	// DefaultServiceName is the default service ID name
	DefaultServiceName = "m3db"

	// DefaultServiceEnvironment is the default service ID environment
	DefaultServiceEnvironment = "default_env"

	// DefaultServiceZone is the default service ID zone
	DefaultServiceZone = "embedded"
)

// AdminHandler represents a generic handler for admin endpoints.
type AdminHandler struct {
	clusterClient m3clusterClient.Client
}

// PlacementService gets a placement service from an m3cluster client
func PlacementService(clusterClient m3clusterClient.Client) (placement.Service, error) {
	cs, err := clusterClient.Services(services.NewOptions())
	if err != nil {
		return nil, err
	}

	sid := services.NewServiceID().
		SetName(DefaultServiceName).
		SetEnvironment(DefaultServiceEnvironment).
		SetZone(DefaultServiceZone)

	ps, err := cs.PlacementService(sid, placement.NewOptions())
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
			SetRack(instanceProto.Rack).
			SetShards(shards).
			SetShardSetID(instanceProto.ShardSetId).
			SetWeight(instanceProto.Weight).
			SetZone(instanceProto.Zone)

		res = append(res, instance)
	}

	return res, nil
}
