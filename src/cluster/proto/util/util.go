// Copyright (c) 2017 Uber Technologies, Inc.
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

// Package util provides utility functions for converting m3cluster native
// types to and from their corresponding protobuf types.
package util

import (
	metadataproto "github.com/m3db/m3cluster/generated/proto/metadata"
	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/services"
)

// StagedPlacementToProto converts a staged placement to a proto.
func StagedPlacementToProto(sp services.StagedPlacement) (*placementproto.PlacementSnapshots, error) {
	return PlacementsToProto(sp.Placements())
}

// PlacementsToProto converts a list of ServicePlacement to a proto.
func PlacementsToProto(placements services.Placements) (*placementproto.PlacementSnapshots, error) {
	snapshots := make([]*placementproto.Placement, 0, len(placements))
	for _, p := range placements {
		placementProto, err := PlacementToProto(p)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, placementProto)
	}
	return &placementproto.PlacementSnapshots{
		Snapshots: snapshots,
	}, nil
}

// PlacementToProto converts a ServicePlacement to a placement proto
func PlacementToProto(p services.Placement) (*placementproto.Placement, error) {
	instances := make(map[string]*placementproto.Instance, p.NumInstances())
	for _, instance := range p.Instances() {
		pi, err := PlacementInstanceToProto(instance)
		if err != nil {
			return nil, err
		}
		instances[instance.ID()] = pi
	}

	return &placementproto.Placement{
		Instances:     instances,
		ReplicaFactor: uint32(p.ReplicaFactor()),
		NumShards:     uint32(p.NumShards()),
		IsSharded:     p.IsSharded(),
		CutoverTime:   p.CutoverNanos(),
		IsMirrored:    p.IsMirrored(),
	}, nil
}

// PlacementInstanceToProto converts a PlacementInstance type to an Instance
// proto message.
func PlacementInstanceToProto(p services.PlacementInstance) (*placementproto.Instance, error) {
	ss, err := p.Shards().Proto()
	if err != nil {
		return &placementproto.Instance{}, err
	}

	return &placementproto.Instance{
		Id:         p.ID(),
		Rack:       p.Rack(),
		Zone:       p.Zone(),
		Weight:     p.Weight(),
		Endpoint:   p.Endpoint(),
		Shards:     ss,
		ShardSetId: p.ShardSetID(),
		Hostname:   p.Hostname(),
		Port:       p.Port(),
	}, nil
}

// MetadataToProto converts an instance of services.Metadata to its
// corresponding proto-serialized Metadata message.
func MetadataToProto(m services.Metadata) metadataproto.Metadata {
	return metadataproto.Metadata{
		Port:              m.Port(),
		LivenessInterval:  int64(m.LivenessInterval()),
		HeartbeatInterval: int64(m.HeartbeatInterval()),
	}
}
