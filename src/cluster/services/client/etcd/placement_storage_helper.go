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

package etcd

import (
	"errors"

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/proto/util"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"

	"github.com/golang/protobuf/proto"
)

var (
	errInvalidProtoForSinglePlacement    = errors.New("invalid proto for single placement")
	errInvalidProtoForPlacementSnapshots = errors.New("invalid proto for placement snapshots")
	errNoPlacementInTheSnapshots         = errors.New("not placement in the snapshots")
)

type placementStorageHelper interface {
	// Placement retrieves the placement stored on kv.Store.
	Placement(store kv.Store, key string) (services.Placement, int, error)

	// PlacementProto retrieves the proto stored on kv.Store.
	PlacementProto(store kv.Store, key string) (proto.Message, int, error)

	// GenerateProto generates the proto message for the new placement, it may read the kv.Store
	// if existing placement data is needed.
	GenerateProto(store kv.Store, key string, p services.Placement) (proto.Message, error)

	// ValidateProto validates if the given proto message is valid for placement.
	ValidateProto(proto proto.Message) error
}

// newHelper returns a new placement storage helper.
func newHelper(opts services.PlacementOptions) placementStorageHelper {
	if opts.IsStaged() {
		return newStagedPlacementHelper()
	}

	return newPlacementHelper()
}

type placementHelper struct{}

func newPlacementHelper() placementStorageHelper {
	return placementHelper{}
}

func (placementHelper) Placement(store kv.Store, key string) (services.Placement, int, error) {
	v, err := store.Get(key)
	if err != nil {
		return nil, 0, err
	}

	p, err := placementFromValue(v)
	return p, v.Version(), err
}

func (placementHelper) PlacementProto(store kv.Store, key string) (proto.Message, int, error) {
	v, err := store.Get(key)
	if err != nil {
		return nil, 0, err
	}

	p, err := placementProtoFromValue(v)
	return p, v.Version(), err
}

func (placementHelper) GenerateProto(store kv.Store, key string, p services.Placement) (proto.Message, error) {
	return util.PlacementToProto(p)
}

func (placementHelper) ValidateProto(proto proto.Message) error {
	placementProto, ok := proto.(*schema.Placement)
	if !ok {
		return errInvalidProtoForSinglePlacement
	}

	p, err := placement.NewPlacementFromProto(placementProto)
	if err != nil {
		return err
	}

	return placement.Validate(p)
}

type stagedPlacementHelper struct{}

func newStagedPlacementHelper() placementStorageHelper {
	return stagedPlacementHelper{}
}

// Placement returns the last placement in the snapshots.
func (h stagedPlacementHelper) Placement(store kv.Store, key string) (services.Placement, int, error) {
	ps, v, err := h.placements(store, key)
	if err != nil {
		return nil, 0, err
	}

	l := len(ps)
	if l == 0 {
		return nil, 0, errNoPlacementInTheSnapshots
	}

	return ps[l-1], v, nil
}

func (h stagedPlacementHelper) PlacementProto(store kv.Store, key string) (proto.Message, int, error) {
	value, err := store.Get(key)
	if err != nil {
		return nil, 0, err
	}

	ps, err := placementSnapshotsProtoFromValue(value)
	return ps, value.Version(), err
}

// GenerateProto generates a proto message with the placement appended to the snapshots.
func (h stagedPlacementHelper) GenerateProto(store kv.Store, key string, p services.Placement) (proto.Message, error) {
	ps, _, err := h.placements(store, key)
	if err != nil && err != kv.ErrNotFound {
		return nil, err
	}

	return util.PlacementsToProto(append(ps, p))
}

func (h stagedPlacementHelper) ValidateProto(proto proto.Message) error {
	placementsProto, ok := proto.(*schema.PlacementSnapshots)
	if !ok {
		return errInvalidProtoForPlacementSnapshots
	}

	_, err := placement.NewPlacementsFromProto(placementsProto)
	return err
}

func (h stagedPlacementHelper) placements(store kv.Store, key string) (services.Placements, int, error) {
	value, err := store.Get(key)
	if err != nil {
		return nil, 0, err
	}

	ps, err := placementsFromValue(value)
	return ps, value.Version(), err
}
