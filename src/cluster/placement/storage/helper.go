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

package storage

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"

	"github.com/gogo/protobuf/proto"
)

var (
	errInvalidProtoForSinglePlacement    = errors.New("invalid proto for single placement")
	errInvalidProtoForPlacementSnapshots = errors.New("invalid proto for placement snapshots")
	errNoPlacementInTheSnapshots         = errors.New("not placement in the snapshots")
)

// helper handles placement marshaling and validation.
type helper interface {
	// Placement retrieves the placement stored in kv.Store.
	Placement() (placement.Placement, int, error)

	// PlacementProto retrieves the proto stored in kv.Store.
	PlacementProto() (proto.Message, int, error)

	// GenerateProto generates the proto message for the new placement, it may read the kv.Store
	// if existing placement data is needed.
	GenerateProto(p placement.Placement) (proto.Message, error)

	// ValidateProto validates if the given proto message is valid for placement.
	ValidateProto(proto proto.Message) error

	// PlacementForVersion returns the placement of a specific version.
	PlacementForVersion(version int) (placement.Placement, error)
}

// newHelper returns a new placement storage helper.
func newHelper(store kv.Store, key string, opts placement.Options) helper {
	if opts.IsStaged() {
		return newStagedPlacementHelper(store, key, opts.Compress())
	}

	return newPlacementHelper(store, key)
}

type placementHelper struct {
	store kv.Store
	key   string
}

func newPlacementHelper(store kv.Store, key string) helper {
	return &placementHelper{
		store: store,
		key:   key,
	}
}

func (h *placementHelper) PlacementForVersion(version int) (placement.Placement, error) {
	values, err := h.store.History(h.key, version, version+1)
	if err != nil {
		return nil, err
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("invalid number of placements returned: %d, expecting 1", len(values))
	}

	return placementFromValue(values[0])
}

func (h *placementHelper) Placement() (placement.Placement, int, error) {
	v, err := h.store.Get(h.key)
	if err != nil {
		return nil, 0, err
	}

	p, err := placementFromValue(v)
	return p, v.Version(), err
}

func (h *placementHelper) PlacementProto() (proto.Message, int, error) {
	v, err := h.store.Get(h.key)
	if err != nil {
		return nil, 0, err
	}

	p, err := placementProtoFromValue(v)
	return p, v.Version(), err
}

func (h *placementHelper) GenerateProto(p placement.Placement) (proto.Message, error) {
	return p.Proto()
}

func (h *placementHelper) ValidateProto(proto proto.Message) error {
	placementProto, ok := proto.(*placementpb.Placement)
	if !ok {
		return errInvalidProtoForSinglePlacement
	}

	p, err := placement.NewPlacementFromProto(placementProto)
	if err != nil {
		return err
	}

	return placement.Validate(p)
}

type stagedPlacementHelper struct {
	store    kv.Store
	key      string
	compress bool
}

func newStagedPlacementHelper(
	store kv.Store,
	key string,
	compress bool,
) helper {
	return &stagedPlacementHelper{
		store:    store,
		key:      key,
		compress: compress,
	}
}

// Placement returns the last placement in the snapshots.
func (h *stagedPlacementHelper) Placement() (placement.Placement, int, error) {
	ps, v, err := h.placements()
	if err != nil {
		return nil, 0, err
	}

	latest := ps.Latest()
	latest.SetVersion(v)

	return latest, v, nil
}

func (h *stagedPlacementHelper) PlacementProto() (proto.Message, int, error) {
	value, err := h.store.Get(h.key)
	if err != nil {
		return nil, 0, err
	}

	ps, err := placementSnapshotsProtoFromValue(value)
	return ps, value.Version(), err
}

// GenerateProto generates a proto message with placement slice
// containing only single active placement - the specified placement with cutover time set to 0.
// This ensures backward comapatiblity with clients that rely on staged placement
// and expect to find at least one placement snapshot having CutoverNanos < now.
func (h *stagedPlacementHelper) GenerateProto(p placement.Placement) (proto.Message, error) {
	active := p.SetCutoverNanos(0)
	ps, err := placement.NewPlacementsFromLatest(active)
	if err != nil {
		return nil, err
	}

	if h.compress {
		return ps.ProtoCompressed()
	}

	return ps.Proto()
}

func (h *stagedPlacementHelper) ValidateProto(proto proto.Message) error {
	placementsProto, ok := proto.(*placementpb.PlacementSnapshots)
	if !ok {
		return errInvalidProtoForPlacementSnapshots
	}

	_, err := placement.NewPlacementsFromProto(placementsProto)
	return err
}

func (h *stagedPlacementHelper) placements() (*placement.Placements, int, error) {
	value, err := h.store.Get(h.key)
	if err != nil {
		return nil, 0, err
	}

	ps, err := placementsFromValue(value)
	return ps, value.Version(), err
}

func (h *stagedPlacementHelper) PlacementForVersion(version int) (placement.Placement, error) {
	values, err := h.store.History(h.key, version, version+1)
	if err != nil {
		return nil, err
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("invalid number of placements returned: %d, expecting 1", len(values))
	}

	v := values[0]
	ps, err := placementsFromValue(v)
	if err != nil {
		return nil, err
	}

	latest := ps.Latest()
	latest.SetVersion(v.Version())

	return latest, nil
}

func placementProtoFromValue(v kv.Value) (*placementpb.Placement, error) {
	var placementProto placementpb.Placement
	if err := v.Unmarshal(&placementProto); err != nil {
		return nil, err
	}

	return &placementProto, nil
}

func placementFromValue(v kv.Value) (placement.Placement, error) {
	placementProto, err := placementProtoFromValue(v)
	if err != nil {
		return nil, err
	}

	p, err := placement.NewPlacementFromProto(placementProto)
	if err != nil {
		return nil, err
	}

	return p.SetVersion(v.Version()), nil
}

func placementSnapshotsProtoFromValue(v kv.Value) (*placementpb.PlacementSnapshots, error) {
	var placementsProto placementpb.PlacementSnapshots
	if err := v.Unmarshal(&placementsProto); err != nil {
		return nil, err
	}

	return &placementsProto, nil
}

func placementsFromValue(v kv.Value) (*placement.Placements, error) {
	placementsProto, err := placementSnapshotsProtoFromValue(v)
	if err != nil {
		return nil, err
	}

	ps, err := placement.NewPlacementsFromProto(placementsProto)
	if err != nil {
		return nil, err
	}

	return ps, nil
}
