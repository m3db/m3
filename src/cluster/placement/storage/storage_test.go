// Copyright (c) 2016 Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageWithSinglePlacement(t *testing.T) {
	ps := newTestPlacementStorage(mem.NewStore(), placement.NewOptions())
	err := ps.Delete()
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)

	pGet, err := ps.SetIfNotExist(p)
	assert.Equal(t, 1, pGet.Version())
	require.NoError(t, err)

	_, err = ps.SetIfNotExist(p)
	require.Error(t, err)
	require.Equal(t, kv.ErrAlreadyExists, err)

	pGet, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(1), pGet)

	_, err = ps.PlacementForVersion(0)
	require.Error(t, err)

	_, err = ps.PlacementForVersion(2)
	require.Error(t, err)

	h, err := ps.PlacementForVersion(1)
	require.NoError(t, err)
	require.Equal(t, pGet, h)

	pGet, err = ps.CheckAndSet(p, pGet.Version())
	require.NoError(t, err)
	assert.Equal(t, 2, pGet.Version())

	_, err = ps.CheckAndSet(p, pGet.Version()-1)
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	pGet, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 2, pGet.Version())
	require.Equal(t, p.SetVersion(2), pGet)

	err = ps.Delete()
	require.NoError(t, err)

	_, err = ps.Placement()
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	pGet, err = ps.SetIfNotExist(p)
	require.NoError(t, err)
	assert.Equal(t, 1, pGet.Version())

	pGet, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, pGet.Version())
	require.Equal(t, p.SetVersion(1), pGet)

	proto, v, err := ps.Proto()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	actualP, err := placement.NewPlacementFromProto(proto.(*placementpb.Placement))
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(0), actualP)
}

func TestStorageWithPlacementSnapshots(t *testing.T) {
	ps := newTestPlacementStorage(mem.NewStore(), placement.NewOptions().SetIsStaged(true))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0).
		SetCutoverNanos(100)

	pGet1, err := ps.SetIfNotExist(p)
	require.NoError(t, err)
	assert.Equal(t, 1, pGet1.Version())

	_, err = ps.SetIfNotExist(p)
	require.Error(t, err)

	pGet1, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, pGet1.Version())
	require.Equal(t, p.SetVersion(1), pGet1)

	_, err = ps.PlacementForVersion(0)
	require.Error(t, err)

	_, err = ps.PlacementForVersion(2)
	require.Error(t, err)

	h, err := ps.PlacementForVersion(1)
	require.NoError(t, err)
	require.Equal(t, pGet1, h)

	_, err = ps.CheckAndSet(p, pGet1.Version())
	require.Error(t, err)

	p = p.SetCutoverNanos(p.CutoverNanos() + 1)
	pGet2, err := ps.CheckAndSet(p, pGet1.Version())
	require.NoError(t, err)
	assert.Equal(t, 2, pGet2.Version())

	_, err = ps.CheckAndSet(p.Clone().SetCutoverNanos(p.CutoverNanos()+1), pGet1.Version()-1)
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	pGet2, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 2, pGet2.Version())
	require.Equal(t, p.SetVersion(2), pGet2)

	newProto, v, err := ps.Proto()
	require.NoError(t, err)
	require.Equal(t, 2, v)

	newPs, err := placement.NewPlacementsFromProto(newProto.(*placementpb.PlacementSnapshots))
	require.NoError(t, err)
	require.Equal(t, pGet1.SetVersion(0), newPs[0])
	require.Equal(t, pGet2.SetVersion(0), newPs[1])

	err = ps.Delete()
	require.NoError(t, err)

	_, err = ps.Placement()
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	pGet2, err = ps.SetIfNotExist(p)
	require.NoError(t, err)
	assert.Equal(t, 1, pGet2.Version())

	pGet3, err := ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, pGet3.Version())
	require.Equal(t, p.SetVersion(1), pGet3)
}

func TestCheckAndSetProto(t *testing.T) {
	m := mem.NewStore()
	ps := newTestPlacementStorage(m, placement.NewOptions())

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)

	pGet, err := ps.SetIfNotExist(p)
	require.NoError(t, err)
	assert.Equal(t, 1, pGet.Version())

	newProto, v, err := ps.Proto()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	_, err = ps.CheckAndSetProto(newProto, 2)
	require.Error(t, err)

	version, err := ps.CheckAndSetProto(newProto, 1)
	require.NoError(t, err)
	assert.Equal(t, 2, version)

	require.NoError(t, ps.Delete())

	version, err = ps.CheckAndSetProto(newProto, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, version)
}

func TestDryrun(t *testing.T) {
	m := mem.NewStore()
	dryrunPS := newTestPlacementStorage(m, placement.NewOptions().SetDryrun(true))
	ps := newTestPlacementStorage(m, placement.NewOptions())

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)

	dryPGet, err := dryrunPS.SetIfNotExist(p)
	require.NoError(t, err)
	assert.Equal(t, 0, dryPGet.Version())

	_, err = ps.Placement()
	require.Error(t, err)

	pGet, err := ps.SetIfNotExist(p)
	require.NoError(t, err)
	assert.Equal(t, 1, pGet.Version())

	pGet, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, pGet.Version())

	_, err = dryrunPS.CheckAndSet(p, 1)
	require.NoError(t, err)

	pGet, _ = ps.Placement()
	require.Equal(t, 1, pGet.Version())

	err = dryrunPS.Delete()
	require.NoError(t, err)

	pGet, err = ps.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, pGet.Version())

	dryPGet, err = dryrunPS.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, dryPGet.Version())

	err = ps.Delete()
	require.NoError(t, err)

	_, err = dryrunPS.Placement()
	require.Error(t, err)
}

func newTestPlacementStorage(store kv.Store, pOpts placement.Options) placement.Storage {
	return NewPlacementStorage(store, "key", pOpts)
}
