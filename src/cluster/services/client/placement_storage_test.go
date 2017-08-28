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

package client

import (
	"testing"

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"

	"github.com/stretchr/testify/require"
)

func TestStorageWithSinglePlacement(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	ps, err := newTestPlacementStorage(opts, placement.NewOptions())
	require.NoError(t, err)

	sid := services.NewServiceID()
	_, _, err = ps.Placement(sid)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	_, _, err = ps.Placement(sid)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	err = ps.Delete(sid)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.Error(t, err)
	require.Equal(t, kv.ErrAlreadyExists, err)

	pGet, v, err := ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.Equal(t, p.SetVersion(1), pGet)

	err = ps.CheckAndSet(sid, p, v)
	require.NoError(t, err)

	err = ps.CheckAndSet(sid, p, v)
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	pGet, v, err = ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.Equal(t, p.SetVersion(2), pGet)

	err = ps.Delete(sid)
	require.NoError(t, err)

	_, _, err = ps.Placement(sid)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	pGet, v, err = ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.Equal(t, p.SetVersion(1), pGet)

	proto, v, err := ps.PlacementProto(sid)
	require.NoError(t, err)
	require.Equal(t, 1, v)

	actualP, err := placement.NewPlacementFromProto(proto.(*schema.Placement))
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(0), actualP)

	// different zone or different env are different services
	_, _, err = ps.Placement(services.NewServiceID().SetName("m3db").SetEnvironment("env"))
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	_, _, err = ps.Placement(services.NewServiceID().SetName("m3db").SetZone("zone"))
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)
}

func TestStorageWithPlacementSnapshots(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	ps, err := newTestPlacementStorage(opts, placement.NewOptions().SetIsStaged(true))
	require.NoError(t, err)

	sid := services.NewServiceID().SetName("m3db").SetZone("zone").SetEnvironment("env")
	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.Error(t, err)
	require.Equal(t, kv.ErrAlreadyExists, err)

	pGet1, v, err := ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.Equal(t, p.SetVersion(1), pGet1)

	err = ps.CheckAndSet(sid, p, v)
	require.NoError(t, err)

	err = ps.CheckAndSet(sid, p, v)
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	pGet2, v, err := ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.Equal(t, p.SetVersion(2), pGet2)

	newProto, v, err := ps.PlacementProto(sid)
	require.NoError(t, err)
	require.Equal(t, 2, v)

	newPs, err := placement.NewPlacementsFromProto(newProto.(*schema.PlacementSnapshots))
	require.NoError(t, err)
	require.Equal(t, pGet1.SetVersion(0), newPs[0])
	require.Equal(t, pGet2.SetVersion(0), newPs[1])

	err = ps.Delete(sid)
	require.NoError(t, err)

	_, _, err = ps.Placement(sid)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	pGet3, v, err := ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.Equal(t, p.SetVersion(1), pGet3)

	// different zone or different env are different services
	_, _, err = ps.Placement(services.NewServiceID().SetName("m3db").SetEnvironment("env"))
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	_, _, err = ps.Placement(services.NewServiceID().SetName("m3db").SetZone("zone"))
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)
}

func TestPlacementNamespaceOverride(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	ps, err := newTestPlacementStorage(
		opts.SetNamespaceOptions(
			services.NewNamespaceOptions().SetPlacementNamespace("test_ns"),
		),
		placement.NewOptions(),
	)
	require.NoError(t, err)

	sid := services.NewServiceID().SetName("m3db")

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)

	err = ps.Set(sid, p)
	require.NoError(t, err)

	newP, v, err := ps.Placement(sid)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.Equal(t, newP, p.SetVersion(v))

	ps2, err := newTestPlacementStorage(opts, placement.NewOptions())
	require.NoError(t, err)

	_, _, err = ps2.Placement(sid)
	require.Error(t, err)
}

func newTestPlacementStorage(opts Options, pOpts services.PlacementOptions) (services.PlacementStorage, error) {
	if opts.KVGen() == nil {
		return nil, errNoKVGen
	}

	return newPlacementStorage(
		keyFnWithNamespace(placementNamespace(opts.NamespaceOptions().PlacementNamespace())),
		opts.KVGen(),
		newHelper(pOpts),
	), nil
}
