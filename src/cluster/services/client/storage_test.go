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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/stretchr/testify/require"
)

func TestPlacementStorage(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	ps, err := newPlacementStorage(opts)
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

	// different zone or different env are different services
	_, _, err = ps.Placement(services.NewServiceID().SetName("m3db").SetEnvironment("env"))
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	_, _, err = ps.Placement(services.NewServiceID().SetName("m3db").SetZone("zone"))
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)
}

// newPlacementStorage returns a client of placement.Storage
func newPlacementStorage(opts Options) (placement.Storage, error) {
	if opts.KVGen() == nil {
		return nil, errNoKVGen
	}

	return &client{
		kvManagers: map[string]*kvManager{},
		opts:       opts,
	}, nil
}
