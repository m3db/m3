// Copyright (c) 2020 Uber Technologies, Inc.
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

package memcluster

import (
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReusesStores(t *testing.T) {
	key := "my_key"

	c := New(kv.NewOverrideOptions())
	store, err := c.TxnStore(kv.NewOverrideOptions())
	require.NoError(t, err)
	version, err := store.Set(key, &dummyProtoMessage{"my_value"})
	require.NoError(t, err)

	// retrieve the same store
	sameStore, err := c.TxnStore(kv.NewOverrideOptions())
	require.NoError(t, err)

	v, err := sameStore.Get(key)
	require.NoError(t, err)
	assert.Equal(t, version, v.Version())

	// other store doesn't have the value.
	otherZone, err := c.TxnStore(kv.NewOverrideOptions().SetZone("other"))
	require.NoError(t, err)
	_, err = otherZone.Get(key)
	assert.EqualError(t, err, "key not found")
}

func TestServices_Placement(t *testing.T) {
	c := New(kv.NewOverrideOptions())
	svcs, err := c.Services(services.NewOverrideOptions())
	require.NoError(t, err)

	placementSvc, err := svcs.PlacementService(services.NewServiceID().SetName("test_svc"), placement.NewOptions())
	require.NoError(t, err)

	p := placement.NewPlacement().SetInstances([]placement.Instance{
		placement.NewInstance().SetHostname("host").SetEndpoint("127.0.0.1"),
	})

	p, err = placementSvc.Set(p)
	require.NoError(t, err)

	retrieved, err := placementSvc.Placement()
	require.NoError(t, err)

	// n.b.: placements are hard to compare directly since they're interfaces and contain more pointers than
	// they ought, and it's not worth writing the method here.
	assert.Equal(t, p.Version(), retrieved.Version())
}

// dummyProtoMessage implements proto.Message and exists solely as a thing
// to pass to a kv.Store.
type dummyProtoMessage struct {
	Val string
}

func (d *dummyProtoMessage) Reset() {
}

func (d *dummyProtoMessage) String() string {
	return d.Val
}

func (d *dummyProtoMessage) ProtoMessage() {
}
