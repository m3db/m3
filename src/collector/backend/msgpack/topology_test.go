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

package msgpack

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const (
	testTopologyKey = "testTopologyKey"
)

func TestTopologyOpenAlreadyOpen(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyOpen
	require.Equal(t, errTopologyIsOpenOrClosed, topo.Open())
}

func TestTopologyOpenSuccess(t *testing.T) {
	topo := testTopology(t)
	require.NoError(t, topo.Open())
}

func TestTopologyRouteClosed(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyClosed

	err := topo.Route(testCounter, testPoliciesList)
	require.Equal(t, errTopologyIsNotOpenOrClosed, err)
}

func TestTopologyRoutePlacementError(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyOpen
	errTestRoute := errors.New("test route error")
	topo.placement = &mockPlacement{
		routeFn: func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error { return errTestRoute },
	}
	err := topo.Route(testCounter, testPoliciesList)
	require.Equal(t, errTestRoute, err)
}

func TestTopologyRouteSuccess(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyOpen
	topo.placement = &mockPlacement{
		routeFn: func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error { return nil },
	}
	err := topo.Route(testCounter, testPoliciesList)
	require.NoError(t, err)
}

func TestTopologyClosedAlreadyClosed(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyClosed
	require.Equal(t, errTopologyIsNotOpenOrClosed, topo.Close())
}

func TestTopologyClosedSuccess(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyOpen
	require.NoError(t, topo.Close())
	require.Equal(t, topologyClosed, topo.state)
	require.Nil(t, topo.placement)
}

func TestTopologyToPlacementClosed(t *testing.T) {
	topo := testTopology(t)
	_, err := topo.toPlacement(nil)
	require.Equal(t, errTopologyIsNotOpenOrClosed, err)
}

func TestTopologyToPlacementNilValue(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyOpen
	_, err := topo.toPlacement(nil)
	require.Equal(t, errNilValue, err)
}

func TestTopologyToPlacementUnmarshalError(t *testing.T) {
	topo := testTopology(t)
	errTestUnmarshal := errors.New("unmarshal error")
	_, err := topo.toPlacement(&mockValue{unmarshalErr: errTestUnmarshal})
	require.Error(t, err)
}

func TestTopologyToPlacementSuccess(t *testing.T) {
	topo := testTopology(t)
	topo.state = topologyOpen
	topoVal, err := topo.serverOpts.TopologyOptions().KVStore().Get(testTopologyKey)
	require.NoError(t, err)
	p, err := topo.toPlacement(topoVal)
	require.NoError(t, err)
	pss := p.(*placementSnapshots)
	require.Equal(t, 1, pss.version)
	require.Equal(t, len(testPlacementSnapshots), len(pss.snapshots))
	for i := 0; i < len(testPlacementSnapshots); i++ {
		validateSnapshot(t, testPlacementSnapshots[i], pss.snapshots[i])
	}
}

func TestTopologyProcessClosed(t *testing.T) {
	topo := testTopology(t)
	require.Equal(t, errTopologyIsNotOpenOrClosed, topo.process(nil))
}

func TestTopologyProcessSuccess(t *testing.T) {
	var (
		allInstances [][]instance
		numCloses    int
	)
	writerMgr := &mockInstanceWriterManager{
		addInstancesFn: func(Instances []instance) error {
			allInstances = append(allInstances, Instances)
			return nil
		},
	}
	opts := newPlacementOptions().SetInstanceWriterManager(writerMgr)
	pss, err := newPlacementSnapshots(1, testPlacementSnapshotsSchema, opts)
	require.NoError(t, err)
	topo := testTopology(t)
	topo.state = topologyOpen
	topo.nowFn = func() time.Time { return time.Unix(0, 99999) }
	topo.placement = &mockPlacement{
		closeFn: func() error { numCloses++; return nil },
	}

	require.NoError(t, topo.process(pss))
	require.NotNil(t, topo.placement)
	require.Equal(t, [][]instance{testPlacementSnapshots[1].Instances()}, allInstances)
	require.Equal(t, 1, numCloses)
}

func testTopology(t *testing.T) *topo {
	store := mem.NewStore()
	_, err := store.SetIfNotExists(testTopologyKey, testPlacementSnapshotsSchema)
	require.NoError(t, err)

	clockOpts := clock.NewOptions()
	topoOpts := testTopologyOptions().SetKVStore(store)
	serverOpts := testServerOptions().
		SetClockOptions(clockOpts).
		SetTopologyOptions(topoOpts)

	writerMgr := newInstanceWriterManager(serverOpts)
	topology := newTopology(writerMgr, serverOpts)
	return topology.(*topo)
}

func testTopologyOptions() TopologyOptions {
	return NewTopologyOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetHashGenFn(func(numShards int) HashFn { return defaultHashGen(numShards) }).
		SetKVStore(mem.NewStore()).
		SetTopologyKey(testTopologyKey)
}

type routeFn func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error
type closeFn func() error

type mockPlacement struct {
	routeFn routeFn
	closeFn closeFn
}

func (mp *mockPlacement) Route(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
	return mp.routeFn(mu, pl)
}

func (mp *mockPlacement) Close() error { return mp.closeFn() }

type mockValue struct {
	version      int
	unmarshalErr error
}

func (mv *mockValue) Unmarshal(proto.Message) error { return mv.unmarshalErr }
func (mv *mockValue) Version() int                  { return mv.version }
func (mv *mockValue) IsNewer(other kv.Value) bool   { return mv.Version() > other.Version() }
