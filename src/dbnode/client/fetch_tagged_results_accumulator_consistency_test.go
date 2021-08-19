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

package client

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
)

var (
	testFetchTaggedSuccessResponse = rpc.FetchTaggedResult_{}
	errTestFetchTagged             = fmt.Errorf("random error")
)

func TestFetchTaggedResultsAccumulatorAnyResponseShouldTerminateConsistencyLevelOneSimpleTopo(t *testing.T) {
	// rf=3, 30 shards total; three identical hosts
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(0, 29, shard.Available),
	})

	// any response should satisfy consistency lvl one
	for i := 0; i < 3; i++ {
		testFetchStateWorkflow{
			t:       t,
			topoMap: topoMap,
			level:   topology.ReadConsistencyLevelOne,
			steps: []testFetchStateWorklowStep{
				{
					hostname:          fmt.Sprintf("testhost%d", i),
					fetchTaggedResult: &testFetchTaggedSuccessResponse,
					expectedDone:      true,
				},
			},
		}.run()
	}

	// should terminate only after all failures, and say it failed
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelOne,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorShardAvailabilityIsEnforced(t *testing.T) {
	// rf=3, 30 shards total; three identical hosts
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Initializing),
		"testhost2": tu.ShardsRange(0, 29, shard.Available),
	})

	// responses from testhost1 should not count towards success
	// for consistency level 1
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelOne,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      false,
			},
		},
	}.run()

	// for consistency level unstrict majority
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost2",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()

	// for consistency level majority
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()

	// for consistency level unstrict all
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictAll,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    false,
			},
		},
	}.run()
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictAll,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    false,
			},
		},
	}.run()
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictAll,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost2",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()

	// for consistency level all
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelAll,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost0",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      true,
				expectedErr:       true,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorAnyResponseShouldTerminateConsistencyLevelOneComplexTopo(t *testing.T) {
	// rf=3, 30 shards total; 2 identical hosts, one additional host with a subset of all shards
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(10, 20, shard.Available),
	})

	// a single response from a host with partial shards isn't enough
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelOne,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      false,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorConsistencyUnstrictMajority(t *testing.T) {
	// rf=3, 30 shards total; three identical hosts
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(0, 29, shard.Available),
	})

	// two success responses should succeed immediately
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost0",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      false,
			},
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      true,
			},
		},
	}.run()

	// two failures, and one success response should succeed
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      true,
			},
		},
	}.run()

	// should terminate only after all failures
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
				expectedErr:    true,
				expectedDone:   true,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorConsistencyUnstrictMajorityComplexTopo(t *testing.T) {
	// rf=3, 30 shards total; three identical hosts
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Initializing),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(0, 29, shard.Available),
		"testhost3": tu.ShardsRange(0, 29, shard.Leaving),
	})

	// one success responses should succeed
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost0",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost2",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:          "testhost3",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
				expectedDone:      true,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorComplextTopoUnstrictMajorityPartialResponses(t *testing.T) {
	// rf=3, 30 shards total; 2 identical "complete hosts", 2 additional hosts which together comprise a "complete" host.
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(15, 29, shard.Available),
		"testhost3": tu.ShardsRange(0, 14, shard.Available),
	})

	// response from testhost2+testhost3 should be sufficient
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost3",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorComplexIncompleteTopoUnstrictMajorityPartialResponses(t *testing.T) {
	// rf=3, 30 shards total; 2 identical "complete hosts", 2 additional hosts which do not comprise a complete host.
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(15, 27, shard.Available),
		"testhost3": tu.ShardsRange(0, 14, shard.Available),
	})

	// response from testhost2+testhost3 should be in-sufficient, as they're not complete together
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelUnstrictMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:          "testhost2",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:          "testhost3",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()
}

func TestFetchTaggedResultsAccumulatorReadConsitencyLevelMajority(t *testing.T) {
	// rf=3, 30 shards total; three identical hosts
	topoMap := tu.MustNewTopologyMap(3, map[string][]shard.Shard{
		"testhost0": tu.ShardsRange(0, 29, shard.Available),
		"testhost1": tu.ShardsRange(0, 29, shard.Available),
		"testhost2": tu.ShardsRange(0, 29, shard.Available),
	})

	// any single success response should not satisfy consistency majority
	for i := 0; i < 3; i++ {
		testFetchStateWorkflow{
			t:       t,
			topoMap: topoMap,
			level:   topology.ReadConsistencyLevelMajority,
			steps: []testFetchStateWorklowStep{
				{
					hostname:          fmt.Sprintf("testhost%d", i),
					fetchTaggedResult: &testFetchTaggedSuccessResponse,
					expectedDone:      false,
				},
			},
		}.run()
	}

	// all responses failing should fail consistency lvl majority
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost1",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:       "testhost2",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()

	// any two responses failing should fail regardless of third response
	testFetchStateWorkflow{
		t:       t,
		topoMap: topoMap,
		level:   topology.ReadConsistencyLevelMajority,
		steps: []testFetchStateWorklowStep{
			{
				hostname:       "testhost0",
				fetchTaggedErr: errTestFetchTagged,
			},
			{
				hostname:          "testhost1",
				fetchTaggedResult: &testFetchTaggedSuccessResponse,
			},
			{
				hostname:       "testhost2",
				fetchTaggedErr: errTestFetchTagged,
				expectedDone:   true,
				expectedErr:    true,
			},
		},
	}.run()
}
