//go:build big
// +build big

//
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
	"math"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

const maxNumHostsPropTest = 10

func TestFetchTaggedResultsAccumulatorGenerativeConsistencyCheck(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 40
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	parameters.Workers = 1
	properties := gopter.NewProperties(parameters)

	properties.Property("accumulator handles read consistency level correctly", prop.ForAll(
		func(input topoAndHostResponses, lvl topology.ReadConsistencyLevel) *gopter.PropResult {
			topoMap := input.topoMap
			success := input.success
			numHosts := topoMap.HostsLen()
			if numHosts > len(success) {
				panic("invalid test setup")
			}

			situationMeetsLevel := input.meetsConsistencyCheckForLevel(t, lvl)

			accum := newFetchTaggedResultAccumulator()
			majority := topoMap.MajorityReplicas()
			accum.Clear()
			accum.Reset(testStartTime, testEndTime, topoMap, majority, lvl)
			var (
				done bool
				err  error
			)
			for idx, host := range topoMap.Hosts() {
				var hostErr error
				opts := fetchTaggedResultAccumulatorOpts{host: host}
				if success[idx] {
					opts.response = &rpc.FetchTaggedResult_{}
				} else {
					hostErr = fmt.Errorf("random err")
				}
				done, err = accum.AddFetchTaggedResponse(opts, hostErr)
				if done {
					break
				}
			}
			require.True(t, done)

			if situationMeetsLevel {
				if err != nil {
					// something's fked
					return &gopter.PropResult{
						Error:  fmt.Errorf("situationMeetsLvl but err=%v", err),
						Status: gopter.PropError,
					}
				}
				return &gopter.PropResult{Status: gopter.PropTrue}
			}
			// i.e. !situationMeetsLevel
			if err == nil {
				return &gopter.PropResult{
					Error:  fmt.Errorf("!situationMeetsLevel && err == nil"),
					Status: gopter.PropError,
				}
			}
			return &gopter.PropResult{Status: gopter.PropTrue}
		},
		genTopologyAndResponse(t),
		gen.OneConstOf(
			topology.ReadConsistencyLevelAll,
			topology.ReadConsistencyLevelMajority,
			topology.ReadConsistencyLevelOne,
			topology.ReadConsistencyLevelUnstrictMajority,
			topology.ReadConsistencyLevelUnstrictAll,
		),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestAggregateResultsAccumulatorGenerativeConsistencyCheck(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 40
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	parameters.Workers = 1
	properties := gopter.NewProperties(parameters)

	properties.Property("accumulator handles read consistency level correctly", prop.ForAll(
		func(input topoAndHostResponses, lvl topology.ReadConsistencyLevel) *gopter.PropResult {
			topoMap := input.topoMap
			success := input.success
			numHosts := topoMap.HostsLen()
			if numHosts > len(success) {
				panic("invalid test setup")
			}

			situationMeetsLevel := input.meetsConsistencyCheckForLevel(t, lvl)

			accum := newFetchTaggedResultAccumulator()
			majority := topoMap.MajorityReplicas()
			accum.Clear()
			accum.Reset(testStartTime, testEndTime, topoMap, majority, lvl)
			var (
				done bool
				err  error
			)
			for idx, host := range topoMap.Hosts() {
				var hostErr error
				opts := aggregateResultAccumulatorOpts{host: host}
				if success[idx] {
					opts.response = &rpc.AggregateQueryRawResult_{}
				} else {
					hostErr = fmt.Errorf("random err")
				}
				done, err = accum.AddAggregateResponse(opts, hostErr)
				if done {
					break
				}
			}
			require.True(t, done)

			if situationMeetsLevel {
				if err != nil {
					// something's fked
					return &gopter.PropResult{
						Error:  fmt.Errorf("situationMeetsLvl but err=%v", err),
						Status: gopter.PropError,
					}
				}
				return &gopter.PropResult{Status: gopter.PropTrue}
			}
			// i.e. !situationMeetsLevel
			if err == nil {
				return &gopter.PropResult{
					Error:  fmt.Errorf("!situationMeetsLevel && err == nil"),
					Status: gopter.PropError,
				}
			}
			return &gopter.PropResult{Status: gopter.PropTrue}
		},
		genTopologyAndResponse(t),
		gen.OneConstOf(
			topology.ReadConsistencyLevelAll,
			topology.ReadConsistencyLevelMajority,
			topology.ReadConsistencyLevelOne,
			topology.ReadConsistencyLevelUnstrictMajority,
			topology.ReadConsistencyLevelUnstrictAll,
		),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

type topoAndHostResponses struct {
	topoMap topology.Map
	success []bool
}

func (res topoAndHostResponses) meetsConsistencyCheckForLevel(
	t *testing.T,
	lvl topology.ReadConsistencyLevel,
) bool {
	topoMap := res.topoMap
	rf, majority, numHosts := topoMap.Replicas(), topoMap.MajorityReplicas(), topoMap.HostsLen()
	successes := res.success

	var requiredNumSuccessResponsePerShard int
	switch lvl {
	case topology.ReadConsistencyLevelAll:
		requiredNumSuccessResponsePerShard = rf
	case topology.ReadConsistencyLevelMajority:
		requiredNumSuccessResponsePerShard = majority
	case topology.ReadConsistencyLevelUnstrictMajority:
		fallthrough
	case topology.ReadConsistencyLevelUnstrictAll:
		fallthrough
	case topology.ReadConsistencyLevelOne:
		requiredNumSuccessResponsePerShard = 1
	}

	// map from hostID -> idx in the hosts array
	hostsIdxMap := make(map[string]int, numHosts)
	for idx, h := range topoMap.Hosts() {
		hostsIdxMap[h.ID()] = idx
	}

	// map from shard -> numSuccess responses
	shardResponses := make(map[uint32]int)
	shards := topoMap.ShardSet()
	for _, s := range shards.AllIDs() {
		shardResponses[s] = 0
		hosts, err := topoMap.RouteShard(s)
		require.NoError(t, err)
		for _, h := range hosts {
			idx, ok := hostsIdxMap[h.ID()]
			require.True(t, ok)
			if successes[idx] {
				shardResponses[s]++
			}
		}
	}

	// ensure all shards have at least `requiredNumSuccessResponsePerShard` responses
	for _, num := range shardResponses {
		if num < requiredNumSuccessResponsePerShard {
			return false
		}
	}

	return true
}

func genTopologyAndResponse(t *testing.T) gopter.Gen {
	return gopter.CombineGens(
		genTopology(t),
		gen.SliceOfN(maxNumHostsPropTest, gen.Bool()),
	).Map(func(values []interface{}) topoAndHostResponses {
		return topoAndHostResponses{
			topoMap: values[0].(topology.Map),
			success: values[1].([]bool),
		}
	})
}

func genTopology(t *testing.T) gopter.Gen {
	var dummyMap topology.Map
	divceil := func(i, j int) int { return int(math.Ceil(float64(i) / float64(j))) }
	hostid := func(i int) string { return fmt.Sprintf("host%d", i) }
	dummyValue := gopter.NewEmptyResult(reflect.TypeOf(dummyMap))
	return func(params *gopter.GenParameters) *gopter.GenResult {
		// gopter hackery to chain generators. can't use Map/FlatMap/MapResult because the
		// composing generator still needs access to the gen.Parameters, and this way, we
		// can re-use the shrinkers from native types, which is massively useful.
		// NB: this isn't thread-safe because it generates data in a part of the lifecycle
		// where gopter workers aren't typically supposed to, so we have to set workers == 1
		// otherwise we see races in the Rng.
		genTopoParamsResult := genTopoGenParam()(params)
		genTopoParamsRes, ok := genTopoParamsResult.Retrieve()
		if !ok {
			return dummyValue
		}
		genTopoParams, ok := genTopoParamsRes.(topoGenParams)
		if !ok {
			return dummyValue
		}

		var (
			rf        = genTopoParams.rf
			numHosts  = genTopoParams.numHosts
			numShards = genTopoParams.numShards
		)

		// create a slice of all shards, i.e. [0, numShards) * rf
		numTotalShards := rf * numShards
		allShards := make([]uint32, 0, numTotalShards)
		for i := 0; i < rf; i++ {
			for j := 0; j < numShards; j++ {
				allShards = append(allShards, uint32(j))
			}
		}
		// yates shuffle the slice to get a random permutation
		shuffleSlice(allShards).shuffle(params.Rng)

		// initialize host -> shard map
		numShardsPerHost := divceil(numTotalShards, numHosts)
		hostShardAssignment := make(map[string]map[uint32]struct{}, numHosts)
		for i := 0; i < numHosts; i++ {
			hostShardAssignment[hostid(i)] = make(map[uint32]struct{}, numShardsPerHost)
		}

		// iterate through shuffled array and assign upto numShardsPerHost shards
		// to each host, ensuring each shard goes to a host at most one time.
		host := 0
		for len(allShards) > 0 {
			shard := allShards[0]
			hostn := hostid(host)
			host = (host + 1) % numHosts

			// i.e host has already received too many shards, skip it
			if len(hostShardAssignment[hostn]) > numShardsPerHost {
				continue
			}

			hss, ok := hostShardAssignment[hostn]
			require.True(t, ok)
			// skip if host already has the current shard
			if _, ok := hss[shard]; ok {
				continue
			}

			// all good, we can assign the shard to the host
			hss[shard] = struct{}{}
			allShards = allShards[1:]
		}

		// create topology map parameters
		assign := make(map[string][]shard.Shard, len(hostShardAssignment))
		for hostid, shardset := range hostShardAssignment {
			shards := make([]shard.Shard, 0, len(shardset))
			for id := range shardset {
				shards = append(shards, shard.NewShard(id).SetState(shard.Available))
			}
			assign[hostid] = shards
		}

		// finally create topology and return
		topoMap := testutil.MustNewTopologyMap(rf, assign)
		return gopter.NewGenResult(topoMap, genTopoParamsResult.Shrinker)
	}
}

type topoGenParams struct {
	rf        int
	numHosts  int
	numShards int
}

func genTopoGenParam() gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(1, 3),                   // RF
		gen.IntRange(3, maxNumHostsPropTest), // Num Hosts
		gen.IntRange(7, 30),                  // Num Shards
	).Map(func(values []interface{}) topoGenParams {
		return topoGenParams{
			rf:        values[0].(int),
			numHosts:  values[1].(int),
			numShards: values[2].(int),
		}
	})
}

type shuffleSlice []uint32

func (s shuffleSlice) shuffle(rng *rand.Rand) {
	// Start from the last element and swap one by one.
	// NB: We don't need to run for the first element that's why i > 0
	for i := len(s) - 1; i > 0; i-- {
		j := rng.Intn(i)
		s[i], s[j] = s[j], s[i]
	}
}
