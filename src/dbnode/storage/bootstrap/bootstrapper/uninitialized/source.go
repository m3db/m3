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

package uninitialized

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3cluster/shard"
)

// The purpose of the unitializedSource is to succeed bootstraps for any
// shard/time-ranges if the given shard/namespace combination has never
// been completely initialized (is a new namespace). This is required for
// allowing us to configure the bootstrappers such that the commitlog
// bootstrapper can precede the peers bootstrapper and still suceed bootstraps
// for brand new namespaces without permitting unintentional data loss by
// putting the noop-all or noop-none bootstrappers at the end of the process.
// Behavior is best understood by reading the test cases for the test:
// TestUnitializedSourceAvailableDataAndAvailableIndex
type uninitializedSource struct {
	opts Options
}

// NewUninitializedSource creates a new uninitialized source.
func NewUninitializedSource(opts Options) (bootstrap.Source, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &uninitializedSource{
		opts: opts,
	}, nil
}

func (s *uninitializedSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}

	return false
}

func (s *uninitializedSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) result.ShardTimeRanges {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

func (s *uninitializedSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) result.ShardTimeRanges {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

func (s *uninitializedSource) availability(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) result.ShardTimeRanges {
	var (
		topoState                = runOpts.InitialTopologyState()
		availableShardTimeRanges = result.ShardTimeRanges{}
	)

	for shardIDUint := range shardsTimeRanges {
		shardID := topology.ShardID(shardIDUint)
		hostShardStates, ok := topoState.ShardStates[shardID]
		if !ok {
			// This shard was not part of the topology when the bootstrapping
			// process began.
			continue
		}

		// The basic idea for the algorithm is that on a shard-by-shard basis we
		// need to determine if the namespace is "new" in the sense that it has
		// never been completely initialized (reached a state where all the hosts
		// in the topology are "available").
		// In order to determine this, we simply count the number of hosts in the
		// "initializing" state. If this number is larger than zero, than the
		// namespace is "new".
		// The one exception to this case is when we perform topology changes and
		// we end up with one extra node that is initializing which should be offset
		// by the corresponding node that is leaving. I.E if numInitializing > 0
		// BUT numLeaving >= numInitializing then it is still not a new namespace.
		// See the TestUnitializedSourceAvailableDataAndAvailableIndex test for more details.
		var (
			numInitializing = 0
			numLeaving      = 0
		)
		for _, hostState := range hostShardStates {
			shardState := hostState.ShardState
			switch shardState {
			case shard.Initializing:
				numInitializing++
			case shard.Unknown:
				// TODO: Right now we ignore unknown shards (which biases us towards
				// failing the bootstrap). Once this interface supports returning errors,
				// we should return an error in this case because the cluster is in a bad
				// state and the operator should make a decision on how they want to proceed.
			case shard.Leaving:
				numLeaving++
			case shard.Available:
			default:
				// TODO(rartoul): Make this a hard error once we refactor the interface to support
				// returning errors.
				panic(fmt.Sprintf("unknown shard state: %v", shardState))
			}
		}

		shardHasEverBeenCompletelyInitialized := numInitializing-numLeaving > 0
		if shardHasEverBeenCompletelyInitialized {
			availableShardTimeRanges[shardIDUint] = shardsTimeRanges[shardIDUint]
		}
	}

	return availableShardTimeRanges
}

func (s *uninitializedSource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	return result.NewDataBootstrapResult(), nil
}

func (s *uninitializedSource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	return result.NewIndexBootstrapResult(), nil
}
