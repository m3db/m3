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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3cluster/shard"
)

// The purpose of the unitializedSource is to succeed bootstraps for any
// shard/time-ranges if the cluster they're associated with has never
// been completely initialized (is a new cluster). This is required for
// allowing us to configure the bootstrappers such that the commitlog
// bootstrapper can precede the peers bootstrapper and still succeed bootstraps
// for brand new namespaces without permitting unintentional data loss by
// putting the noop-all or noop-none bootstrappers at the end of the process.
// Behavior is best understood by reading the test cases for the test:
// TestUnitializedSourceAvailableDataAndAvailableIndex
type uninitializedTopologySource struct {
	opts Options
}

// newTopologyUninitializedSource creates a new uninitialized source.
func newTopologyUninitializedSource(opts Options) bootstrap.Source {
	return &uninitializedTopologySource{
		opts: opts,
	}
}

func (s *uninitializedTopologySource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}

	return false
}

func (s *uninitializedTopologySource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

func (s *uninitializedTopologySource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

func (s *uninitializedTopologySource) availability(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
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
		// need to determine if the cluster is "new" in the sense that it has
		// never been completely initialized (reached a state where all the hosts
		// in the topology are "available" for that specific shard).
		// In order to determine this, we simply count the number of hosts in the
		// "initializing" state. If this number is larger than zero, than the
		// cluster is "new".
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
			case shard.Leaving:
				numLeaving++
			case shard.Available:
			case shard.Unknown:
				fallthrough
			default:
				// TODO(rartoul): Make this a hard error once we refactor the interface to support
				// returning errors.
				errMsg := fmt.Sprintf("unknown shard state: %v", shardState)
				s.opts.InstrumentOptions().Logger().Error(errMsg)
				return nil, errors.New(errMsg)
			}
		}

		// This heuristic works for all scenarios except for if we tried to change the replication
		// factor of a cluster that was already initialized. In that case, we might have to come
		// up with a new heuristic, or simply require that the peers bootstrapper be configured as
		// a bootstrapper if users want to change the replication factor dynamically, which is fine
		// because otherwise you'd have to wait for one entire retention period for the replicaiton
		// factor to actually increase correctly.
		shardHasNeverBeenCompletelyInitialized := numInitializing-numLeaving > 0
		if shardHasNeverBeenCompletelyInitialized {
			availableShardTimeRanges[shardIDUint] = shardsTimeRanges[shardIDUint]
		}
	}

	return availableShardTimeRanges, nil
}

func (s *uninitializedTopologySource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	availability, err := s.availability(ns, shardsTimeRanges, runOpts)
	if err != nil {
		return nil, err
	}

	missing := shardsTimeRanges.Copy()
	missing.Subtract(availability)

	if missing.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	return missing.ToUnfulfilledDataResult(), nil
}

func (s *uninitializedTopologySource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	availability, err := s.availability(ns, shardsTimeRanges, runOpts)
	if err != nil {
		return nil, err
	}

	missing := shardsTimeRanges.Copy()
	missing.Subtract(availability)

	if missing.IsEmpty() {
		return result.NewIndexBootstrapResult(), nil
	}

	return missing.ToUnfulfilledIndexResult(), nil
}
