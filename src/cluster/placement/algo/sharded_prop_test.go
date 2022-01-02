// Copyright (c) 2021 Uber Technologies, Inc.
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

package algo

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"

	"github.com/m3db/m3/src/cluster/placement"
)

const minSuccessfulTests = 100

func TestInitialPlacementIsBalancedPropTest(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)

	parameters.MinSuccessfulTests = minSuccessfulTests
	parameters.Rng.Seed(seed)

	props.Property("Initial placement is balanced", prop.ForAll(
		testInitialPlacementIsBalanced,
		gen.IntRange(1, 3),
		gen.IntRange(1, 20),
		gen.IntRange(64, 3072),
	))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func testInitialPlacementIsBalanced(replicaCount, instanceCount, shardCount int) (bool, error) {
	instances := make([]placement.Instance, 0)
	for i := 0; i < replicaCount; i++ {
		for j := 0; j < instanceCount; j++ {
			var (
				instanceID     = fmt.Sprintf("instance-%d-%03d", i, j)
				isolationGroup = fmt.Sprintf("iso-%d", i)
				instance       = placement.NewEmptyInstance(instanceID, isolationGroup, "zone", "endpoint", 1)
			)
			instances = append(instances, instance)
		}
	}

	shardIDs := make([]uint32, shardCount)
	for i := range shardIDs {
		shardIDs[i] = uint32(i)
	}

	algo := newShardedAlgorithm(placement.NewOptions())
	p, err := algo.InitialPlacement(instances, shardIDs, replicaCount)
	if err != nil {
		return false, err
	}

	for _, shardID := range shardIDs {
		if n := len(p.InstancesForShard(shardID)); n != replicaCount {
			return false, fmt.Errorf("shard %d has %d replicas, but replication factor is %d", shardID, n, replicaCount)
		}
	}

	var (
		min = math.MaxInt32
		max = math.MinInt32
	)
	for _, instance := range p.Instances() {
		n := instance.Shards().NumShards()
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if max-min > 1 {
		return false, fmt.Errorf("shard count differs by more than 1, min=%v max=%v", min, max)
	}
	return true, nil
}
