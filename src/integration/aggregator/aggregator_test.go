// +build cluster_integration
//
// Copyright (c) 2021  Uber Technologies, Inc.
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

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/inprocess"
)

func TestAggregator(t *testing.T) {
	m3, closer := testSetup(t)
	defer closer()

	RunTest(t, m3)
}

func testSetup(t *testing.T) (resources.M3Resources, func()) {
	cfgs, err := inprocess.NewClusterConfigsFromYAML(
		TestAggregatorDBNodeConfig, TestAggregatorCoordinatorConfig, TestAggregatorAggregatorConfig,
	)
	require.NoError(t, err)

	m3, err := inprocess.NewCluster(cfgs,
		resources.ClusterOptions{
			DBNode: resources.NewDBNodeClusterOptions(),
			Aggregator: &resources.AggregatorClusterOptions{
				RF:                 2,
				NumShards:          4,
				NumInstances:       2,
				NumIsolationGroups: 2,
			},
		},
	)
	require.NoError(t, err)

	return m3, func() {
		assert.NoError(t, m3.Cleanup())
	}
}
