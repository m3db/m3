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

// Package repair contains integration tests for repair and replication among dbnodes.
package repair

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/integration/resources"
)

const (
	// TestRepairDBNodeConfig is the test config for the dbnode.
	TestRepairDBNodeConfig = `
db:
  repair:
    enabled: true
    throttle: 1ms
    checkInterval: 1ms
  replication:
    clusters:
      - name: "the_other_cluster"
        repairEnabled: true
        client:
          config:
            service:
              env: default_env
              zone: embedded
              service: m3db
              cacheDir: /var/lib/m3kv
              etcdClusters:
                - zone: embedded
                  endpoints:
                    - the_other_cluster_node:2379
`

	// TestRepairCoordinatorConfig is the test config for the coordinator.
	TestRepairCoordinatorConfig = `
clusters:
  - namespaces:
      - namespace: coldWritesRepairAndNoIndex
        type: unaggregated
        retention: 4h
`
)

// RunTest contains the logic for running the repair and replication test.
func RunTest(t *testing.T, cluster1, cluster2 resources.M3Resources) {
	var (
		namespace = "coldWritesRepairAndNoIndex"
		id        = "foo"
	)
	// write data for 'now - 2 * blockSize' to node 0 in cluster1
	require.NoError(t, writeData(
		namespace,
		id,
		time.Now().Add(-2*time.Hour),
		12.3456789,
		cluster1.Nodes()[0],
	))

	// verify the datapoint is present in the node
	results, err := fetchData(
		namespace,
		id,
		cluster1.Nodes()[0],
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))

	// test repair
	require.NoError(t, resources.Retry(verifyDataPoint(cluster1.Nodes()[1], namespace, id)))

	// test replication
	require.NoError(t, resources.Retry(verifyDataPoint(cluster2.Nodes()[0], namespace, id)))
	require.NoError(t, resources.Retry(verifyDataPoint(cluster2.Nodes()[1], namespace, id)))
}

func verifyDataPoint(dbnode resources.Node, namespace, id string) func() error {
	return func() error {
		dp, err := fetchData(namespace, id, dbnode)
		if err != nil {
			return err
		}
		if len(dp) == 0 {
			return errors.New("no datapoints")
		}
		return nil
	}
}

func writeData(namespace, id string, ts time.Time, value float64, dbnode resources.Node) error {
	return dbnode.WritePoint(&rpc.WriteRequest{
		NameSpace: namespace,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:         ts.UnixNano(),
			TimestampTimeType: rpc.TimeType_UNIX_NANOSECONDS,
			Value:             value,
		},
	})
}

func fetchData(namespace, id string, dbnode resources.Node) ([]*rpc.Datapoint, error) {
	result, err := dbnode.Fetch(&rpc.FetchRequest{
		NameSpace:  namespace,
		ID:         id,
		RangeStart: 0,
		RangeEnd:   time.Now().UnixNano(),
		RangeType:  rpc.TimeType_UNIX_NANOSECONDS,
	})
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, nil
	}

	return result.Datapoints, err
}
