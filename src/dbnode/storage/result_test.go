// Copyright (c) 2023 Databricks, Inc.
//

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	var a, b tickResult
	a.metricToCardinality = map[uint64]*metricCardinality{
		0: {name: []byte("a"), cardinality: 1},
		1: {name: []byte("b"), cardinality: 101},
	}
	b.metricToCardinality = map[uint64]*metricCardinality{
		1: {name: []byte("b"), cardinality: 1000},
		2: {name: []byte("c"), cardinality: 101},
	}
	a.merge(b, 3)
	require.Equal(t, 3, len(a.metricToCardinality))
	require.Equal(t, 2, len(b.metricToCardinality))
	require.Equal(t, 1, a.metricToCardinality[0].cardinality)
	require.Equal(t, 1101, a.metricToCardinality[1].cardinality)
	require.Equal(t, 101, a.metricToCardinality[2].cardinality)

	a.metricToCardinality = map[uint64]*metricCardinality{
		0: {name: []byte("a"), cardinality: 1},
		1: {name: []byte("b"), cardinality: 101},
	}
	require.Equal(t, 2, len(a.metricToCardinality))
	a.merge(b, 2)
	require.Equal(t, 2, len(a.metricToCardinality))
	require.Equal(t, 1101, a.metricToCardinality[1].cardinality)
	require.Equal(t, 101, a.metricToCardinality[2].cardinality)
}
