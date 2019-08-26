// Copyright (c) 2019 Uber Technologies, Inc.
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

package instrument

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
)

var bootstrappers = [][]string{
	{
		"filesystem",
		"peers",
		"commitlog",
		"uninitialized_topology",
	},
	{
		"uninitialized_topology",
		"filesystem",
		"peers",
		"commitlog",
	},
	{
		"a",
		"b",
		"c",
		"d",
	},
	{
		"foo",
		"bar",
		"hello",
		"world",
		"more",
		"labels",
	},
	{
		"x",
		"y",
	},
	{
		"foo",
	},
	{"", "", "", ""},
	{},
}

func TestBootstrapGaugeEmitterNoopScope(t *testing.T) {
	scope := tally.NewTestScope("testScope", nil)
	t.Run("TestNewBootstrapGaugeEmitter", func(t *testing.T) {
		for _, bs := range bootstrappers {

			bge := NewStringListEmitter(scope, "bootstrappers")
			require.NotNil(t, bge)
			require.NotNil(t, bge.gauges)
			require.Equal(t, 1, len(bge.gauges))

			g := bge.newGauges(scope, bs)
			require.NotNil(t, g)
			require.Equal(t, len(bs), len(g))

			require.False(t, bge.running)

			err := bge.UpdateStringList(bs)
			require.Error(t, err)
			require.NotNil(t, bge.gauges)
			require.Equal(t, 1, len(bge.gauges))

			err = bge.Close()
			require.Error(t, err)

			err = bge.Start(bs)
			require.NoError(t, err)
			require.NotNil(t, bge.gauges)
			require.True(t, bge.running)
			require.Equal(t, len(bs), len(bge.gauges))
			if !reflect.DeepEqual(g, bge.gauges) {
				t.Errorf("expected: %#v, got: %#v", g, bge.gauges)
			}

			err = bge.Close()
			require.NoError(t, err)
			require.False(t, bge.running)
		}
	})

	t.Run("TestEmitting", func(t *testing.T) {
		scope := tally.NewTestScope("testScope", nil)
		bs0 := bootstrappers[0]
		bge := NewStringListEmitter(scope, "bootstrappers")
		require.NotNil(t, bge)
		require.NotNil(t, bge.gauges)
		require.Equal(t, len(bge.gauges), 1)

		// Start
		err := bge.Start(bs0)
		require.NoError(t, err)
		require.True(t, bge.running)
		require.NotNil(t, bge.gauges)
		require.Equal(t, len(bge.gauges), len(bs0))

		gauges := scope.Snapshot().Gauges()
		require.Equal(t, len(bs0), len(gauges), "length of gauges after start")

		for _, id := range []string{
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs0[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs0[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs0[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs0[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			require.Equal(t, float64(1), g.Value(), "gauge value after start")
		}

		// 	// Update to new
		bs1 := bootstrappers[1]
		err = bge.UpdateStringList(bs1)
		require.NoError(t, err)
		require.True(t, bge.running, "state of bootstrapGaugeEmitter after update")
		require.NotNil(t, bge.gauges)
		require.Equal(t, len(bge.gauges), len(bs1))

		gauges = scope.Snapshot().Gauges()
		require.Equal(t, 8, len(gauges), "length of gauges after updating")
		for i, id := range []string{
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs0[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs0[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs0[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs0[3]),
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs1[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs1[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs1[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs1[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			if i < 4 {
				require.Equal(t, float64(0), g.Value(), "first gauge value of after update")
			} else {
				require.Equal(t, float64(1), g.Value(), "second gauge value of after update")
			}
		}

		// Update back to old
		err = bge.UpdateStringList(bs0)
		require.NoError(t, err)
		require.True(t, bge.running, "state of bootstrapGaugeEmitter after update")
		require.NotNil(t, bge.gauges)
		require.Equal(t, len(bge.gauges), len(bs0))

		gauges = scope.Snapshot().Gauges()
		require.Equal(t, 8, len(gauges), "length of gauges after updating")
		for i, id := range []string{
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs0[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs0[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs0[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs0[3]),
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs1[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs1[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs1[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs1[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			if i < 4 {
				require.Equal(t, float64(1), g.Value(), "first gauge value of after update")
			} else {
				require.Equal(t, float64(0), g.Value(), "second gauge value of after update")
			}
		}

		// Update back to old again -- test emitting same metric twice
		err = bge.UpdateStringList(bs0)
		require.NoError(t, err)
		require.True(t, bge.running, "state of bootstrapGaugeEmitter after update")
		require.NotNil(t, bge.gauges)
		require.Equal(t, len(bge.gauges), len(bs0))

		gauges = scope.Snapshot().Gauges()
		require.Equal(t, 8, len(gauges), "length of gauges after updating")
		for i, id := range []string{
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs0[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs0[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs0[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs0[3]),
			fmt.Sprintf("testScope.bootstrappers_0+type=%s", bs1[0]),
			fmt.Sprintf("testScope.bootstrappers_1+type=%s", bs1[1]),
			fmt.Sprintf("testScope.bootstrappers_2+type=%s", bs1[2]),
			fmt.Sprintf("testScope.bootstrappers_3+type=%s", bs1[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			if i < 4 {
				require.Equal(t, float64(1), g.Value(), "first gauge value of after update")
			} else {
				require.Equal(t, float64(0), g.Value(), "second gauge value of after update")
			}
		}

		// Close
		err = bge.Close()
		require.NoError(t, err)
		require.False(t, bge.running)
	})

	t.Run("MultipleLabelCombinations", func(t *testing.T) {
		bge := NewStringListEmitter(scope, "bootstrappers")
		require.NotNil(t, bge)
		require.NotNil(t, bge.gauges)

		err := bge.Start(bootstrappers[0])
		require.NoError(t, err)
		require.True(t, bge.running)
		require.NotNil(t, bge.gauges)

		// Update same Gauge with standard bootstrappers
		for _, bs := range bootstrappers {
			err = bge.UpdateStringList(bs)
			require.NoError(t, err)
			require.True(t, bge.running)
			require.NotNil(t, bge.gauges)
		}

		err = bge.Close()
		require.NoError(t, err)
		require.False(t, bge.running)
	})
}

// This test might timeout instead of just fail
func TestStringListEmitterPrometheusScope(t *testing.T) {
	r := promreporter.NewReporter(promreporter.Options{})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:         "",
		Tags:           map[string]string{},
		CachedReporter: r,
	}, 0)
	defer closer.Close()

	bge := NewStringListEmitter(scope, "bootstrappers")
	require.NotNil(t, bge)
	require.NotNil(t, bge.gauges)

	err := bge.Start(bootstrappers[0])
	require.NoError(t, err)
	require.True(t, bge.running)
	require.NotNil(t, bge.gauges)

	for _, bs := range bootstrappers {
		err = bge.UpdateStringList(bs)
		require.NoError(t, err)
		require.True(t, bge.running)
		require.NotNil(t, bge.gauges)
	}

	err = bge.Close()
	require.NoError(t, err)
	require.False(t, bge.running)
}
