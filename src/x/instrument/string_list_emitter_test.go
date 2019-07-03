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
)

var bootstrappers = [][]string{
	[]string{
		"filesystem",
		"peers",
		"commitlog",
		"uninitialized_topology",
	},
	[]string{
		"uninitialized_topology",
		"filesystem",
		"commitlog",
		"peers",
	},
	[]string{"", "", "", ""},
	[]string{},
}

func TestBootstrapGaugeEmitter(t *testing.T) {
	t.Run("TestNewBootstrapGaugeEmitter", func(t *testing.T) {
		scope := tally.NewTestScope("testScope", nil)
		for _, bs := range bootstrappers {

			bge := NewStringListEmitter(scope, "bootstrapper_bootstrappers", "bootstrapper")
			require.NotNil(t, bge)
			require.Nil(t, bge.gauge)

			g := bge.newGauge(scope, bs)
			require.NotNil(t, g)

			require.False(t, bge.running)

			err := bge.UpdateStringList(bs)
			require.Error(t, err)
			require.Nil(t, bge.gauge)

			err = bge.Close()
			require.Error(t, err)

			err = bge.Start(bs)
			require.NoError(t, err)
			require.NotNil(t, bge.gauge)
			require.True(t, bge.running)
			if !reflect.DeepEqual(g, bge.gauge) {
				t.Errorf("expected: %#v, got: %#v", g, bge.gauge)
			}

			err = bge.Close()
			require.NoError(t, err)
			require.False(t, bge.running)
		}
	})

	t.Run("TestEmitting", func(t *testing.T) {
		scope := tally.NewTestScope("testScope", nil)
		bs0 := bootstrappers[0]
		bge := NewStringListEmitter(scope, "bootstrapper_bootstrappers", "bootstrapper")
		require.NotNil(t, bge)
		require.Nil(t, bge.gauge)

		// Start
		err := bge.Start(bs0)
		require.NoError(t, err)
		require.True(t, bge.running)
		require.NotNil(t, bge.gauge)

		gauges := scope.Snapshot().Gauges()
		require.Equal(t, 1, len(gauges), "length of gauges after start")

		for _, id := range []string{
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			require.Equal(t, float64(1), g.Value(), "gauge value after start")
		}

		// Update to new
		bs1 := bootstrappers[1]
		err = bge.UpdateStringList(bs1)
		require.NoError(t, err)
		require.True(t, bge.running, "state of bootstrapGaugeEmitter after update")

		gauges = scope.Snapshot().Gauges()
		require.Equal(t, 2, len(gauges), "length of gauges after updating")
		for i, id := range []string{
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3]),
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs1[0], bs1[1], bs1[2], bs1[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			if i == 0 {
				require.Equal(t, float64(0), g.Value(), "first gauge value of after update")
			} else {
				require.Equal(t, float64(1), g.Value(), "second gauge value of after update")
			}
		}

		// Update back to old
		err = bge.UpdateStringList(bs0)
		require.NoError(t, err)
		require.True(t, bge.running, "state of bootstrapGaugeEmitter after update")

		gauges = scope.Snapshot().Gauges()
		require.Equal(t, 2, len(gauges), "length of gauges after updating")
		for i, id := range []string{
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3]),
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs1[0], bs1[1], bs1[2], bs1[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			if i == 0 {
				require.Equal(t, float64(1), g.Value(), "first gauge value of after update")
			} else {
				require.Equal(t, float64(0), g.Value(), "second gauge value of after update")
			}
		}

		// Update back to old again -- test emitting same metric twice
		err = bge.UpdateStringList(bs0)
		require.NoError(t, err)
		require.True(t, bge.running, "state of bootstrapGaugeEmitter after emitting same metric twice")

		gauges = scope.Snapshot().Gauges()
		require.Equal(t, 2, len(gauges), "length of gauges after updating")
		for i, id := range []string{
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3]),
			fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs1[0], bs1[1], bs1[2], bs1[3]),
		} {
			g, ok := gauges[id]
			require.True(t, ok)
			if i == 0 {
				require.Equal(t, float64(1), g.Value(), "first gauge value of after emitting same metric twice")
			} else {
				require.Equal(t, float64(0), g.Value(), "second gauge value of after emitting same metric twice")
			}
		}

		// Close
		err = bge.Close()
		require.NoError(t, err)
		require.False(t, bge.running)
	})
}
