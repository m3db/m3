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

package native

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlias(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "bender", now, values),
		ts.NewSeries(ctx, "fry", now, values),
		ts.NewSeries(ctx, "leela", now, values),
	}
	a := "farnsworth"

	results, err := alias(nil, singlePathSpec{
		Values: series,
	}, a)
	require.NoError(t, err)
	require.Equal(t, len(series), results.Len())
	for _, s := range results.Values {
		assert.Equal(t, a, s.Name())
	}
}

func TestAliasSubWithNoBackReference(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.quux.john.Frank--submit", now, values),
		ts.NewSeries(ctx, "stats.foo.counts.bar.baz.quail15-foo.qux.qaz.calls.success.quux.bob.BOB--nosub", now, values),
		ts.NewSeries(ctx, "stats.foo.counts.bar.baz.quail01-foo.qux.qaz.calls.success.quux.bob.BOB--woop", now, values),
		ts.NewSeries(ctx, "stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.quacks.john.Frank--submit", now, values),
		ts.NewSeries(ctx, "stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.arbiter", now, values), // doesn't match regex
	}

	results, err := aliasSub(ctx, singlePathSpec{
		Values: series,
	}, "success\\.([-_\\w]+)\\.([-_\\w]+)\\.([-_\\w]+)", "$1-$3")
	require.NoError(t, err)
	require.Equal(t, 5, results.Len())

	var names, pathExpr []string
	for _, s := range results.Values {
		names = append(names, s.Name())
		pathExpr = append(pathExpr, s.Specification)
	}

	assert.Equal(t, []string{
		"stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.quux-Frank--submit",
		"stats.foo.counts.bar.baz.quail15-foo.qux.qaz.calls.quux-BOB--nosub",
		"stats.foo.counts.bar.baz.quail01-foo.qux.qaz.calls.quux-BOB--woop",
		"stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.quacks-Frank--submit",
		"stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.arbiter", // unchanged
	}, names)

	// Path expressions should remain unchanged
	assert.Equal(t, []string{
		"stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.quux.john.Frank--submit",
		"stats.foo.counts.bar.baz.quail15-foo.qux.qaz.calls.success.quux.bob.BOB--nosub",
		"stats.foo.counts.bar.baz.quail01-foo.qux.qaz.calls.success.quux.bob.BOB--woop",
		"stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.quacks.john.Frank--submit",
		"stats.foo.counts.bar.baz.quail08-foo.qux.qaz.calls.success.arbiter",
	}, pathExpr)
}

func TestAliasSubWithBackReferences(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := []float64{1.0, 2.0, 3.0, 4.0}

	input := struct {
		name     string
		pattern  string
		replace  string
		expected string
	}{
		"stats.foo.timers.qaz.qaz.views.quail.end_to_end_latency.p95",
		`stats.(.*).timers.\w+.qaz.views.quail.(.*)`,
		`\1.\2`,
		"foo.end_to_end_latency.p95",
	}
	series := []*ts.Series{ts.NewSeries(ctx, input.name, now, common.NewTestSeriesValues(ctx, 1000, values))}
	results, err := aliasSub(ctx, singlePathSpec{
		Values: series,
	}, input.pattern, input.replace)
	require.NoError(t, err)
	expected := []common.TestSeries{
		{Name: input.expected, Data: values},
	}
	common.CompareOutputsAndExpected(t, 1000, now, expected, results.Values)

	results, err = aliasSub(ctx, singlePathSpec{
		Values: series,
	}, input.pattern, `\1.\3`)
	require.Error(t, err)
	require.Nil(t, results.Values)
}

func TestAliasByMetric(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	series := []*ts.Series{
		ts.NewSeries(ctx, "foo.bar.baz.foo01-foo.writes.success", now, values),
		ts.NewSeries(ctx, "foo.bar.baz.foo02-foo.writes.success.P99", now, values),
		ts.NewSeries(ctx, "foo.bar.baz.foo03-foo.writes.success.P75", now, values),
		ts.NewSeries(ctx, "scale(stats.foobar.gauges.quazqux.latency_minutes.foo, 60.123)", now, values),
	}

	results, err := aliasByMetric(ctx, singlePathSpec{
		Values: series,
	})
	require.NoError(t, err)
	require.Equal(t, len(series), len(results.Values))
	assert.Equal(t, "success", results.Values[0].Name())
	assert.Equal(t, "P99", results.Values[1].Name())
	assert.Equal(t, "P75", results.Values[2].Name())
	assert.Equal(t, "foo", results.Values[3].Name())
}

func TestAliasByNode(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	series := []*ts.Series{
		ts.NewSeries(ctx, "foo.bar.baz.foo01-foo.writes.success", now, values),
		ts.NewSeries(ctx, "foo.bar.baz.foo02-foo.writes.success.P99", now, values),
		ts.NewSeries(ctx, "foo.bar.baz.foo03-foo.writes.success.P75", now, values),
	}

	results, err := aliasByNode(ctx, singlePathSpec{
		Values: series,
	}, 3, 5, 6)
	require.NoError(t, err)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "foo01-foo.success", results.Values[0].Name())
	assert.Equal(t, "foo02-foo.success.P99", results.Values[1].Name())
	assert.Equal(t, "foo03-foo.success.P75", results.Values[2].Name())

	results, err = aliasByNode(nil, singlePathSpec{
		Values: series,
	}, -1)
	require.NoError(t, err)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "success", results.Values[0].Name())
	assert.Equal(t, "P99", results.Values[1].Name())
	assert.Equal(t, "P75", results.Values[2].Name())
}

func TestAliasByNodeWithComposition(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "derivative(servers.bob02-foo.cpu.load_5)", now, values),
		ts.NewSeries(ctx, "derivative(derivative(servers.bob02-foo.cpu.load_5))", now, values),
		ts.NewSeries(ctx, "~~~", now, values),
		ts.NewSeries(ctx, "", now, values),
	}
	results, err := aliasByNode(ctx, singlePathSpec{
		Values: series,
	}, 0, 1)
	require.NoError(t, err)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "servers.bob02-foo", results.Values[0].Name())
	assert.Equal(t, "servers.bob02-foo", results.Values[1].Name())
	assert.Equal(t, "~~~", results.Values[2].Name())
	assert.Equal(t, "", results.Values[3].Name())
}

func TestAliasByNodeWithManyPathExpressions(t *testing.T) {
	ctx := common.NewTestContext()
	defer func() { _ = ctx.Close() }()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "sumSeries(servers.bob02-foo.cpu.load_5,servers.bob01-foo.cpu.load_5)", now, values),
		ts.NewSeries(ctx, "sumSeries(sumSeries(servers.bob04-foo.cpu.load_5,servers.bob03-foo.cpu.load_5))", now, values),
	}
	results, err := aliasByNode(ctx, singlePathSpec{
		Values: series,
	}, 0, 1)
	require.NoError(t, err)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "servers.bob02-foo", results.Values[0].Name())
	assert.Equal(t, "servers.bob04-foo", results.Values[1].Name())
}

func TestAliasByNodeWitCallSubExpressions(t *testing.T) {
	ctx := common.NewTestContext()
	defer func() { _ = ctx.Close() }()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "asPercent(foo01,sumSeries(bar,baz))", now, values),
		ts.NewSeries(ctx, "asPercent(foo02,sumSeries(bar,baz))", now, values),
	}
	results, err := aliasByNode(ctx, singlePathSpec{
		Values: series,
	}, 0)
	require.NoError(t, err)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "foo01", results.Values[0].Name())
	assert.Equal(t, "foo02", results.Values[1].Name())
}
