package engine

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/querycontext"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlias(t *testing.T) {
	ctx := querycontext.NewTestContext()
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
	require.Nil(t, err)
	require.NotNil(t, results)
	require.Equal(t, len(series), results.Len())
	for _, s := range results.Values {
		assert.Equal(t, a, s.Name())
	}
}

func TestAliasSubWithNoBackReference(t *testing.T) {
	ctx := querycontext.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.dispatch.tcollector.TCollector--submit", now, values),
		ts.NewSeries(ctx, "stats.sjc1.counts.autobahn.production.hyperbahn15-sjc1.tchannel.inbound.calls.success.dispatch.onedirection.OneDirection--fittedMultiDriver", now, values),
		ts.NewSeries(ctx, "stats.sjc1.counts.autobahn.production.hyperbahn01-sjc1.tchannel.inbound.calls.success.dispatch.onedirection.OneDirection--fitted", now, values),
		ts.NewSeries(ctx, "stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.arbiter.tcollector.TCollector--submit", now, values),
		ts.NewSeries(ctx, "stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.arbiter", now, values), // doesn't match regex
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
		"stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.dispatch-TCollector--submit",
		"stats.sjc1.counts.autobahn.production.hyperbahn15-sjc1.tchannel.inbound.calls.dispatch-OneDirection--fittedMultiDriver",
		"stats.sjc1.counts.autobahn.production.hyperbahn01-sjc1.tchannel.inbound.calls.dispatch-OneDirection--fitted",
		"stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.arbiter-TCollector--submit",
		"stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.arbiter", // unchanged
	}, names)

	// Path expressions should remain unchanged
	assert.Equal(t, []string{
		"stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.dispatch.tcollector.TCollector--submit",
		"stats.sjc1.counts.autobahn.production.hyperbahn15-sjc1.tchannel.inbound.calls.success.dispatch.onedirection.OneDirection--fittedMultiDriver",
		"stats.sjc1.counts.autobahn.production.hyperbahn01-sjc1.tchannel.inbound.calls.success.dispatch.onedirection.OneDirection--fitted",
		"stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.arbiter.tcollector.TCollector--submit",
		"stats.sjc1.counts.autobahn.production.hyperbahn08-sjc1.tchannel.inbound.calls.success.arbiter",
	}, pathExpr)
}

func TestAliasSubWithBackReferences(t *testing.T) {
	ctx := querycontext.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := []float64{1.0, 2.0, 3.0, 4.0}

	input := struct {
		name     string
		pattern  string
		replace  string
		expected string
	}{
		"stats.sjc1.timers.scream.scream.views.record_trip.end_to_end_latency.p95",
		`stats.(.*).timers.\w+.scream.views.record_trip.(.*)`,
		`\1.\2`,
		"sjc1.end_to_end_latency.p95",
	}
	series := []*ts.Series{ts.NewSeries(ctx, input.name, now, querycontext.NewTestSeriesValues(ctx, 1000, values))}
	results, err := aliasSub(ctx, singlePathSpec{
		Values: series,
	}, input.pattern, input.replace)
	require.NoError(t, err)
	expected := []querycontext.TestSeries{
		querycontext.TestSeries{Name: input.expected, Data: values},
	}
	querycontext.CompareOutputsAndExpected(t, 1000, now, expected, results.Values)

	results, err = aliasSub(ctx, singlePathSpec{
		Values: series,
	}, input.pattern, `\1.\3`)
	require.Error(t, err)
	require.Nil(t, results.Values)
}

func TestAliasByMetric(t *testing.T) {
	ctx := querycontext.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	series := []*ts.Series{
		ts.NewSeries(ctx, "statsdex.metrics4.query.statsdex01-sjc1.writes.success", now, values),
		ts.NewSeries(ctx, "statsdex.metrics4.query.statsdex02-sjc1.writes.success.P99", now, values),
		ts.NewSeries(ctx, "statsdex.metrics4.query.statsdex03-sjc1.writes.success.P75", now, values),
		ts.NewSeries(ctx, "scale(stats.dca1.gauges.vertica.latency_minutes.foo, 60.123))", now, values),
	}

	results, err := aliasByMetric(ctx, singlePathSpec{
		Values: series,
	})
	require.Nil(t, err)
	require.NotNil(t, results)
	require.Equal(t, len(series), len(results.Values))
	assert.Equal(t, "success", results.Values[0].Name())
	assert.Equal(t, "P99", results.Values[1].Name())
	assert.Equal(t, "P75", results.Values[2].Name())
	assert.Equal(t, "foo", results.Values[3].Name())
}

func TestAliasByNode(t *testing.T) {
	ctx := querycontext.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	series := []*ts.Series{
		ts.NewSeries(ctx, "statsdex.metrics4.query.statsdex01-sjc1.writes.success", now, values),
		ts.NewSeries(ctx, "statsdex.metrics4.query.statsdex02-sjc1.writes.success.P99", now, values),
		ts.NewSeries(ctx, "statsdex.metrics4.query.statsdex03-sjc1.writes.success.P75", now, values),
	}

	results, err := aliasByNode(ctx, singlePathSpec{
		Values: series,
	}, 3, 5, 6)
	require.Nil(t, err)
	require.NotNil(t, results)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "statsdex01-sjc1.success", results.Values[0].Name())
	assert.Equal(t, "statsdex02-sjc1.success.P99", results.Values[1].Name())
	assert.Equal(t, "statsdex03-sjc1.success.P75", results.Values[2].Name())

	results, err = aliasByNode(nil, singlePathSpec{
		Values: series,
	}, -1)
	require.Nil(t, err)
	require.NotNil(t, results)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "success", results.Values[0].Name())
	assert.Equal(t, "P99", results.Values[1].Name())
	assert.Equal(t, "P75", results.Values[2].Name())
}

func TestAliasByNodeWithComposition(t *testing.T) {
	ctx := querycontext.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)
	series := []*ts.Series{
		ts.NewSeries(ctx, "derivative(servers.onedirection02-sjc1.cpu.load_5)", now, values),
		ts.NewSeries(ctx, "derivative(derivative(servers.onedirection02-sjc1.cpu.load_5))", now, values),
		ts.NewSeries(ctx, "~~~", now, values),
		ts.NewSeries(ctx, "", now, values),
	}
	results, err := aliasByNode(ctx, singlePathSpec{
		Values: series,
	}, 0, 1)
	require.Nil(t, err)
	require.NotNil(t, results)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "servers.onedirection02-sjc1", results.Values[0].Name())
	assert.Equal(t, "servers.onedirection02-sjc1", results.Values[1].Name())
	assert.Equal(t, "~~~", results.Values[2].Name())
	assert.Equal(t, "", results.Values[3].Name())
}
