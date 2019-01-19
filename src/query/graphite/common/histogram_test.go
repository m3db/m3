package common

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	histogramStepSize = 10

	testBucketID  = "bucket id thing"
	testBucketKey = "bucket key thing"
)

var hti = histogramTagInfo{testBucketID, testBucketKey}

func TestParseHistogramLabel(t *testing.T) {
	histogramTests := [...]struct {
		input          string
		wantParseError string
		left, right    string
	}{

		{
			input:          "0-infinity",
			wantParseError: "cannot draw a distribution of 0-infinity",
		},
		{
			input:          "1",
			wantParseError: "cannot split the label",
		},

		{
			input:          "1ms",
			wantParseError: "cannot split the label",
		},
		{
			input:          "-",
			wantParseError: "cannot split the label",
		},
		{
			input:          "2-",
			wantParseError: "cannot split the label",
		},
		{
			input:          "-2-",
			wantParseError: "cannot split the label",
		},
		{
			input:          "1ms--2ms",
			wantParseError: "invalid range",
		},
		{
			input:          "1--2",
			wantParseError: "invalid range",
		},
		{
			input: "1-2",
			left:  "1", right: "2",
		},
		{
			input: "1ms-2ms",
			left:  "1ms", right: "2ms",
		},
		{
			input: "-1-2",
			left:  "-1", right: "2",
		},
		{
			input: "-3--2",
			left:  "-3", right: "-2",
		},
		{
			input: "-infinity-2",
			left:  "-infinity", right: "2",
		},
		{
			input: "5-infinity",
			left:  "5", right: "infinity",
		},
	}
	ctx := NewTestContext()
	defer ctx.Close()
	for _, tt := range histogramTests {
		t.Run(tt.input, func(t *testing.T) {
			left, right, err := parseBucket(tt.input)
			if tt.wantParseError != "" {
				require.Error(t, err, "input: %q should be %q", tt.input, tt.wantParseError)
				assert.Contains(t, err.Error(), tt.wantParseError, "input: %q should be %q", tt.input, tt.wantParseError)

				s := ts.NewSeries(ctx, "test.series.0", ctx.StartTime, NewTestSeriesValues(ctx, 10, []float64{0, 1}))
				s.Tags = map[string]string{testBucketID: "0", testBucketKey: tt.input}
				_, err := newHistogramSeries(s, hti)
				assert.Error(t, err, "failed parsing should fail to create a series")
				return
			}
			assert.NoError(t, err, "unexpected error for %q", tt.input)
			assert.Equal(t, tt.left, left, "unexpected value for %q", tt.input)
			assert.Equal(t, tt.right, right, "unexpected value for %q", tt.input)
		})
	}
}

func TestParseHistogramDuration(t *testing.T) {
	histogramTests := [...]struct {
		input     string
		wantError string

		expectLow, expectHigh time.Duration
	}{

		{
			input:     "3ms-2ms",
			wantError: "must exceed low duration",
		},

		{
			input:     "zebra-2ms",
			wantError: "cannot parse low value",
		},
		{
			input:     "1ms-zebra",
			wantError: "cannot parse high value",
		},

		{
			input:     "0ms-1ms",
			wantError: "cannot parse low value",
		},

		{
			input:     "2ms-2000us",
			wantError: "durations cannot both be 2ms",
		},

		{
			input:      "-infinity-5ms",
			expectLow:  time.Duration(math.MinInt64),
			expectHigh: 5 * time.Millisecond,
		},

		{
			input:      "0-100µs",
			expectLow:  time.Duration(0),
			expectHigh: 100 * time.Microsecond,
		},

		{
			input:      "100us-100ms",
			expectLow:  100 * time.Microsecond,
			expectHigh: 100 * time.Millisecond,
		},

		{
			input:      "50ms-100ms",
			expectLow:  50 * time.Millisecond,
			expectHigh: 100 * time.Millisecond,
		},

		{
			input:      "100ms-infinity",
			expectLow:  100 * time.Millisecond,
			expectHigh: 100 * time.Millisecond,
		},
	}
	ctx := NewTestContext()
	defer ctx.Close()
	for _, tt := range histogramTests {
		t.Run(tt.input, func(t *testing.T) {
			left, right, err := parseBucket(tt.input)
			require.NoError(t, err, "bucket parsing errors should be tested elsewhere")
			hdr, err := parseDurationRange(left, right)
			if tt.wantError != "" {
				require.Error(t, err, "input: %q should be %q", tt.input, tt.wantError)
				assert.Contains(t, err.Error(), tt.wantError, "input: %q should be %q", tt.input, tt.wantError)

				s := ts.NewSeries(ctx, "test.series.0", ctx.StartTime, NewTestSeriesValues(ctx, 10, []float64{0, 1}))
				s.Tags = map[string]string{testBucketID: "0", testBucketKey: tt.input}
				_, err := newHistogramSeries(s, hti)
				assert.Error(t, err, "failed parsing should fail to create a series")
				return
			}
			assert.NoError(t, err, "unexpected error for %q", tt.input)
			assert.Equal(t, tt.expectLow, hdr.low, "unexpected value for %q", tt.input)
			assert.Equal(t, tt.expectHigh, hdr.high, "unexpected value for %q", tt.input)
		})
	}
}

func TestParseHistogramRange(t *testing.T) {
	histogramTests := [...]struct {
		input     string
		wantError string

		expectLow, expectHigh float64
	}{

		{
			input:     "3-2",
			wantError: "must exceed low value",
		},

		{
			input:     "zebra-2",
			wantError: "cannot parse low value",
		},
		{
			input:     "1-zebra",
			wantError: "cannot parse high value",
		},

		{
			input:     "2-2.0",
			wantError: "must exceed low value",
		},

		{
			input:      "0-100",
			expectLow:  0,
			expectHigh: 100,
		},

		{
			input:      "100-1000",
			expectLow:  100,
			expectHigh: 1000,
		},

		{
			input:      "50-100",
			expectLow:  50,
			expectHigh: 100,
		},

		{
			input:      "100-infinity",
			expectLow:  100,
			expectHigh: 100,
		},
	}
	ctx := NewTestContext()
	defer ctx.Close()

	for _, tt := range histogramTests {
		t.Run(tt.input, func(t *testing.T) {
			left, right, err := parseBucket(tt.input)
			require.NoError(t, err, "bucket parsing errors should be tested elsewhere")
			hvr, err := parseValueRange(left, right)
			if tt.wantError != "" {
				require.Error(t, err, "input: %q should be %q", tt.input, tt.wantError)
				assert.Contains(t, err.Error(), tt.wantError, "input: %q should be %q", tt.input, tt.wantError)

				s := ts.NewSeries(ctx, "test.series.0", ctx.StartTime, NewTestSeriesValues(ctx, 10, []float64{0, 1}))
				s.Tags = map[string]string{testBucketID: "0", testBucketKey: tt.input}
				_, err := newHistogramSeries(s, hti)
				assert.Error(t, err, "failed parsing should fail to create a series")
				return
			}
			assert.NoError(t, err, "unexpected error for %q", tt.input)
			assert.Equal(t, tt.expectLow, hvr.low, "unexpected value for %q", tt.input)
			assert.Equal(t, tt.expectHigh, hvr.high, "unexpected value for %q", tt.input)
		})
	}
}

type histogramRangeTestSeries struct {
	// const
	name               string
	ctx                *Context
	inputs             map[string][]float64
	expectedPercentile map[float64][]float64
	expectedCDF        map[int][]float64
	failureExpected    bool

	// mutable
	series []*ts.Series
}

func fetchHistogramRangeSeries(t *testing.T, ctx *Context) []histogramRangeTestSeries {
	nan := math.NaN()
	tests := [...]histogramRangeTestSeries{
		{
			name: "histogram buckets",
			inputs: map[string][]float64{
				"50-100":       {10, 10, 20, 0, 0, nan},
				"100-150":      {10, 10, 20, 0, 0, nan},
				"150-159":      {nan, nan, nan, nan, nan, nan},
				"160-200":      {10, 20, 0, 30, 0, nan},
				"200-infinity": {10, 10, 0, 30, 0, nan},
			},
			expectedPercentile: map[float64][]float64{
				0.1:  []float64{100, 100, 100, 200, nan, nan},
				50.0: []float64{150, 200, 100, 200, nan, nan},
				99.9: []float64{200, 200, 150, 200, nan, nan},
				100:  []float64{200, 200, 150, 200, nan, nan},
			},
			expectedCDF: map[int][]float64{
				1:   []float64{0, 0, 0, 0, nan, nan},
				100: []float64{10 / 40.0, 10 / 50.0, 20 / 40.0, 0, nan, nan},
				150: []float64{20 / 40.0, 20 / 50.0, 1.0, 0, nan, nan},
				200: []float64{1, 1, 1, 1, nan, nan},
				201: []float64{1, 1, 1, 1, nan, nan},
			},
		},
	}
	ret := make([]histogramRangeTestSeries, len(tests))
	for i, tt := range tests {
		ret[i] = tt
		ret[i].ctx = ctx
		ret[i].series = make([]*ts.Series, len(tt.inputs))

		j := 0
		for bucket, input := range tt.inputs {
			series := ts.NewSeries(
				ctx,
				"test.series."+bucket,
				ctx.StartTime,
				NewTestSeriesValues(ctx, histogramStepSize, input),
			)
			series.Tags = map[string]string{
				testBucketID:  strconv.Itoa(j),
				testBucketKey: bucket,
				"foo":         "bar",
			}
			ret[i].series[j] = series
			j++
		}
		require.True(t, j >= 2, "we should have produced two or more series")
	}
	return ret
}

type histogramDurationTestSeries struct {
	// const
	name               string
	ctx                *Context
	inputs             map[string][]float64
	expectedPercentile map[float64][]time.Duration
	expectedCDF        map[time.Duration][]float64
	failureExpected    bool

	// mutable
	series []*ts.Series
}

func fetchHistogramDurationSeries(t *testing.T, ctx *Context) []histogramDurationTestSeries {
	const (
		x50us  = 50 * time.Microsecond
		x100us = 100 * time.Microsecond
		x100ms = 100 * time.Millisecond
		x110ms = 110 * time.Millisecond
		x149ms = 149 * time.Millisecond
		x150ms = 150 * time.Millisecond
		x249ms = 249 * time.Millisecond
		x250ms = 250 * time.Millisecond
		x200ms = 200 * time.Millisecond
		x500ms = 500 * time.Millisecond
	)
	nan := math.NaN()

	tests := [...]histogramDurationTestSeries{
		{
			name: "gap in bucket",
			inputs: map[string][]float64{
				"100ms-110ms":    {10, 40, 1, 1},
				"200ms-250ms":    {10, 60, 0, 99},
				"250ms-infinity": {80, 0, 10, 0},
			},
			expectedPercentile: map[float64][]time.Duration{
				0.1:  {x110ms, x110ms, x110ms, x110ms},
				10.0: {x110ms, x110ms, x250ms, x250ms},
				50.0: {x250ms, x250ms, x250ms, x250ms},
			},
			expectedCDF: map[time.Duration][]float64{
				x100ms: {0, 0, 0, 0},
				x110ms: {10 / 100.0, 40 / 100.0, 1 / 11.0, 1 / 100.0},
				// 149ms has no precise bucket.
				x149ms: {10 / 100.0, 40 / 100.0, 1 / 11.0, 1 / 100.0},
				x249ms: {10 / 100.0, 40 / 100.0, 1 / 11.0, 1 / 100.0},
				// Everything before 250ms is the same as anything 110ms and beyond.
				x250ms: {1.0, 1.0, 1.0, 1.0},
			},
		},
		{
			name: "bucket granularity changes in the middle",
			inputs: map[string][]float64{
				"100ms-110ms": {nan, 10, 5, 5},
				"100ms-150ms": {10, nan, nan, nan},
				"110ms-150ms": {nan, 10, 10, 10},
				"150ms-200ms": {5, nan, nan, nan},
				"160ms-200ms": {nan, 5, 10, 10},
				"200ms-250ms": {5.0, 5, 5, 5},
			},
			expectedPercentile: map[float64][]time.Duration{
				10.0:  {x150ms, x110ms, x110ms, x110ms},
				50.0:  {x150ms, x150ms, x150ms, x150ms},
				90.0:  {x250ms, x250ms, x250ms, x250ms},
				100.0: {x250ms, x250ms, x250ms, x250ms},
			},
		},
		{
			name: "single series",
			inputs: map[string][]float64{
				"100ms-150ms": {10, 10, 20, 30},
			},
			expectedPercentile: map[float64][]time.Duration{
				0.0:  {x150ms, x150ms, x150ms, x150ms},
				50.0: {x150ms, x150ms, x150ms, x150ms},
				100:  {x150ms, x150ms, x150ms, x150ms},
			},
			expectedCDF: map[time.Duration][]float64{
				x100ms: {0, 0, 0, 0},
				x149ms: {0, 0, 0, 0},
				x150ms: {1, 1, 1, 1},
			},
		},
		{
			// TODO(observability): Will this ever happen to us?
			name: "unequal number of elements",
			inputs: map[string][]float64{
				"0-100µs":     {10, 10, 20},
				"100us-150ms": {10, 10, 20, 30},
			},
			expectedPercentile: map[float64][]time.Duration{0.1: {0}},
			expectedCDF:        map[time.Duration][]float64{0: {math.NaN()}},
			failureExpected:    true,
		},
		{
			name: "negative numbers",
			inputs: map[string][]float64{
				"100us-100ms": {10, 10, 20, 30},
				"100ms-149ms": {-10, -10, 1, -25},
				"150ms-200ms": {0, 0, 0, -5},
			},
			expectedPercentile: map[float64][]time.Duration{},
			expectedCDF: map[time.Duration][]float64{
				x100ms: {nan, nan, 20 / 21.0, nan},
				x149ms: {nan, nan, 1.0, nan},
			},
		},
		{
			name: "equal number of elements",
			inputs: map[string][]float64{
				"40us-50us":      {nan, nan, nan, nan},
				"50us-100us":     {10, 10, 20, 0},
				"100us-100ms":    {10, 10, 2000, 30},
				"100ms-149ms":    {1, 1, 1, 1},
				"150ms-159ms":    {nan, nan, nan, nan},
				"160ms-200ms":    {10, 2000, 20, 30},
				"500ms-infinity": {10, 10, 20, 30},
			},
			expectedPercentile: map[float64][]time.Duration{
				0:     {x50us, x50us, x50us, x50us},
				0.1:   {x100us, x100us, x100us, x100ms},
				1.0:   {x100us, x149ms, x100ms, x100ms},
				50.0:  {x149ms, x200ms, x100ms, x200ms},
				75.0:  {x200ms, x200ms, x100ms, x500ms},
				90.0:  {x500ms, x200ms, x100ms, x500ms},
				99.99: {x500ms, x500ms, x500ms, x500ms},
				100:   {x500ms, x500ms, x500ms, x500ms},
			},
			expectedCDF: map[time.Duration][]float64{
				49 * time.Microsecond: {0, 0, 0, 0},
				50 * time.Microsecond: {0, 0, 0, 0},
				51 * time.Microsecond: {0, 0, 0, 0},

				100 * time.Microsecond: {10 / 41.0, 10 / 2031.0, 20 / 2061.0, 0},
				101 * time.Microsecond: {10 / 41.0, 10 / 2031.0, 20 / 2061.0, 0 / 91.0},

				100 * time.Millisecond: {20 / 41.0, 20 / 2031.0, 2020 / 2061.0, 30 / 91.0},
				149 * time.Millisecond: {21 / 41.0, 21 / 2031.0, 2021 / 2061.0, 31 / 91.0},
				150 * time.Millisecond: {21 / 41.0, 21 / 2031.0, 2021 / 2061.0, 31 / 91.0},

				199 * time.Millisecond: {21 / 41.0, 21.0 / 2031.0, 2021 / 2061.0, 31 / 91.0},
				200 * time.Millisecond: {31 / 41.0, 2021 / 2031.0, 2041 / 2061.0, 61 / 91.0},
				201 * time.Millisecond: {31 / 41.0, 2021 / 2031.0, 2041 / 2061.0, 61 / 91.0},
				499 * time.Millisecond: {31 / 41.0, 2021 / 2031.0, 2041 / 2061.0, 61 / 91.0},

				500 * time.Millisecond: {1, 1, 1, 1},
				501 * time.Millisecond: {1, 1, 1, 1},
			},
		},
		{
			name: "total is zero",
			inputs: map[string][]float64{
				"100us-100ms": {0, nan},
				"100ms-149ms": {0, nan},
				"150ms-200ms": {0, nan},
			},
			expectedPercentile: map[float64][]time.Duration{},
			expectedCDF:        map[time.Duration][]float64{},
		},
	}

	ret := make([]histogramDurationTestSeries, len(tests))
	for i, tt := range tests {
		ret[i] = tt
		ret[i].ctx = ctx
		ret[i].series = make([]*ts.Series, len(tt.inputs))

		j := 0
		for bucket, input := range tt.inputs {
			series := ts.NewSeries(
				ctx,
				"test.series."+bucket,
				ctx.StartTime,
				NewTestSeriesValues(ctx, histogramStepSize, input),
			)
			series.Tags = map[string]string{
				testBucketID:  strconv.Itoa(j),
				testBucketKey: bucket,
				"foo":         "bar",
			}
			ret[i].series[j] = series
			j++
		}
		require.True(t, j >= 1, "we should have produced one or more series")
	}
	return ret
}

func histogramDebugInfo(hc *histogramCollection) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "{len:%d", hc.Len())
	for _, s := range hc.hs {
		fmt.Fprintf(&b, " [%v-%v]", s.Lower(), s.Upper())
	}
	b.WriteString(")")
	return b.Bytes()
}

func TestHistogramCollection(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	for _, tt := range fetchHistogramDurationSeries(t, ctx) {
		if len(tt.series) < 2 {
			continue
		}
		tt.series[1] = tt.series[0]
		mc, err := newHistogramCollection(ctx, tt.series, hti)
		assert.Nil(t, mc, "histogram collection should not be nil")
		require.Error(t, err, "histogram collection creation should fail")
		assert.Contains(t, err.Error(), "already seen range")
	}

	for _, tt := range fetchHistogramDurationSeries(t, ctx) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mc, err := newHistogramCollection(ctx, tt.series, hti)
			assert.NoError(t, err, "histogram collection creation should succeed")
			require.NotNil(t, mc, "histogram collection should not be nil")
			assert.Len(t, mc.hs, len(tt.series), "all series should have been consumed")

			di := string(histogramDebugInfo(mc))
			assert.NotEmpty(t, di, "debug info should have some content")
			assert.Contains(t, di, "len:", "debug info should contain the count of buckets")

			for _, hti := range []histogramTagInfo{
				histogramTagInfo{"bogus", "tags"},
				histogramTagInfo{testBucketID, "tags"},
				histogramTagInfo{"bogus", testBucketKey},
			} {
				_, err = newHistogramCollection(ctx, tt.series, hti)
				assert.Error(t, err, "bogus tags %v should cause a failure", hti)
			}
		})
	}
	_, err := newHistogramCollection(ctx, []*ts.Series{}, hti)
	require.Error(t, err, "empty series should fail")
	assert.Contains(t, err.Error(), "histograms must have at least one bucket")

	unequalStepSize := []*ts.Series{
		ts.NewSeries(ctx, "test.series.0", ctx.StartTime, NewTestSeriesValues(ctx, 10, []float64{0, 1})),
		ts.NewSeries(ctx, "test.series.1", ctx.StartTime, NewTestSeriesValues(ctx, 200, []float64{0, 1})),
	}
	for i, s := range unequalStepSize {
		s.Tags = map[string]string{testBucketID: fmt.Sprint(i), testBucketKey: fmt.Sprint(i+1) + "ms-200ms"}
	}
	_, err = newHistogramCollection(ctx, unequalStepSize, hti)
	require.Error(t, err, "two series with different step values should fail")
	assert.Contains(t, err.Error(), "millisPerStep for all series must be equal")

	dualSeries := func(b0, b1 string) []*ts.Series {
		s := []*ts.Series{
			ts.NewSeries(ctx, "test.series.0", ctx.StartTime, NewTestSeriesValues(ctx, 10, []float64{0, 1})),
			ts.NewSeries(ctx, "test.series.1", ctx.StartTime, NewTestSeriesValues(ctx, 10, []float64{0, 1})),
		}
		s[0].Tags = map[string]string{testBucketID: "1", testBucketKey: b0}
		s[1].Tags = map[string]string{testBucketID: "0", testBucketKey: b1}
		return s
	}

	_, err = newHistogramCollection(ctx, dualSeries("1-2", "1-2"), hti)
	require.Error(t, err, "two series with the same range")
	assert.Contains(t, err.Error(), "already seen range")

	_, err = newHistogramCollection(ctx, dualSeries("1-2", "10ms-20ms"), hti)
	require.Error(t, err, "two series with different range types should fail")
	assert.Contains(t, err.Error(), "cannot mix bucketTypes")

	_, err = newHistogramCollection(ctx, dualSeries("1s-2s", "1000ms-2000ms"), hti)
	require.Error(t, err, "two series with the same range described a different way should fail")
	assert.Contains(t, err.Error(), "already seen range")

	midpoint, _ := newHistogramCollection(ctx, dualSeries("200ms-400ms", "150ms-450ms"), hti)
	require.NotNil(t, midpoint, "two series with the same midpoint should succeed")
	require.Equal(t, 2, midpoint.Len(), "should get two series back")
	assert.Equal(t, "150ms-450ms", midpoint.seriesAt(0).Tags[testBucketKey], "bucket id sort failure")
	assert.Equal(t, "200ms-400ms", midpoint.seriesAt(1).Tags[testBucketKey], "bucket id sort failure")
}

func TestHistogramSeries(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	_, err := newHistogramSeries(&ts.Series{}, hti)
	assert.Error(t, err, "empty series should fail")

	_, err = newHistogramSeries(nil, hti)
	assert.Error(t, err, "nil should fail")

	t.Run("cdf failure", func(t *testing.T) { testHistogramCDFFailure(t) })
	t.Run("percentile failure", func(t *testing.T) { testHistogramPercentileFailure(t, ctx) })
	for _, tt := range fetchHistogramRangeSeries(t, ctx) {
		t.Run(tt.name+"/percentile", func(t *testing.T) {
			for number, expected := range tt.expectedPercentile {
				testHistogramPercentileSeries(t, tt.ctx, tt.series, tt.failureExpected, number, expected)
			}
		})
		t.Run(tt.name+"/cdf", func(t *testing.T) {
			for number, expected := range tt.expectedCDF {
				testHistogramCDFSeries(t, tt.ctx, tt.series, tt.failureExpected, fmt.Sprint(number), expected)
			}
		})
	}
	for _, tt := range fetchHistogramDurationSeries(t, ctx) {
		t.Run(tt.name+"/percentile", func(t *testing.T) { testHistogramPercentile(t, tt) })
		t.Run(tt.name+"/cdf", func(t *testing.T) { testHistogramCDF(t, tt) })
		t.Run(tt.name+" as series", func(t *testing.T) {
			for _, series := range tt.series {
				hs, err := newHistogramSeries(series, hti)
				assert.NoError(t, err, "individual series creation should succeed")
				assert.NotNil(t, hs, "result of successful series should be nil")
			}
		})
	}
}

func TestHistogramPercentileSortsByAndDedupesPercentiles(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	start := time.Now().Truncate(time.Minute).Add(-time.Hour)

	forBucket := func(bucketID, bucketTag string, s *ts.Series) *ts.Series {
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}
		s.Tags["bucketid"] = bucketID
		s.Tags["bucket"] = bucketTag
		return s
	}

	r, err := HistogramPercentile(ctx, ts.SeriesList{
		Values: []*ts.Series{
			forBucket("01", "0-5", ts.NewSeries(ctx, "foo.a", start, ts.NewConstantValues(ctx, 5, 60, 60000))),
			forBucket("02", "5-10", ts.NewSeries(ctx, "foo.b", start, ts.NewConstantValues(ctx, 7, 60, 60000))),
			forBucket("03", "10-15", ts.NewSeries(ctx, "foo.c", start, ts.NewConstantValues(ctx, 9, 60, 60000))),
			forBucket("04", "15-20", ts.NewSeries(ctx, "foo.c", start, ts.NewConstantValues(ctx, 11, 60, 60000))),
		},
		SortApplied: false,
	}, "bucketid", "bucket", []float64{
		99.0,
		99.0,
		95.0,
		50.0,
	})
	require.NoError(t, err)

	require.Equal(t, 3, r.Len())
	// Check sort asc and dedupe of p99
	assert.Equal(t, r.Values[0].Tags["histogramPercentile"], "p50")
	assert.Equal(t, r.Values[1].Tags["histogramPercentile"], "p95")
	assert.Equal(t, r.Values[2].Tags["histogramPercentile"], "p99")
	assert.True(t, r.SortApplied)
}

func TestHistogramCDFSortsByAndDedupesPercentiles(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	start := time.Now().Truncate(time.Minute).Add(-time.Hour)

	forBucket := func(bucketID, bucketTag string, s *ts.Series) *ts.Series {
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}
		s.Tags["bucketid"] = bucketID
		s.Tags["bucket"] = bucketTag
		return s
	}

	r, err := HistogramCDF(ctx, ts.SeriesList{
		Values: []*ts.Series{
			forBucket("01", "0-5", ts.NewSeries(ctx, "foo.a", start, ts.NewConstantValues(ctx, 5, 60, 60000))),
			forBucket("02", "5-10", ts.NewSeries(ctx, "foo.b", start, ts.NewConstantValues(ctx, 7, 60, 60000))),
			forBucket("03", "10-15", ts.NewSeries(ctx, "foo.c", start, ts.NewConstantValues(ctx, 9, 60, 60000))),
			forBucket("04", "15-20", ts.NewSeries(ctx, "foo.c", start, ts.NewConstantValues(ctx, 11, 60, 60000))),
		},
		SortApplied: false,
	}, "bucketid", "bucket", []string{
		"20",
		"20",
		"10",
	})
	require.NoError(t, err)

	require.Equal(t, 2, r.Len())
	// Check sort asc and dedupe of 20
	assert.Equal(t, r.Values[0].Tags["histogramCDF"], "10")
	assert.Equal(t, r.Values[1].Tags["histogramCDF"], "20")
	assert.True(t, r.SortApplied)
}

func testHistogramCDFFailure(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()
	tt := fetchHistogramDurationSeries(t, ctx)[0]
	testHistogramCDFSeries(t, ctx, tt.series, true, "not a valid duration", []float64{0, 1, 2, 3})
	testHistogramCDFSeries(t, ctx, tt.series, true, "        ", []float64{0, 1, 2, 3})
}

func testHistogramPercentileFailure(t *testing.T, ctx *Context) {
	var hc *histogramCollection
	for _, v := range []float64{-0.1, 100.1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			_, err := hc.percentile(v)
			assert.Error(t, err, "expected %v to fail", v)
		})
	}
}

func testHistogramPercentile(t *testing.T, tt histogramDurationTestSeries) {
	for percentile, expected := range tt.expectedPercentile {
		t.Run(fmt.Sprintf("p%v", percentile), func(t *testing.T) {
			testHistogramDurationPercentile(t, tt, percentile, expected)
		})
	}
}

func testHistogramDurationPercentile(
	t *testing.T,
	tt histogramDurationTestSeries,
	percentile float64,
	expected []time.Duration,
) {
	t.Parallel()
	expectedMillis := make([]float64, len(expected))
	for i := range expected {
		expectedMillis[i] = float64(expected[i]) / float64(time.Millisecond)
	}
	mp, err := HistogramPercentile(tt.ctx, ts.SeriesList{Values: tt.series}, testBucketID, testBucketKey, []float64{percentile})
	if tt.failureExpected {
		assert.Error(t, err, "expected percentile to fail")
		return
	}
	require.NoError(t, err)
	require.Equal(t, 1, mp.Len())
	values := mp.Values[0].SafeValues()
	require.Len(t, expected, len(values), "%s", "slice of expected values must match return length")
	for i, v := range values {
		assert.NotEqual(t, 0, v, "values returned by percentile should never be zero: %v", v)
		exp := float64(expected[i]) / float64(time.Millisecond)
		assert.InDelta(
			t, exp, v, 0.0001,
			"Expected %v (aka %f), got %v, for percentile(%v). Step %d/%d",
			exp, exp, v, percentile, i+1, len(values),
		)
	}
	if assert.NotEmpty(t, mp.Values[0].Tags, "tags should be preserved") {
		assert.Empty(t, mp.Values[0].Tags[testBucketID], "bucketid tag should not be preserved")
	}
}

func testHistogramCDF(t *testing.T, tt histogramDurationTestSeries) {
	for duration, expected := range tt.expectedCDF {
		fn := func(d string) {
			testHistogramCDFSeries(t, tt.ctx, tt.series, tt.failureExpected, d, expected)
		}
		// "200ms", "200ns", "1s"
		fn(duration.String())
		// "200", "0.2", "1000"
		fn(fmt.Sprint(float64(duration) / float64(time.Millisecond)))
	}
}

func testHistogramPercentileSeries(
	t *testing.T,
	ctx *Context,
	series []*ts.Series,
	failureExpected bool,
	pval float64,
	expected []float64,
) {
	t.Run(
		fmt.Sprint(pval),
		func(t *testing.T) {
			t.Parallel()
			mp, err := HistogramPercentile(ctx, ts.SeriesList{Values: series},
				testBucketID, testBucketKey, []float64{pval})
			if failureExpected {
				assert.Error(t, err, "expected percentile to fail")
				return
			}
			require.Equal(t, 1, mp.Len(), "expected exactly one series output")
			CompareOutputsAndExpected(t, histogramStepSize, ctx.StartTime,
				[]TestSeries{{Name: mp.Values[0].Name(), Data: expected}}, mp.Values)
			if assert.NotEmpty(t, mp.Values[0].Tags, "tags should be preserved") {
				assert.Empty(t, mp.Values[0].Tags[testBucketID], "bucketid tag should not be preserved")
			}
		})
}

func testHistogramCDFSeries(
	t *testing.T,
	ctx *Context,
	series []*ts.Series,
	failureExpected bool,
	duration string,
	expected []float64,
) {
	t.Run(
		duration,
		func(t *testing.T) {
			t.Parallel()
			mp, err := HistogramCDF(ctx, ts.SeriesList{Values: series},
				testBucketID, testBucketKey, []string{duration})
			if failureExpected {
				assert.Error(t, err, "expected cdf to fail")
				return
			}
			require.Equal(t, 1, mp.Len(), "expected exactly one series output")
			CompareOutputsAndExpected(t, histogramStepSize, ctx.StartTime,
				[]TestSeries{{Name: mp.Values[0].Name(), Data: expected}}, mp.Values)
			if assert.NotEmpty(t, mp.Values[0].Tags, "tags should be preserved") {
				assert.Empty(t, mp.Values[0].Tags[testBucketID], "bucketid tag should not be preserved")
			}
		})
}
