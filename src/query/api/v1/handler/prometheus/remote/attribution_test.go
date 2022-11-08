package remote

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var TEST_LABEL_A = prompb.Label{Name: []byte("labelA"), Value: []byte("value-A")}
var TEST_LABEL_B = prompb.Label{Name: []byte("labelB"), Value: []byte("valueB")}
var TEST_LABEL_A1 = prompb.Label{Name: []byte("labelA"), Value: []byte("value-A1")}

func TestPromAttributionMetrics_SingleLabel(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     10,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name)},
	}
	pam, _ := newPromAttributionMetrics(scope, &opts, logger)
	ts := prompb.TimeSeries{
		Labels:  []prompb.Label{TEST_LABEL_A},
		Samples: make([]prompb.Sample, 3),
	}
	pam.attribute(ts)
	foundMetric := xclock.WaitUntil(func() bool {
		found, ok := scope.Snapshot().Counters()["base.attribution.name.sample_count+attr_labelA=value-A,test=prom-attribution-test"]
		return ok && found.Value() == 3
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestPromAttributionMetrics_MultipleLabels(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     10,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name), string(TEST_LABEL_B.Name)},
	}
	pam, _ := newPromAttributionMetrics(scope, &opts, logger)
	tsList := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{TEST_LABEL_A},
			Samples: make([]prompb.Sample, 3),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B},
			Samples: make([]prompb.Sample, 2),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A},
			Samples: make([]prompb.Sample, 7),
		},
	}
	for _, ts := range tsList {
		pam.attribute(ts)
	}
	foundMetric := xclock.WaitUntil(func() bool {
		found, ok := scope.Snapshot().Counters()["base.attribution.name.sample_count+attr_labelA_labelB=value-A:valueB,test=prom-attribution-test"]
		return ok && found.Value() == 7
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestPromAttributionMetrics_Capacity(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     1,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name)},
	}
	pam, _ := newPromAttributionMetrics(scope, &opts, logger)
	tsList := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{TEST_LABEL_A},
			Samples: make([]prompb.Sample, 3),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 2),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A},
			Samples: make([]prompb.Sample, 7),
		},
	}
	for _, ts := range tsList {
		pam.attribute(ts)
	}
	// Because capacity is one, label A with multiple values will only have 1 counter, the other one should go to miss
	foundMetric := xclock.WaitUntil(func() bool {
		counters := scope.Snapshot().Counters()
		found, ok := counters["base.attribution.name.sample_count+attr_labelA=value-A,test=prom-attribution-test"]
		_, notOk := counters["base.attribution.name.sample_count+attr_labelA=value-A1,test=prom-attribution-test"]
		return ok && found.Value() == 10 && !notOk
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestPromAttributionMetrics_EqMatch(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     10,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name)},
		Matchers:     []string{"labelA==value-A"},
	}
	pam, _ := newPromAttributionMetrics(scope, &opts, logger)
	tsList := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{TEST_LABEL_A},
			Samples: make([]prompb.Sample, 3),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 2),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A},
			Samples: make([]prompb.Sample, 7),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 11),
		},
	}
	for _, ts := range tsList {
		pam.attribute(ts)
	}
	// samples that contain labelA with value-A will be used
	foundMetric := xclock.WaitUntil(func() bool {
		counters := scope.Snapshot().Counters()
		found, ok := counters["base.attribution.name.sample_count+attr_labelA=value-A,test=prom-attribution-test"]
		_, notOk := counters["base.attribution.name.sample_count+attr_labelA=value-A1,test=prom-attribution-test"]
		return ok && found.Value() == 10 && !notOk
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestPromAttributionMetrics_NeMatch(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     10,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name)},
		Matchers:     []string{"labelA!=value-A"},
	}
	pam, _ := newPromAttributionMetrics(scope, &opts, logger)
	tsList := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{TEST_LABEL_A},
			Samples: make([]prompb.Sample, 3),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 2),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A},
			Samples: make([]prompb.Sample, 7),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 11),
		},
	}
	for _, ts := range tsList {
		pam.attribute(ts)
	}
	// samples that contain labelA without value-A will be used
	foundMetric := xclock.WaitUntil(func() bool {
		counters := scope.Snapshot().Counters()
		found, ok := counters["base.attribution.name.sample_count+attr_labelA=value-A1,test=prom-attribution-test"]
		_, notOk := counters["base.attribution.name.sample_count+attr_labelA=value-A,test=prom-attribution-test"]
		return ok && found.Value() == 13 && !notOk
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestPromAttributionMetrics_InvalidMatcher(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     10,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name)},
		Matchers:     []string{"labe_lA=~value-A"},
	}
	pam, err := newPromAttributionMetrics(scope, &opts, logger)
	require.Nil(t, pam)
	require.Equal(t, errInvalidMatcher, err)
}

func TestPromAttributionMetrics_MutilMatch(t *testing.T) {
	logger := instrument.NewTestDebugLogger(t)
	scope := tally.NewTestScope("base", map[string]string{"test": "prom-attribution-test"})
	opts := instrument.AttributionConfiguration{
		Name:         "name",
		Capacity:     10,
		SamplingRate: 1,
		Labels:       []string{string(TEST_LABEL_A.Name)},
		Matchers:     []string{"labelA==value-A", "labelB==valueB"},
	}
	pam, _ := newPromAttributionMetrics(scope, &opts, logger)
	tsList := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{TEST_LABEL_A},
			Samples: make([]prompb.Sample, 3),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 2),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A},
			Samples: make([]prompb.Sample, 7),
		},
		{
			Labels:  []prompb.Label{TEST_LABEL_B, TEST_LABEL_A1},
			Samples: make([]prompb.Sample, 11),
		},
	}
	for _, ts := range tsList {
		pam.attribute(ts)
	}
	// samples that contain labelA==value-A and labelB==valueB will be used
	foundMetric := xclock.WaitUntil(func() bool {
		counters := scope.Snapshot().Counters()
		found, ok := counters["base.attribution.name.sample_count+attr_labelA=value-A,test=prom-attribution-test"]
		_, notOk := counters["base.attribution.name.sample_count+attr_labelA=value-A1,test=prom-attribution-test"]
		return ok && found.Value() == 7 && !notOk
	}, 5*time.Second)
	require.True(t, foundMetric)
}
