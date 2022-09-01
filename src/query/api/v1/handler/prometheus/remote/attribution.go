package remote

import (
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
)

/**
 * This class attributes metrics with matched configured labels for visibility purpose.
 * For example, today m3 only emits totally write QPS, with this attribution, we could plot
 * QPS breakdown by user defined label like namespace in Kubernetes or any label with finite
 * values (configured via capacity) and business meanings.
 */

var errNoOption = errors.New("no option configured for promAttributionMetrics")

type promAttributionMetrics struct {
	opts               *instrument.AttributionConfiguration
	baseScope          tally.Scope
	attributedCounters sync.Map
	counterSize        int32
	skipSamples        tally.Counter
	missSamples        tally.Counter
}

func (pam *promAttributionMetrics) attribute(ts prompb.TimeSeries) {
	if rand.Float64() >= pam.opts.SamplingRate {
		return
	}
	labelValues := make([]string, len(pam.opts.Labels))
	found := 0
	sample_count := int64(len(ts.Samples))
	for _, l := range ts.Labels {
		labelName := string(l.Name)
		for i, label := range pam.opts.Labels {
			if labelName == label {
				found++
				labelValues[i] = string(l.Value)
			}
		}
	}
	if found < len(pam.opts.Labels) {
		// time series doesn't match all the configured labels, skip the attrubtion
		pam.skipSamples.Inc(sample_count)
		return
	}
	attributeLabels := strings.Join(pam.opts.Labels, "_")
	attributeValues := strings.Join(labelValues, ":")
	// look up if the counter in the map already, if not in the map and the counter reaches its capacity, consider this is a miss
	_, ok := pam.attributedCounters.Load(attributeValues)
	if !ok && pam.counterSize >= int32(pam.opts.Capacity) {
		pam.missSamples.Inc(sample_count)
		return
	}
	// get counter and increase the values
	c, ok := pam.attributedCounters.LoadOrStore(attributeValues,
		pam.baseScope.Tagged(map[string]string{attributeLabels: attributeValues}).Counter("sample_count"))
	if !ok {
		atomic.AddInt32(&pam.counterSize, 1)
	}
	(c.(tally.Counter)).Inc(sample_count)
}

func newPromAttributionMetrics(scope tally.Scope, opts *instrument.AttributionConfiguration) (*promAttributionMetrics, error) {
	if opts == nil {
		return nil, errNoOption
	}
	baseScope := scope.SubScope("attribution").SubScope(opts.Name)
	return &promAttributionMetrics{
		opts:               opts,
		baseScope:          baseScope,
		attributedCounters: sync.Map{},
		counterSize:        0,
		skipSamples:        baseScope.Counter("skip_sample_count"),
		missSamples:        baseScope.Counter("miss_sample_count"),
	}, nil
}
