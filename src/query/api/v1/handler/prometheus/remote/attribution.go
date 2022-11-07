package remote

import (
	"errors"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

/**
 * This class attributes metrics with matched configured labels for visibility purpose.
 * For example, today m3 only emits totally write QPS, with this attribution, we could plot
 * QPS breakdown by user defined label like namespace in Kubernetes or any label with finite
 * values (configured via capacity) and business meanings.
 */

var errNoOption = errors.New("no option configured for promAttributionMetrics")
var errInvalidMatcher = errors.New("invalid matcher configured for promAttributionMetrics")

type matchOp uint8

const (
	Eq matchOp = iota // ==
	Ne                // !=
)

type matcher struct {
	op      matchOp
	pattern string
}

func newMatcher(config string) (string, *matcher, error) {
	eqRegexp := regexp.MustCompile(`(\w+)==(.*)`)
	if eqRegexp.MatchString(config) {
		groups := eqRegexp.FindStringSubmatch(config)
		return groups[1], &matcher{op: Eq, pattern: groups[2]}, nil
	}
	neRegexp := regexp.MustCompile(`(\w+)!=(.*)`)
	if neRegexp.MatchString(config) {
		groups := neRegexp.FindStringSubmatch(config)
		return groups[1], &matcher{op: Ne, pattern: groups[2]}, nil
	}
	return "", nil, errInvalidMatcher
}

func (m *matcher) match(value string) bool {
	switch m.op {
	case Eq:
		return m.pattern == value
	case Ne:
		return m.pattern != value
	default:
		return false
	}
}

type promAttributionMetrics struct {
	opts               *instrument.AttributionConfiguration
	matchers           map[string]*matcher
	baseScope          tally.Scope
	attributedCounters sync.Map
	counterSize        int32
	skipSamples        tally.Counter
	missSamples        tally.Counter
}

func (pam *promAttributionMetrics) match(labels []prompb.Label) bool {
	for _, l := range labels {
		name := string(l.Name)
		if m, ok := pam.matchers[name]; ok {
			if !m.match(string(l.Value)) {
				// if 1 of the matches violates, skip the attribution
				return false
			}
		}
	}
	return true
}

func (pam *promAttributionMetrics) attribute(ts prompb.TimeSeries) {
	if rand.Float64() >= pam.opts.SamplingRate || !pam.match(ts.Labels) {
		return
	}
	matchedValues := make([]string, len(pam.opts.Labels))
	found := 0
	sample_count := int64(len(ts.Samples))
	for _, l := range ts.Labels {
		labelName := string(l.Name)
		for i, label := range pam.opts.Labels {
			if labelName == label {
				found++
				matchedValues[i] = string(l.Value)
			}
		}
	}
	if found < len(pam.opts.Labels) {
		// time series doesn't match all the configured labels, skip the attrubtion
		pam.skipSamples.Inc(sample_count)
		return
	}
	attributeLabels := strings.Join(pam.opts.Labels, "_")
	attributeValues := strings.Join(matchedValues, ":")
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

func newPromAttributionMetrics(scope tally.Scope,
	opts *instrument.AttributionConfiguration,
	logger *zap.Logger) (*promAttributionMetrics, error) {
	if opts == nil {
		return nil, errNoOption
	}
	logger.Info("Creating new attribution group", zap.String("name", opts.Name))
	baseScope := scope.SubScope("attribution").SubScope(opts.Name)
	matchers := map[string]*matcher{}
	for _, mCfg := range opts.Matchers {
		if key, m, err := newMatcher(mCfg); err != nil {
			logger.Error("Encounter invalid matchers", zap.String("match", mCfg), zap.Error(err))
			return nil, err
		} else {
			logger.Info("Adding matcher to attribution group",
				zap.String("attribution", opts.Name),
				zap.String("key", key),
				zap.Uint8("op", uint8(m.op)),
				zap.String("value", m.pattern))
			matchers[key] = m
		}
	}
	return &promAttributionMetrics{
		opts:               opts,
		matchers:           matchers,
		baseScope:          baseScope,
		attributedCounters: sync.Map{},
		counterSize:        0,
		skipSamples:        baseScope.Counter("skip_sample_count"),
		missSamples:        baseScope.Counter("miss_sample_count"),
	}, nil
}
