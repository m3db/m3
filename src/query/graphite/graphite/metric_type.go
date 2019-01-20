package graphite

import (
	"errors"

	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/query/graphite/ts"
)

// MetricType is an enumeration counts, timers and gauges
type MetricType int

const (
	// Counts are for metrics that are summed
	Counts = iota
	// Timers are for measuring durations and are averaged
	Timers
	// Gauges are for measuring levels and are averaged
	Gauges
	// Ratios is a metric type that does not exist in raw/consolidated metrics
	// It is used to identify the metric type of results of certain expressions like divideSeries or asPercent
	Ratios
)

const (
	// CountsStr is the string value of counts, which is used in statsd format
	CountsStr = "counts"
	// CounterStr is the string value of counter, which is used in m3 format
	CounterStr = "counter"
	// TimersStr is the string value of timers, which is used in statsd format
	TimersStr = "timers"
	// TimerStr is the string value of timer, which is used in m3 format
	TimerStr = "timer"
	// GaugesStr is the string value of gauges, which is used in statsd format
	GaugesStr = "gauges"
	// GaugeStr is the string value of gauge, which is used in m3 format
	GaugeStr = "gauge"
	// RatiosStr is the string value of ratios
	RatiosStr = "ratios"
	// TypeStr is the string value of type
	TypeStr = "type"
)

// MetricTypeFromID returns a metric.Type from an id.
func MetricTypeFromID(id string) metric.Type {
	mt := ExtractNthMetricPart(id, 2)
	switch mt {
	case CountsStr:
		return metric.CounterType
	case TimersStr:
		return metric.TimerType
	case GaugesStr:
		return metric.GaugeType
	default:
		return metric.UnknownType
	}
}

// MetricTypeFromTags returns a MetricType for a series by
// introspecting the series Tags property for a metric type name.
// Defaults to Counts
func MetricTypeFromTags(tags map[string]string) MetricType {
	v := tags[TypeStr]
	switch v {
	case CountsStr, CounterStr:
		return Counts
	case TimersStr, TimerStr:
		return Timers
	case GaugesStr, GaugeStr:
		return Gauges
	case RatiosStr:
		return Ratios
	default:
		return Counts
	}
}

// MetricTypeStrFromTags returns a MetricType string to use in the MetricID of a series
// by introspecting the series Tags property for a metric type name.
// It will return an error if the metric type is not counter, timer or gauge.
func MetricTypeStrFromTags(tags map[string]string) (string, error) {
	mt := MetricTypeFromTags(tags)
	switch mt {
	case Counts:
		return CountsStr, nil
	case Timers:
		return TimersStr, nil
	case Gauges:
		return GaugesStr, nil
	default:
		return "", errors.New("unknown type in the tags")
	}
}

// ConsolidationFuncFromTags returns the appropriate consolidation function for
// the metric type represented in the given series.
func ConsolidationFuncFromTags(tags map[string]string) ts.ConsolidationApproach {
	mt := MetricTypeFromTags(tags)
	return ConsolidationFuncForMetricType(mt)
}

// ConsolidationFuncForMetricType returns the appropriate consolidation function
// for the specified metric type.
func ConsolidationFuncForMetricType(mt MetricType) ts.ConsolidationApproach {
	switch mt {
	case Counts:
		return ts.ConsolidationSum
	case Timers:
		return ts.ConsolidationAvg
	case Gauges:
		return ts.ConsolidationAvg
	case Ratios:
		return ts.ConsolidationAvg
	default:
		return ts.ConsolidationAvg
	}
}
