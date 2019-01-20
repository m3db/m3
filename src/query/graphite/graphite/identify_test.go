package graphite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	statsdStat    = "dispatch.production.san_francisco.uberx.drivers"
	malformedStat = "dispatchproductionsan_franciscouberxdrivers"
	dashedString  = "some-other-delimiter"
)

func TestExtractNthMetricPartNoDots(t *testing.T) {
	assert.Equal(t, "dispatchproductionsan_franciscouberxdrivers", ExtractNthMetricPart(malformedStat, 0))
}

func TestExtractNthMetricPartStandardCase(t *testing.T) {
	assert.Equal(t, "dispatch", ExtractNthMetricPart(statsdStat, 0))
	assert.Equal(t, "production", ExtractNthMetricPart(statsdStat, 1))
	assert.Equal(t, "drivers", ExtractNthMetricPart(statsdStat, 4))
}

func TestExtractNthMetricPartPastEnd(t *testing.T) {
	assert.Equal(t, "", ExtractNthMetricPart(statsdStat, 10))
}

func TestExtractNthMetricPartNegativeN(t *testing.T) {
	assert.Equal(t, "", ExtractNthMetricPart(statsdStat, -2))
}

func TestExtractNthStringPart(t *testing.T) {
	assert.Equal(t, "other", ExtractNthStringPart(dashedString, 1, '-'))
}
