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

package graphite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	statsdStat    = "foo.bar.baz.qux.quz"
	malformedStat = "foobarbazquxquz"
	dashedString  = "some-other-delimiter"
)

func TestExtractNthEmptyMetric(t *testing.T) {
	assert.Equal(t, "", ExtractNthMetricPart("", 0))
	assert.Equal(t, 0, CountMetricParts(""))
}

func TestExtractNthMetricPartNoDots(t *testing.T) {
	nodots := "foobarbazquxquz"
	assert.Equal(t, nodots, ExtractNthMetricPart(malformedStat, 0))
	assert.Equal(t, 1, CountMetricParts(malformedStat))
}

func TestExtractNthMetricPartStandardCase(t *testing.T) {
	assert.Equal(t, "foo", ExtractNthMetricPart(statsdStat, 0))
	assert.Equal(t, "bar", ExtractNthMetricPart(statsdStat, 1))
	assert.Equal(t, "quz", ExtractNthMetricPart(statsdStat, 4))
	assert.Equal(t, 5, CountMetricParts(statsdStat))
}

func TestExtractNthMetricPartPastEnd(t *testing.T) {
	assert.Equal(t, "", ExtractNthMetricPart(statsdStat, 10))
}

func TestExtractNthMetricPartNegativeN(t *testing.T) {
	assert.Equal(t, "", ExtractNthMetricPart(statsdStat, -2))
}

func TestExtractNthStringPart(t *testing.T) {
	assert.Equal(t, "other", ExtractNthStringPart(dashedString, 1, '-'))
	assert.Equal(t, 3, countMetricPartsWithDelimiter(dashedString, '-'))
}

func TestDropLastMetricPart(t *testing.T) {
	assert.Equal(t, "", DropLastMetricPart(""))
	assert.Equal(t, "", DropLastMetricPart("abc"))
	assert.Equal(t, "abc", DropLastMetricPart("abc.def"))
	assert.Equal(t, "abc.def.ghi", DropLastMetricPart("abc.def.ghi.jkl"))
	assert.Equal(t, "abc.def.ghi", DropLastMetricPart("abc.def.ghi."))
}
