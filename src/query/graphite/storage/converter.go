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

package storage

import (
	"fmt"

	"github.com/m3db/m3/src/query/models"
)

const (
	// graphiteFormat is the format for graphite metric tag names, which will be
	// represented as tag/value pairs in m3db.
	//
	// NB: stats.gauges.donkey.kong.barrels would become the following tag set:
	// {__graphite0__: stats}
	// {__graphite1__: gauges}
	// {__graphite2__: donkey}
	// {__graphite3__: kong}
	// {__graphite4__: barrels}
	graphiteFormat          = "__graphite%d__"
	numPreFormattedKeyNames = 124
	carbonSeparatorByte     = byte('.')
	carbonGlobRune          = '*'
)

var (
	// Number of pre-formatted key names to generate in the init() function.
	preFormattedKeyNames [][]byte
)

func init() {
	preFormattedKeyNames = make([][]byte, numPreFormattedKeyNames)
	for i := 0; i < numPreFormattedKeyNames; i++ {
		preFormattedKeyNames[i] = generateKeyName(i)
	}
}

func getOrGenerateKeyName(idx int) []byte {
	if idx < len(preFormattedKeyNames) {
		return preFormattedKeyNames[idx]
	}

	return []byte(fmt.Sprintf(graphiteFormat, idx))
}

func generateKeyName(idx int) []byte {
	return []byte(fmt.Sprintf(graphiteFormat, idx))
}

func glob(metric string) []byte {
	globLen := len(metric)
	for _, c := range metric {
		if c == carbonGlobRune {
			globLen++
		}
	}

	glob := make([]byte, globLen)
	i := 0
	for _, c := range metric {
		if c == carbonGlobRune {
			glob[i] = carbonSeparatorByte
			i++
		}

		glob[i] = byte(c)
		i++
	}

	return glob
}

func convertMetricPartToMatcher(count int, metric string) models.Matcher {
	return models.Matcher{
		Type:  models.MatchRegexp,
		Name:  getOrGenerateKeyName(count),
		Value: glob(metric),
	}
}

func matcherTerminator(count int) models.Matcher {
	return models.Matcher{
		Type:  models.MatchNotRegexp,
		Name:  getOrGenerateKeyName(count),
		Value: []byte(".*"),
	}
}
