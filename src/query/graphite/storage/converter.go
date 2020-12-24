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
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
)

const (
	wildcard = "*"
)

func convertMetricPartToMatcher(
	count int,
	metric string,
) (models.Matcher, error) {
	var matchType models.MatchType
	if metric == wildcard {
		if count == 0 {
			// Match field does not actually match all values
			// for the first metric selector in practice.
			// Need to special case this and just use a regexp match all
			// on this first value.
			// This is ok since there usually a very small amount of distinct
			// values in the first dot separator.
			return models.Matcher{
				Type:  models.MatchRegexp,
				Name:  graphite.TagName(count),
				Value: []byte(".*"),
			}, nil
		}
		return models.Matcher{
			Type: models.MatchField,
			Name: graphite.TagName(count),
		}, nil
	}

	value, isRegex, err := graphite.GlobToRegexPattern(metric)
	if err != nil {
		return models.Matcher{}, err
	}

	if isRegex {
		matchType = models.MatchRegexp
	} else {
		matchType = models.MatchEqual
	}

	return models.Matcher{
		Type:  matchType,
		Name:  graphite.TagName(count),
		Value: value,
	}, nil
}

func matcherTerminator(count int) models.Matcher {
	return models.Matcher{
		Type: models.MatchNotField,
		Name: graphite.TagName(count),
	}
}
