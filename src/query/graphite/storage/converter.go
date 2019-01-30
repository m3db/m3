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
	"bytes"
	"fmt"
	"strings"

	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
)

const (
	carbonSeparatorByte = byte('.')
	carbonGlobRune      = '*'
)

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
		Name:  graphite.TagName(count),
		Value: glob(metric),
	}
}

func matcherTerminator(count int) models.Matcher {
	return models.Matcher{
		Type:  models.MatchNotRegexp,
		Name:  graphite.TagName(count),
		Value: []byte(".*"),
	}
}

func convertTagsToMetricName(tags models.Tags) (string, error) {
	var builder strings.Builder
	for i, tag := range tags.Tags {
		if !bytes.Equal(tag.Name, graphite.TagName(i)) {
			// If not in order or a completely different named tag
			// then abort, we can't generate the metric name.
			err := fmt.Errorf("unexpected tag name: expected=%s, actual=%s",
				graphite.TagName(i), tag.Name)
			return "", err
		}

		_, err := builder.Write(tag.Value)
		if err != nil {
			return "", err
		}

		if i != len(tags.Tags)-1 {
			_, err := builder.WriteRune('.')
			if err != nil {
				return "", err
			}
		}
	}

	return builder.String(), nil
}
