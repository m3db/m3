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

package common

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/x/errors"
)

var (
	backReferenceRe = regexp.MustCompile(`\\\d+`)
)

// Alias takes one metric or a wildcard seriesList and a string in quotes.
// Prints the string instead of the metric name in the legend.
func Alias(_ *Context, series ts.SeriesList, a string) (ts.SeriesList, error) {
	renamed := make([]*ts.Series, series.Len())
	for i := range series.Values {
		renamed[i] = series.Values[i].RenamedTo(a)
	}
	series.Values = renamed
	return series, nil
}

// AliasByMetric takes a seriesList and applies an alias derived from the base
// metric name.
func AliasByMetric(ctx *Context, series ts.SeriesList) (ts.SeriesList, error) {
	renamed := make([]*ts.Series, series.Len())
	for i, s := range series.Values {
		firstPart := strings.Split(s.Name(), ",")[0]
		terms := strings.Split(firstPart, ".")
		renamed[i] = s.RenamedTo(terms[len(terms)-1])
	}
	series.Values = renamed
	return series, nil
}

// AliasSub runs series names through a regex search/replace.
func AliasSub(_ *Context, input ts.SeriesList, search, replace string) (ts.SeriesList, error) {
	regex, err := regexp.Compile(search)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	output := make([]*ts.Series, input.Len())
	for idx, series := range input.Values {
		name := series.Name()
		if submatches := regex.FindStringSubmatch(name); submatches == nil {
			// if the pattern doesn't match, we don't change the series name.
			output[idx] = series
		} else {
			// go regexp package doesn't support back-references, so we need to work around it.
			newName := regex.ReplaceAllString(name, replace)
			newName = backReferenceRe.ReplaceAllStringFunc(newName, func(matched string) string {
				index, retErr := strconv.Atoi(matched[1:])
				if retErr != nil {
					err = retErr
					return ""
				}
				if index >= len(submatches) {
					err = errors.NewInvalidParamsError(fmt.Errorf("invalid group reference in %s", replace))
					return ""
				}
				return submatches[index]
			})
			if err != nil {
				return ts.NewSeriesList(), err
			}
			output[idx] = series.RenamedTo(newName)
		}
	}

	input.Values = output
	return input, nil
}
