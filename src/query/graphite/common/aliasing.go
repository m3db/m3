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

	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
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

// AliasByTagsStrings allows you to alias by strings and tags.
// Tags are denoted by {{.TagName}} in the argument you pass in.
// e.g. 'fetch foo:bar | AliasByTagsStrings {{.foo}}-baz' will produce the alias 'bar-baz'
func AliasByTagsStrings(ctx *Context, series ts.SeriesList, str string) (ts.SeriesList, error) {
	wordsList, tags, err := getTagsFromString(str)
	if err != nil {
		return ts.SeriesList{}, err
	}

	if len(tags) == 0 {
		// TODO(r): Test this case, had to add a return here... clearly no tests for this.
		return Alias(ctx, series, wordsList)
	}

	renamed := make([]*ts.Series, series.Len())

	for i, s := range series.Values {
		var values []interface{}
		for _, t := range tags {
			if val, ok := s.Tags[t]; ok {
				values = append(values, val)
			} else {
				// If tag specified in the alias function does not exist in the timeseries
				return ts.SeriesList{}, fmt.Errorf("This tag (%v) does not exist for timeseries: %v", t, s.Name())
			}
		}
		str = fmt.Sprintf(wordsList, values...)
		renamed[i] = s.RenamedTo(str)
	}

	series.Values = renamed
	return series, nil
}

// getTagsFromString pulls out the tags specified in the argument provided
// by the user in the alias function and stores them in a 'tags' list. It then
// replaces the tag with a %s so that the main function can replace those
// with the tag values. For example, if you give the argument '{{.foo}}-baz',
// this function pulls out 'foo' (the tag) and you are left with '%s-baz'
func getTagsFromString(tag string) (string, []string, error) {
	var tags, text []string

	opening := "{{."
	closing := "}}"

	for {
		tagStart := strings.Index(tag, opening)
		tagEnd := strings.Index(tag, closing)
		if tagStart == -1 && tagEnd == -1 {
			text = append(text, tag)
			break
		}
		if tagStart == -1 && tagEnd != -1 {
			return "", nil, fmt.Errorf("Found unexpected closing bracket ('}') in string: %s", tag)
		}
		if tagEnd == -1 || tagEnd < tagStart {
			return "", nil, fmt.Errorf("Unable to find closing bracket ('}') in string: %s", tag)
		}
		text = append(text, tag[:tagStart])
		text = append(text, "%s")
		tags = append(tags, tag[tagStart+len(opening):tagEnd])
		tag = tag[tagEnd+len(closing):]
	}
	return strings.Join(text, ""), tags, nil
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

// AliasByNode renames a time series result according to a subset of the nodes
// in its hierarchy.
func AliasByNode(_ *Context, seriesList ts.SeriesList, nodes ...int) (ts.SeriesList, error) {
	renamed := make([]*ts.Series, 0, seriesList.Len())
	for _, series := range seriesList.Values {
		name := series.Name()
		left := strings.LastIndex(name, "(") + 1
		name = name[left:]
		right := strings.IndexAny(name, ",)")
		if right == -1 {
			right = len(name)
		}
		nameParts := strings.Split(name[0:right], ".")
		newNameParts := make([]string, 0, len(nodes))
		for _, node := range nodes {
			// NB(jayp): graphite supports negative indexing, so we need to also!
			if node < 0 {
				node += len(nameParts)
			}
			if node < 0 || node >= len(nameParts) {
				continue
			}
			newNameParts = append(newNameParts, nameParts[node])
		}
		newName := strings.Join(newNameParts, ".")
		newSeries := series.RenamedTo(newName)
		renamed = append(renamed, newSeries)
	}
	seriesList.Values = renamed
	return seriesList, nil
}

// AliasSub runs series names through a regex search/replace.
func AliasSub(_ *Context, input ts.SeriesList, search, replace string) (ts.SeriesList, error) {
	regex, err := regexp.Compile(search)
	if err != nil {
		return ts.SeriesList{}, err
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
				return ts.SeriesList{}, err
			}
			output[idx] = series.RenamedTo(newName)
		}
	}

	input.Values = output
	return input, nil
}

// AliasByTags takes in multiple tag names and will rename the series
// based on available tags. if a tag value is not found, it is ignored.
func AliasByTags(_ *Context, in ts.SeriesList, tags ...string) (ts.SeriesList, error) {
	values := make([]string, len(tags))
	renamed := make([]*ts.Series, in.Len())
	for i, s := range in.Values {
		for j, t := range tags {
			if value, exists := s.Tags[t]; exists {
				values[j] = value
			} else {
				values[j] = ""
			}
		}
		renamed[i] = s.RenamedTo(strings.TrimSpace(strings.Join(values, " ")))
	}
	in.Values = renamed
	return in, nil
}
