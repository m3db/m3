// Copyright (c) 2018 Uber Technologies, Inc.
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

package native

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	endParam          = "end"
	startParam        = "start"
	timeParam         = "time"
	queryParam        = "query"
	stepParam         = "step"
	debugParam        = "debug"
	endExclusiveParam = "end-exclusive"

	formatErrStr = "error parsing param: %s, error: %v"
)

func parseTime(r *http.Request, key string) (time.Time, error) {
	if t := r.FormValue(key); t != "" {
		return util.ParseTimeString(t)
	}

	return time.Time{}, errors.ErrNotFound
}

// nolint: unparam
func parseDuration(r *http.Request, key string) (time.Duration, error) {
	str := r.FormValue(key)
	if str == "" {
		return 0, errors.ErrNotFound
	}

	value, err := time.ParseDuration(str)
	if err == nil {
		return value, nil
	}

	// Try parsing as an integer value specifying seconds, the Prometheus default
	if seconds, intErr := strconv.ParseInt(str, 10, 64); intErr == nil {
		return time.Duration(seconds) * time.Second, nil
	}

	return 0, err
}

// parseParams parses all params from the GET request
func parseParams(r *http.Request) (models.RequestParams, *xhttp.ParseError) {
	params := models.RequestParams{
		Now: time.Now(),
	}

	t, err := prometheus.ParseRequestTimeout(r)
	if err != nil {
		return params, xhttp.NewParseError(err, http.StatusBadRequest)
	}
	params.Timeout = t

	start, err := parseTime(r, startParam)
	if err != nil {
		return params, xhttp.NewParseError(fmt.Errorf(formatErrStr, startParam, err), http.StatusBadRequest)
	}
	params.Start = start

	end, err := parseTime(r, endParam)
	if err != nil {
		return params, xhttp.NewParseError(fmt.Errorf(formatErrStr, endParam, err), http.StatusBadRequest)
	}
	params.End = end

	step, err := parseDuration(r, stepParam)
	if err != nil {
		return params, xhttp.NewParseError(fmt.Errorf(formatErrStr, stepParam, err), http.StatusBadRequest)
	}
	params.Step = step

	query, err := parseQuery(r)
	if err != nil {
		return params, xhttp.NewParseError(fmt.Errorf(formatErrStr, queryParam, err), http.StatusBadRequest)
	}
	params.Query = query

	// Skip debug if unable to parse debug param
	debugVal := r.FormValue(debugParam)
	if debugVal != "" {
		debug, err := strconv.ParseBool(r.FormValue(debugParam))
		if err != nil {
			logging.WithContext(r.Context()).Warn("unable to parse debug flag", zap.Any("error", err))
		}
		params.Debug = debug
	}

	// Default to including end if unable to parse the flag
	endExclusiveVal := r.FormValue(endExclusiveParam)
	params.IncludeEnd = true
	if endExclusiveVal != "" {
		excludeEnd, err := strconv.ParseBool(endExclusiveVal)
		if err != nil {
			logging.WithContext(r.Context()).Warn("unable to parse end inclusive flag", zap.Any("error", err))
		}

		params.IncludeEnd = !excludeEnd
	}

	return params, nil
}

// parseInstantaneousParams parses all params from the GET request
func parseInstantaneousParams(r *http.Request) (models.RequestParams, *xhttp.ParseError) {
	params := models.RequestParams{
		Now:        time.Now(),
		Step:       time.Second,
		IncludeEnd: true,
	}

	t, err := prometheus.ParseRequestTimeout(r)
	if err != nil {
		return params, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	params.Timeout = t
	instant := time.Now()
	if t := r.FormValue(timeParam); t != "" {
		instant, err = util.ParseTimeString(t)
		if err != nil {
			return params, xhttp.NewParseError(fmt.Errorf(formatErrStr, timeParam, err), http.StatusBadRequest)
		}
	}

	params.Start = instant
	params.End = instant

	query, err := parseQuery(r)
	if err != nil {
		return params, xhttp.NewParseError(fmt.Errorf(formatErrStr, queryParam, err), http.StatusBadRequest)
	}
	params.Query = query

	// Skip debug if unable to parse debug param
	debugVal := r.FormValue(debugParam)
	if debugVal != "" {
		debug, err := strconv.ParseBool(r.FormValue(debugParam))
		if err != nil {
			logging.WithContext(r.Context()).Warn("unable to parse debug flag", zap.Any("error", err))
		}

		params.Debug = debug
	}

	return params, nil
}

func parseQuery(r *http.Request) (string, error) {
	queries, ok := r.URL.Query()[queryParam]
	if !ok || len(queries) == 0 || queries[0] == "" {
		return "", errors.ErrNoQueryFound
	}

	// TODO: currently, we only support one target at a time
	if len(queries) > 1 {
		return "", errors.ErrBatchQuery
	}

	return queries[0], nil
}

func renderResultsJSON(
	w io.Writer,
	series []*ts.Series,
	params models.RequestParams,
) {
	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	jw.BeginObjectField("data")
	jw.BeginObject()

	jw.BeginObjectField("resultType")
	jw.WriteString("matrix")

	jw.BeginObjectField("result")
	jw.BeginArray()
	for _, s := range series {
		jw.BeginObject()
		jw.BeginObjectField("metric")
		jw.BeginObject()
		for _, t := range s.Tags.Tags {
			jw.BeginObjectField(string(t.Name))
			jw.WriteString(string(t.Value))
		}
		jw.EndObject()

		jw.BeginObjectField("values")
		jw.BeginArray()
		vals := s.Values()
		length := s.Len()
		for i := 0; i < length; i++ {
			dp := vals.DatapointAt(i)
			// Skip points before the query boundary. Ideal place to adjust these would be at the result node but that would make it inefficient
			// since we would need to create another block just for the sake of restricting the bounds.
			// Each series have the same start time so we just need to calculate the correct startIdx once
			// NB(r): Removing the optimization of computing startIdx once just in case our assumptions are wrong,
			// we can always add this optimization back later.  Without this code I see datapoints more often.
			if dp.Timestamp.Before(params.Start) {
				continue
			}

			jw.BeginArray()
			jw.WriteInt(int(dp.Timestamp.Unix()))
			jw.WriteString(utils.FormatFloat(dp.Value))
			jw.EndArray()
		}
		jw.EndArray()

		fixedStep, ok := s.Values().(ts.FixedResolutionMutableValues)
		if ok {
			jw.BeginObjectField("step_size_ms")
			jw.WriteInt(int(fixedStep.Resolution() / time.Millisecond))
		}
		jw.EndObject()
	}
	jw.EndArray()

	jw.EndObject()

	jw.EndObject()
	jw.Close()
}

func renderResultsInstantaneousJSON(
	w io.Writer,
	series []*ts.Series,
) {
	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	jw.BeginObjectField("data")
	jw.BeginObject()

	jw.BeginObjectField("resultType")
	jw.WriteString("vector")

	jw.BeginObjectField("result")
	jw.BeginArray()
	for _, s := range series {
		jw.BeginObject()
		jw.BeginObjectField("metric")
		jw.BeginObject()
		for _, t := range s.Tags.Tags {
			jw.BeginObjectField(string(t.Name))
			jw.WriteString(string(t.Value))
		}
		jw.EndObject()

		jw.BeginObjectField("value")
		vals := s.Values()
		length := s.Len()
		dp := vals.DatapointAt(length - 1)
		jw.BeginArray()
		jw.WriteInt(int(dp.Timestamp.Unix()))
		jw.WriteString(utils.FormatFloat(dp.Value))
		jw.EndArray()
		jw.EndObject()
	}
	jw.EndArray()

	jw.EndObject()

	jw.EndObject()
	jw.Close()
}

func renderM3QLResultsJSON(
	w io.Writer,
	series []*ts.Series,
	params models.RequestParams,
) {
	jw := json.NewWriter(w)
	jw.BeginArray()

	for _, s := range series {
		jw.BeginObject()
		jw.BeginObjectField("target")
		jw.WriteString(s.Name())

		jw.BeginObjectField("tags")
		jw.BeginObject()

		for _, tag := range s.Tags.Tags {
			jw.BeginObjectField(string(tag.Name))
			jw.WriteString(string(tag.Value))
		}

		jw.EndObject()

		jw.BeginObjectField("datapoints")
		jw.BeginArray()
		for i := 0; i < s.Len(); i++ {
			dp := s.Values().DatapointAt(i)
			jw.BeginArray()
			jw.WriteFloat64(dp.Value)
			jw.WriteInt(int(dp.Timestamp.Unix()))
			jw.EndArray()
		}
		jw.EndArray()

		jw.BeginObjectField("step_size_ms")
		jw.WriteInt(int(params.Step.Seconds() * 1000))

		jw.EndObject()
	}

	jw.EndArray()
	jw.Close()
}
