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

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/query/util/logging"

	"go.uber.org/zap"
)

const (
	endParam          = "end"
	startParam        = "start"
	targetParam       = "target"
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
	if d := r.FormValue(key); d != "" {
		return time.ParseDuration(d)
	}

	return 0, errors.ErrNotFound
}

// parseParams parses all params from the GET request
func parseParams(r *http.Request) (models.RequestParams, *handler.ParseError) {
	params := models.RequestParams{
		Now: time.Now(),
	}

	t, err := prometheus.ParseRequestTimeout(r)
	if err != nil {
		return params, handler.NewParseError(err, http.StatusBadRequest)
	}
	params.Timeout = t

	start, err := parseTime(r, startParam)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, startParam, err), http.StatusBadRequest)
	}
	params.Start = start

	end, err := parseTime(r, endParam)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, endParam, err), http.StatusBadRequest)
	}
	params.End = end

	step, err := parseDuration(r, stepParam)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, stepParam, err), http.StatusBadRequest)
	}
	params.Step = step

	target, err := parseTarget(r)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, targetParam, err), http.StatusBadRequest)
	}
	params.Target = target

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

func parseTarget(r *http.Request) (string, error) {
	targetQueries, ok := r.URL.Query()[targetParam]
	if !ok || len(targetQueries) == 0 || targetQueries[0] == "" {
		return "", errors.ErrNoTargetFound
	}

	// TODO: currently, we only support one target at a time
	if len(targetQueries) > 1 {
		return "", errors.ErrBatchQuery
	}

	return targetQueries[0], nil
}

func renderResultsJSON(w io.Writer, series []*ts.Series, params models.RequestParams) {
	startIdx := 0
	jw := json.NewWriter(w)
	jw.BeginArray()
	for _, s := range series {
		jw.BeginObject()
		jw.BeginObjectField("target")
		jw.WriteString(s.Name())

		jw.BeginObjectField("tags")
		jw.BeginObject()
		for _, t := range s.Tags {
			jw.BeginObjectField(t.Name)
			jw.WriteString(t.Value)
		}
		jw.EndObject()

		jw.BeginObjectField("datapoints")
		jw.BeginArray()
		vals := s.Values()
		for i := startIdx; i < s.Len(); i++ {
			dp := vals.DatapointAt(i)
			// Skip points before the query boundary. Ideal place to adjust these would be at the result node but that would make it inefficient
			// since we would need to create another block just for the sake of restricting the bounds.
			// Each series have the same start time so we just need to calculate the correct startIdx once
			if dp.Timestamp.Before(params.Start) {
				startIdx = i + 1
				continue
			}

			jw.BeginArray()
			jw.WriteFloat64(dp.Value)
			jw.WriteInt(int(dp.Timestamp.Unix()))
			jw.EndArray()
		}
		jw.EndArray()

		fixedStep, ok := s.Values().(ts.FixedResolutionMutableValues)
		if ok {
			jw.BeginObjectField("step_size_ms")
			jw.WriteInt(int(util.DurationToMS(fixedStep.MillisPerStep())))
			jw.EndObject()
		}
	}

	jw.EndArray()
	jw.Close()
}
