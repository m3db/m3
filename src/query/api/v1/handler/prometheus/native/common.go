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
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/json"
	xerrors "github.com/m3db/m3/src/x/errors"
)

const (
	endParam          = "end"
	startParam        = "start"
	timeParam         = "time"
	queryParam        = "query"
	debugParam        = "debug"
	endExclusiveParam = "end-exclusive"
	blockTypeParam    = "block-type"

	formatErrStr = "error parsing param: %s, error: %v"
	nowTimeValue = "now"
)

func parseTime(r *http.Request, key string, now time.Time) (time.Time, error) {
	if t := r.FormValue(key); t != "" {
		if t == nowTimeValue {
			return now, nil
		}
		return util.ParseTimeString(t)
	}
	return time.Time{}, errors.ErrNotFound
}

// parseParams parses all params from the GET request
func parseParams(
	r *http.Request,
	engineOpts executor.EngineOptions,
	fetchOpts *storage.FetchOptions,
) (models.RequestParams, error) {
	var params models.RequestParams

	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf(formatErrStr, timeParam, err)
		return params, xerrors.NewInvalidParamsError(err)
	}

	params.Now = time.Now()
	if v := r.FormValue(timeParam); v != "" {
		var err error
		params.Now, err = parseTime(r, timeParam, params.Now)
		if err != nil {
			err = fmt.Errorf(formatErrStr, timeParam, err)
			return params, xerrors.NewInvalidParamsError(err)
		}
	}

	start, err := parseTime(r, startParam, params.Now)
	if err != nil {
		err = fmt.Errorf(formatErrStr, startParam, err)
		return params, xerrors.NewInvalidParamsError(err)
	}

	params.Start = start
	end, err := parseTime(r, endParam, params.Now)
	if err != nil {
		err = fmt.Errorf(formatErrStr, endParam, err)
		return params, xerrors.NewInvalidParamsError(err)
	}
	if start.After(end) {
		err = fmt.Errorf("start (%s) must be before end (%s)", start, end)
		return params, xerrors.NewInvalidParamsError(err)
	}
	params.End = end

	timeout := fetchOpts.Timeout
	if timeout <= 0 {
		err := fmt.Errorf("expected positive timeout, instead got: %d", timeout)
		return params, xerrors.NewInvalidParamsError(
			fmt.Errorf(formatErrStr, handleroptions.TimeoutParam, err))
	}
	params.Timeout = timeout

	step := fetchOpts.Step
	if step <= 0 {
		err := fmt.Errorf("expected positive step size, instead got: %d", step)
		return params, xerrors.NewInvalidParamsError(
			fmt.Errorf(formatErrStr, handleroptions.StepParam, err))
	}
	params.Step = fetchOpts.Step

	query, err := parseQuery(r)
	if err != nil {
		return params, xerrors.NewInvalidParamsError(
			fmt.Errorf(formatErrStr, queryParam, err))
	}
	params.Query = query

	if debugVal := r.FormValue(debugParam); debugVal != "" {
		params.Debug, err = strconv.ParseBool(debugVal)
		if err != nil {
			return params, xerrors.NewInvalidParamsError(
				fmt.Errorf(formatErrStr, debugParam, err))
		}
	}

	params.BlockType = models.TypeSingleBlock
	if blockType := r.FormValue(blockTypeParam); blockType != "" {
		intVal, err := strconv.ParseInt(blockType, 10, 8)
		if err != nil {
			return params, xerrors.NewInvalidParamsError(
				fmt.Errorf(formatErrStr, blockTypeParam, err))
		}

		blockType := models.FetchedBlockType(intVal)

		// Ignore error from receiving an invalid block type, and return default.
		if err := blockType.Validate(); err != nil {
			return params, xerrors.NewInvalidParamsError(
				fmt.Errorf(formatErrStr, blockTypeParam, err))
		}

		params.BlockType = blockType
	}

	// Default to including end if unable to parse the flag
	endExclusiveVal := r.FormValue(endExclusiveParam)
	params.IncludeEnd = true
	if endExclusiveVal != "" {
		excludeEnd, err := strconv.ParseBool(endExclusiveVal)
		if err != nil {
			return params, xerrors.NewInvalidParamsError(
				fmt.Errorf(formatErrStr, endExclusiveParam, err))
		}

		params.IncludeEnd = !excludeEnd
	}

	params.LookbackDuration = engineOpts.LookbackDuration()
	if v := fetchOpts.LookbackDuration; v != nil {
		params.LookbackDuration = *v
	}

	return params, nil
}

// parseInstantaneousParams parses all params from the GET request
func parseInstantaneousParams(
	r *http.Request,
	engineOpts executor.EngineOptions,
	fetchOpts *storage.FetchOptions,
) (models.RequestParams, error) {
	if err := r.ParseForm(); err != nil {
		return models.RequestParams{}, xerrors.NewInvalidParamsError(err)
	}

	if fetchOpts.Step == 0 {
		fetchOpts.Step = time.Second
	}

	r.Form.Set(startParam, nowTimeValue)
	r.Form.Set(endParam, nowTimeValue)
	params, err := parseParams(r, engineOpts, fetchOpts)
	if err != nil {
		return params, err
	}

	return params, nil
}

func parseQuery(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", err
	}

	// NB(schallert): r.Form is generic over GET and POST requests, with body
	// parameters taking precedence over URL parameters (see r.ParseForm() docs
	// for more details). We depend on the generic behavior for properly parsing
	// POST and GET queries.
	queries, ok := r.Form[queryParam]
	if !ok || len(queries) == 0 || queries[0] == "" {
		return "", errors.ErrNoQueryFound
	}

	// TODO: currently, we only support one target at a time
	if len(queries) > 1 {
		return "", errors.ErrBatchQuery
	}

	return queries[0], nil
}

func filterNaNSeries(
	series []*ts.Series,
	startInclusive time.Time,
	endInclusive time.Time,
) []*ts.Series {
	filtered := series[:0]
	for _, s := range series {
		dps := s.Values().Datapoints()
		hasVal := false
		for _, dp := range dps {
			if !math.IsNaN(dp.Value) {
				ts := dp.Timestamp
				if ts.Before(startInclusive) || ts.After(endInclusive) {
					continue
				}

				hasVal = true
				break
			}
		}

		if hasVal {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

// RenderResultsOptions is a set of options for rendering the result.
type RenderResultsOptions struct {
	KeepNaNs bool
	Start    time.Time
	End      time.Time
}

// RenderResultsJSON renders results in JSON for range queries.
func RenderResultsJSON(
	w io.Writer,
	result ReadResult,
	opts RenderResultsOptions,
) error {
	var (
		series   = result.Series
		warnings = result.Meta.WarningStrings()
	)

	// NB: if dropping NaNs, drop series with only NaNs from output entirely.
	if !opts.KeepNaNs {
		series = filterNaNSeries(series, opts.Start, opts.End)
	}

	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	if len(warnings) > 0 {
		jw.BeginObjectField("warnings")
		jw.BeginArray()
		for _, warn := range warnings {
			jw.WriteString(warn)
		}

		jw.EndArray()
	}

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

			// If keepNaNs is set to false and the value is NaN, drop it from the response.
			if !opts.KeepNaNs && math.IsNaN(dp.Value) {
				continue
			}

			// Skip points before the query boundary. Ideal place to adjust these
			// would be at the result node but that would make it inefficient since
			// we would need to create another block just for the sake of restricting
			// the bounds.
			if dp.Timestamp.Before(opts.Start) {
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
	return jw.Close()
}

// renderResultsInstantaneousJSON renders results in JSON for instant queries.
func renderResultsInstantaneousJSON(
	w io.Writer,
	result ReadResult,
	keepNaNs bool,
) {
	var (
		series   = result.Series
		warnings = result.Meta.WarningStrings()
		isScalar = result.BlockType == block.BlockScalar || result.BlockType == block.BlockTime
	)

	resultType := "vector"
	if isScalar {
		resultType = "scalar"
	}

	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	if len(warnings) > 0 {
		jw.BeginObjectField("warnings")
		jw.BeginArray()
		for _, warn := range warnings {
			jw.WriteString(warn)
		}

		jw.EndArray()
	}

	jw.BeginObjectField("data")
	jw.BeginObject()

	jw.BeginObjectField("resultType")
	jw.WriteString(resultType)

	jw.BeginObjectField("result")
	jw.BeginArray()
	for _, s := range series {
		vals := s.Values()
		length := s.Len()
		dp := vals.DatapointAt(length - 1)

		if isScalar {
			jw.WriteInt(int(dp.Timestamp.Unix()))
			jw.WriteString(utils.FormatFloat(dp.Value))
			continue
		}

		// If keepNaNs is set to false and the value is NaN, drop it from the response.
		if !keepNaNs && math.IsNaN(dp.Value) {
			continue
		}

		jw.BeginObject()
		jw.BeginObjectField("metric")
		jw.BeginObject()
		for _, t := range s.Tags.Tags {
			jw.BeginObjectField(string(t.Name))
			jw.WriteString(string(t.Value))
		}
		jw.EndObject()

		jw.BeginObjectField("value")
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
