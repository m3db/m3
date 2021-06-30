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
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// QueryParam is the name of the query form/url parameter
	QueryParam = "query"

	endParam          = "end"
	startParam        = "start"
	timeParam         = "time"
	debugParam        = "debug"
	endExclusiveParam = "end-exclusive"
	blockTypeParam    = "block-type"

	formatErrStr = "error parsing param: %s, error: %v"
	nowTimeValue = "now"
)

// ParseTime parses a time out of a request key, with a default value.
func ParseTime(r *http.Request, key string, now time.Time) (time.Time, error) {
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
		params.Now, err = ParseTime(r, timeParam, params.Now)
		if err != nil {
			err = fmt.Errorf(formatErrStr, timeParam, err)
			return params, xerrors.NewInvalidParamsError(err)
		}
	}

	start, err := ParseTime(r, startParam, params.Now)
	if err != nil {
		err = fmt.Errorf(formatErrStr, startParam, err)
		return params, xerrors.NewInvalidParamsError(err)
	}

	params.Start = xtime.ToUnixNano(start)
	end, err := ParseTime(r, endParam, params.Now)
	if err != nil {
		err = fmt.Errorf(formatErrStr, endParam, err)
		return params, xerrors.NewInvalidParamsError(err)
	}
	if start.After(end) {
		err = fmt.Errorf("start (%s) must be before end (%s)", start, end)
		return params, xerrors.NewInvalidParamsError(err)
	}
	params.End = xtime.ToUnixNano(end)

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

	query, err := ParseQuery(r)
	if err != nil {
		return params, xerrors.NewInvalidParamsError(
			fmt.Errorf(formatErrStr, QueryParam, err))
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

// ParseQuery parses a query out of an HTTP request.
func ParseQuery(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", err
	}

	// NB(schallert): r.Form is generic over GET and POST requests, with body
	// parameters taking precedence over URL parameters (see r.ParseForm() docs
	// for more details). We depend on the generic behavior for properly parsing
	// POST and GET queries.
	queries, ok := r.Form[QueryParam]
	if !ok || len(queries) == 0 || queries[0] == "" {
		return "", errors.ErrNoQueryFound
	}

	// TODO: currently, we only support one target at a time
	if len(queries) > 1 {
		return "", errors.ErrBatchQuery
	}

	return queries[0], nil
}

// RenderResultsOptions is a set of options for rendering the result.
type RenderResultsOptions struct {
	KeepNaNs                bool
	Start                   xtime.UnixNano
	End                     xtime.UnixNano
	ReturnedSeriesLimit     int
	ReturnedDatapointsLimit int
}

// RenderResultsResult is the result from rendering results.
type RenderResultsResult struct {
	// Datapoints is the count of datapoints rendered.
	Datapoints int
	// Series is the count of series rendered.
	Series int
	// TotalSeries is the count of series in total.
	TotalSeries int
	// LimitedMaxReturnedData indicates if the results rendering
	// was truncated by a limit on returned series or datapoints.
	LimitedMaxReturnedData bool
}

// RenderResultsJSON renders results in JSON for range queries.
func RenderResultsJSON(
	jw json.Writer,
	result ReadResult,
	opts RenderResultsOptions,
) RenderResultsResult {
	var (
		series             = result.Series
		warnings           = result.Meta.WarningStrings()
		seriesRendered     = 0
		datapointsRendered = 0
		limited            = false
	)

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
		vals := s.Values()
		length := s.Len()

		// If a limit of the number of datapoints is present, then write
		// out series' data up until that limit is hit.
		if opts.ReturnedSeriesLimit > 0 && seriesRendered+1 > opts.ReturnedSeriesLimit {
			limited = true
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && datapointsRendered+length > opts.ReturnedDatapointsLimit {
			limited = true
			break
		}

		hasData := false
		for i := 0; i < length; i++ {
			dp := vals.DatapointAt(i)

			// If keepNaNs is set to false and the value is NaN, drop it from the response.
			// If the series has no datapoints at all then this datapoint iteration will
			// count zero total and end up skipping writing the series entirely.
			if !opts.KeepNaNs && math.IsNaN(dp.Value) {
				continue
			}

			// Skip points before the query boundary. Ideal place to adjust these
			// would be at the result node but that would make it inefficient since
			// we would need to create another block just for the sake of restricting
			// the bounds.
			if dp.Timestamp.Before(opts.Start) || dp.Timestamp.After(opts.End) {
				continue
			}

			// On first datapoint for the series, write out the series beginning content.
			if !hasData {
				jw.BeginObject()
				jw.BeginObjectField("metric")
				jw.BeginObject()
				for _, t := range s.Tags.Tags {
					jw.BeginObjectBytesField(t.Name)
					jw.WriteBytesString(t.Value)
				}
				jw.EndObject()

				jw.BeginObjectField("values")
				jw.BeginArray()

				seriesRendered++
				hasData = true
			}
			datapointsRendered++

			jw.BeginArray()
			jw.WriteInt(int(dp.Timestamp.Seconds()))
			jw.WriteString(utils.FormatFloat(dp.Value))
			jw.EndArray()
		}

		if !hasData {
			// No datapoints written for series so continue to
			// next instead of writing the end content.
			continue
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
	return RenderResultsResult{
		Series:                 seriesRendered,
		Datapoints:             datapointsRendered,
		TotalSeries:            len(series),
		LimitedMaxReturnedData: limited,
	}
}

// renderResultsInstantaneousJSON renders results in JSON for instant queries.
func renderResultsInstantaneousJSON(
	jw json.Writer,
	result ReadResult,
	opts RenderResultsOptions,
) RenderResultsResult {
	var (
		series        = result.Series
		warnings      = result.Meta.WarningStrings()
		isScalar      = result.BlockType == block.BlockScalar || result.BlockType == block.BlockTime
		keepNaNs      = opts.KeepNaNs
		returnedCount = 0
		limited       = false
	)

	resultType := "vector"
	if isScalar {
		resultType = "scalar"
	}

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

		if opts.ReturnedSeriesLimit > 0 && returnedCount >= opts.ReturnedSeriesLimit {
			limited = true
			break
		}
		if opts.ReturnedDatapointsLimit > 0 && returnedCount >= opts.ReturnedDatapointsLimit {
			limited = true
			break
		}

		if isScalar {
			jw.WriteInt(int(dp.Timestamp.Seconds()))
			jw.WriteString(utils.FormatFloat(dp.Value))
			returnedCount++
			continue
		}

		// If keepNaNs is set to false and the value is NaN, drop it from the response.
		if !keepNaNs && math.IsNaN(dp.Value) {
			continue
		}

		returnedCount++

		jw.BeginObject()
		jw.BeginObjectField("metric")
		jw.BeginObject()
		for _, t := range s.Tags.Tags {
			jw.BeginObjectBytesField(t.Name)
			jw.WriteBytesString(t.Value)
		}
		jw.EndObject()

		jw.BeginObjectField("value")
		jw.BeginArray()
		jw.WriteInt(int(dp.Timestamp.Seconds()))
		jw.WriteString(utils.FormatFloat(dp.Value))
		jw.EndArray()
		jw.EndObject()
	}
	jw.EndArray()

	jw.EndObject()

	jw.EndObject()

	return RenderResultsResult{
		LimitedMaxReturnedData: limited,
		// Series and datapoints are the same count for instant
		// queries since a series has one datapoint.
		Datapoints:  returnedCount,
		Series:      returnedCount,
		TotalSeries: len(series),
	}
}
