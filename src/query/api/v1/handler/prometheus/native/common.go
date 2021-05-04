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
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util"
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

// RenderResultsOptions is a set of options for rendering the result.
type RenderResultsOptions struct {
	KeepNaNs                bool
	Start                   time.Time
	End                     time.Time
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
