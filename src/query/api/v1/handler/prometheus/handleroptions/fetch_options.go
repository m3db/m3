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

package handleroptions

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/headers"
)

type headerKey string

const (
	// RequestHeaderKey is the key which headers will be added to in the
	// request context.
	RequestHeaderKey headerKey = "RequestHeaderKey"
	// StepParam is the step parameter.
	StepParam = "step"
	// LookbackParam is the lookback parameter.
	LookbackParam = "lookback"
	// TimeoutParam is the timeout parameter.
	TimeoutParam = "timeout"
	maxInt64     = float64(math.MaxInt64)
	minInt64     = float64(math.MinInt64)
	maxTimeout   = 10 * time.Minute
)

// FetchOptionsBuilder builds fetch options based on a request and default
// config.
type FetchOptionsBuilder interface {
	// NewFetchOptions parses an http request into fetch options.
	NewFetchOptions(
		ctx context.Context,
		req *http.Request,
	) (context.Context, *storage.FetchOptions, error)
}

// FetchOptionsBuilderOptions provides options to use when creating a
// fetch options builder.
type FetchOptionsBuilderOptions struct {
	Limits        FetchOptionsBuilderLimitsOptions
	RestrictByTag *storage.RestrictByTag
	Timeout       time.Duration
}

// Validate validates the fetch options builder options.
func (o FetchOptionsBuilderOptions) Validate() error {
	return validateTimeout(o.Timeout)
}

// FetchOptionsBuilderLimitsOptions provides limits options to use when
// creating a fetch options builder.
type FetchOptionsBuilderLimitsOptions struct {
	SeriesLimit                 int
	DocsLimit                   int
	ReturnedSeriesLimit         int
	ReturnedDatapointsLimit     int
	ReturnedSeriesMetadataLimit int
	RequireExhaustive           bool
}

type fetchOptionsBuilder struct {
	opts FetchOptionsBuilderOptions
}

// NewFetchOptionsBuilder returns a new fetch options builder.
func NewFetchOptionsBuilder(
	opts FetchOptionsBuilderOptions,
) (FetchOptionsBuilder, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return fetchOptionsBuilder{opts: opts}, nil
}

// ParseLimit parses request limit from either header or query string.
func ParseLimit(req *http.Request, header, formValue string, defaultLimit int) (int, error) {
	if str := req.Header.Get(header); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return 0, err
		}
		return n, nil
	}

	if str := req.FormValue(formValue); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return 0, err
		}
		return n, nil
	}

	return defaultLimit, nil
}

// ParseRequireExhaustive parses request limit require exhaustive from header or
// query string.
func ParseRequireExhaustive(req *http.Request, defaultValue bool) (bool, error) {
	if str := req.Header.Get(headers.LimitRequireExhaustiveHeader); str != "" {
		v, err := strconv.ParseBool(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return false, err
		}
		return v, nil
	}

	if str := req.FormValue("requireExhaustive"); str != "" {
		v, err := strconv.ParseBool(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return false, err
		}
		return v, nil
	}

	return defaultValue, nil
}

// NewFetchOptions parses an http request into fetch options.
func (b fetchOptionsBuilder) NewFetchOptions(
	ctx context.Context,
	req *http.Request,
) (context.Context, *storage.FetchOptions, error) {
	ctx, fetchOpts, err := b.newFetchOptions(ctx, req)
	if err != nil {
		// Always invalid request if parsing fails params.
		return nil, nil, xerrors.NewInvalidParamsError(err)
	}
	return ctx, fetchOpts, nil
}

func (b fetchOptionsBuilder) newFetchOptions(
	ctx context.Context,
	req *http.Request,
) (context.Context, *storage.FetchOptions, error) {
	fetchOpts := storage.NewFetchOptions()

	if source := req.Header.Get(headers.SourceHeader); len(source) > 0 {
		fetchOpts.Source = []byte(source)
	}

	seriesLimit, err := ParseLimit(req, headers.LimitMaxSeriesHeader,
		"limit", b.opts.Limits.SeriesLimit)
	if err != nil {
		return nil, nil, err
	}

	fetchOpts.SeriesLimit = seriesLimit

	docsLimit, err := ParseLimit(req, headers.LimitMaxDocsHeader,
		"docsLimit", b.opts.Limits.DocsLimit)
	if err != nil {
		return nil, nil, err
	}

	fetchOpts.DocsLimit = docsLimit

	returnedSeriesLimit, err := ParseLimit(req, headers.LimitMaxReturnedSeriesHeader,
		"returnedSeriesLimit", b.opts.Limits.ReturnedSeriesLimit)
	if err != nil {
		return nil, nil, err
	}

	fetchOpts.ReturnedSeriesLimit = returnedSeriesLimit

	returnedDatapointsLimit, err := ParseLimit(req, headers.LimitMaxReturnedDatapointsHeader,
		"returnedDatapointsLimit", b.opts.Limits.ReturnedDatapointsLimit)
	if err != nil {
		return nil, nil, err
	}

	fetchOpts.ReturnedDatapointsLimit = returnedDatapointsLimit

	returnedSeriesMetadataLimit, err := ParseLimit(req, headers.LimitMaxReturnedSeriesMetadataHeader,
		"returnedSeriesMetadataLimit", b.opts.Limits.ReturnedSeriesMetadataLimit)
	if err != nil {
		return nil, nil, err
	}

	fetchOpts.ReturnedSeriesMetadataLimit = returnedSeriesMetadataLimit

	requireExhaustive, err := ParseRequireExhaustive(req, b.opts.Limits.RequireExhaustive)
	if err != nil {
		return nil, nil, err
	}

	fetchOpts.RequireExhaustive = requireExhaustive

	if str := req.Header.Get(headers.MetricsTypeHeader); str != "" {
		mt, err := storagemetadata.ParseMetricsType(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse metrics type: input=%s, err=%v", str, err)
			return nil, nil, err
		}

		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType =
			newOrExistingRestrictQueryOptionsRestrictByType(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType.MetricsType = mt
	}

	if str := req.Header.Get(headers.MetricsStoragePolicyHeader); str != "" {
		sp, err := policy.ParseStoragePolicy(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse storage policy: input=%s, err=%v", str, err)
			return nil, nil, err
		}

		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType =
			newOrExistingRestrictQueryOptionsRestrictByType(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType.StoragePolicy = sp
	}

	if str := req.Header.Get(headers.RestrictByTagsJSONHeader); str != "" {
		// Allow header to override any default restrict by tags config.
		var opts StringTagOptions
		if err := json.Unmarshal([]byte(str), &opts); err != nil {
			return nil, nil, err
		}

		tagOpts, err := opts.StorageOptions()
		if err != nil {
			return nil, nil, err
		}

		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByTag = tagOpts
	} else if defaultTagOpts := b.opts.RestrictByTag; defaultTagOpts != nil {
		// Apply defaults if not overridden by header.
		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByTag = defaultTagOpts
	}

	if restrict := fetchOpts.RestrictQueryOptions; restrict != nil {
		if err := restrict.Validate(); err != nil {
			err = fmt.Errorf(
				"could not validate restrict options: err=%v", err)
			return nil, nil, err
		}
	}

	// NB(r): Eventually all query parameters that are common across the
	// implementations should be parsed here so they can be fed to the engine.
	if step, ok, err := ParseStep(req); err != nil {
		err = fmt.Errorf(
			"could not parse step: err=%v", err)
		return nil, nil, err
	} else if ok {
		fetchOpts.Step = step
	}
	if lookback, ok, err := ParseLookbackDuration(req); err != nil {
		err = fmt.Errorf(
			"could not parse lookback: err=%v", err)
		return nil, nil, err
	} else if ok {
		fetchOpts.LookbackDuration = &lookback
	}

	fetchOpts.Timeout, err = ParseRequestTimeout(req, b.opts.Timeout)
	if err != nil {
		return nil, nil, fmt.Errorf("could not parse timeout: err=%w", err)
	}

	// Set timeout on the returned context.
	ctx, _ = contextWithRequestAndTimeout(ctx, req, fetchOpts)
	return ctx, fetchOpts, nil
}

func newOrExistingRestrictQueryOptions(
	fetchOpts *storage.FetchOptions,
) *storage.RestrictQueryOptions {
	if v := fetchOpts.RestrictQueryOptions; v != nil {
		return v
	}
	return &storage.RestrictQueryOptions{}
}

func newOrExistingRestrictQueryOptionsRestrictByType(
	fetchOpts *storage.FetchOptions,
) *storage.RestrictByType {
	if v := fetchOpts.RestrictQueryOptions.RestrictByType; v != nil {
		return v
	}
	return &storage.RestrictByType{}
}

// contextWithRequestAndTimeout sets up a context with the request's context
// and the configured timeout.
func contextWithRequestAndTimeout(
	ctx context.Context,
	r *http.Request,
	opts *storage.FetchOptions,
) (context.Context, context.CancelFunc) {
	ctx = context.WithValue(ctx, RequestHeaderKey, r.Header)
	return context.WithTimeout(ctx, opts.Timeout)
}

// ParseStep parses the step duration for an HTTP request.
func ParseStep(r *http.Request) (time.Duration, bool, error) {
	step := r.FormValue(StepParam)
	if step == "" {
		return 0, false, nil
	}
	value, err := parseStep(r, StepParam)
	if err != nil {
		return 0, false, err
	}
	return value, true, err
}

func parseStep(r *http.Request, key string) (time.Duration, error) {
	step, err := ParseDuration(r, key)
	if err != nil {
		return 0, err
	}
	if step <= 0 {
		return 0, fmt.Errorf("expected postive step size, instead got: %d", step)
	}
	return step, nil
}

// ParseLookbackDuration parses a lookback duration for an HTTP request.
func ParseLookbackDuration(r *http.Request) (time.Duration, bool, error) {
	lookback := r.FormValue(LookbackParam)
	if lookback == "" {
		return 0, false, nil
	}

	if lookback == StepParam {
		// Use the step size as lookback.
		step, err := parseStep(r, StepParam)
		if err != nil {
			return 0, false, err
		}
		return step, true, nil
	}

	// Otherwise it is specified as duration value.
	value, err := parseStep(r, LookbackParam)
	if err != nil {
		return 0, false, err
	}

	return value, true, nil
}

// ParseDuration parses a duration HTTP parameter.
// nolint: unparam
func ParseDuration(r *http.Request, key string) (time.Duration, error) {
	str := strings.TrimSpace(r.FormValue(key))
	if str == "" {
		return 0, errors.ErrNotFound
	}

	value, durationErr := time.ParseDuration(str)
	if durationErr == nil {
		return value, nil
	}

	// Try parsing as a float value specifying seconds, the Prometheus default.
	seconds, floatErr := strconv.ParseFloat(str, 64)
	if floatErr == nil {
		ts := seconds * float64(time.Second)
		if ts > maxInt64 || ts < minInt64 {
			return 0, fmt.Errorf("cannot parse duration='%s': as_float_err="+
				"int64 overflow after float conversion", str)
		}

		return time.Duration(ts), nil
	}

	return 0, fmt.Errorf("cannot parse duration='%s': as_duration_err=%s, as_float_err=%s",
		str, durationErr, floatErr)
}

// ParseRequestTimeout parses the input request timeout with a default.
func ParseRequestTimeout(
	r *http.Request,
	configFetchTimeout time.Duration,
) (time.Duration, error) {
	var timeout string
	if v := r.FormValue(TimeoutParam); v != "" {
		timeout = v
	}
	// Note: Header should take precedence.
	if v := r.Header.Get(TimeoutParam); v != "" {
		timeout = v
	}
	// Prefer the M3-Timeout header to the incorrect header using the param name. The param name should have never been
	// allowed as a header, but we continue to support it for backwards compatibility.
	if v := r.Header.Get(headers.TimeoutHeader); v != "" {
		timeout = v
	}

	if timeout == "" {
		return configFetchTimeout, nil
	}

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		return 0, xerrors.NewInvalidParamsError(
			fmt.Errorf("invalid 'timeout': %v", err))
	}

	if err := validateTimeout(duration); err != nil {
		return 0, err
	}

	return duration, nil
}

func validateTimeout(v time.Duration) error {
	if v <= 0 {
		return xerrors.NewInvalidParamsError(
			fmt.Errorf("invalid 'timeout': less than or equal to zero %v", v))
	}
	if v > maxTimeout {
		return xerrors.NewInvalidParamsError(
			fmt.Errorf("invalid 'timeout': %v greater than max %v", v, maxTimeout))
	}
	return nil
}
