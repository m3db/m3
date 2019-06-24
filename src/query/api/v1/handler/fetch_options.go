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

package handler

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// FetchOptionsBuilder builds fetch options based on a request and default
// config.
type FetchOptionsBuilder interface {
	NewFetchOptions(req *http.Request) (*storage.FetchOptions, *xhttp.ParseError)
}

// FetchOptionsBuilderOptions provides options to use when creating a
// fetch options builder.
type FetchOptionsBuilderOptions struct {
	Limit int
}

type fetchOptionsBuilder struct {
	opts FetchOptionsBuilderOptions
}

// NewFetchOptionsBuilder returns a new fetch options builder.
func NewFetchOptionsBuilder(
	opts FetchOptionsBuilderOptions,
) FetchOptionsBuilder {
	return fetchOptionsBuilder{opts: opts}
}

func (b fetchOptionsBuilder) NewFetchOptions(
	req *http.Request,
) (*storage.FetchOptions, *xhttp.ParseError) {
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.Limit = b.opts.Limit
	if str := req.Header.Get(LimitMaxSeriesHeader); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
		fetchOpts.Limit = n
	}
	if str := req.Header.Get(MetricsTypeHeader); str != "" {
		mt, err := storage.ParseMetricsType(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse metrics type: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
		fetchOpts.RestrictFetchOptions = newOrExistingRestrictFetchOptions(fetchOpts)
		fetchOpts.RestrictFetchOptions.MetricsType = mt
	}
	if str := req.Header.Get(MetricsStoragePolicyHeader); str != "" {
		sp, err := policy.ParseStoragePolicy(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse storage policy: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
		fetchOpts.RestrictFetchOptions = newOrExistingRestrictFetchOptions(fetchOpts)
		fetchOpts.RestrictFetchOptions.StoragePolicy = sp
	}
	if restrict := fetchOpts.RestrictFetchOptions; restrict != nil {
		if err := restrict.Validate(); err != nil {
			err = fmt.Errorf(
				"could not validate restrict options: err=%v", err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}
	return fetchOpts, nil
}

func newOrExistingRestrictFetchOptions(
	fetchOpts *storage.FetchOptions,
) *storage.RestrictFetchOptions {
	if v := fetchOpts.RestrictFetchOptions; v != nil {
		return v
	}
	return &storage.RestrictFetchOptions{}
}
