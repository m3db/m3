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

package handler

import (
	"net/http"
	"strings"

	"github.com/m3db/m3/src/query/block"
)

// HeaderKeyType is the type for the header key
type HeaderKeyType int

const (
	// HeaderKey is the key which headers will be added to in the request context
	HeaderKey HeaderKeyType = iota

	// RoutePrefixV1 is the v1 prefix for all coordinator routes
	RoutePrefixV1 = "/api/v1"

	// limitHeader is the header added when returned series are limited.
	limitHeader = "M3-Results-Limited"

	// limitHeaderSeriesLimitApplied is the header applied when fetch results are
	// maxed.
	limitHeaderSeriesLimitApplied = "max_fetch_series_limit_applied"
)

// AddWarningHeaders adds any warning headers present in the result's metadata.
// No-op if no warnings encountered.
func AddWarningHeaders(w http.ResponseWriter, meta block.ResultMetadata) {
	ex := meta.Exhaustive
	warns := len(meta.Warnings)
	if ex {
		warns++
	}

	if warns == 0 {
		return
	}

	warnings := make([]string, 0, warns)
	if ex {
		warnings = append(warnings, limitHeaderSeriesLimitApplied)
	}

	for _, warn := range meta.Warnings {
		warnings = append(warnings, warn.Header())
	}

	w.Header().Set(limitHeader, strings.Join(warnings, ","))
}
