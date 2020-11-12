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

package handleroptions

import (
	"net/http"
	"strings"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/headers"
)

// AddResponseHeaders adds any warning headers present in the result's metadata,
// and also effective parameters relative to the request such as effective
// timeout in use.
func AddResponseHeaders(
	w http.ResponseWriter,
	meta block.ResultMetadata,
	fetchOpts *storage.FetchOptions,
) {
	if fetchOpts != nil {
		w.Header().Set(headers.TimeoutHeader, fetchOpts.Timeout.String())
	}

	ex := meta.Exhaustive
	warns := len(meta.Warnings)
	if !ex {
		warns++
	}

	if warns == 0 {
		return
	}

	warnings := make([]string, 0, warns)
	if !ex {
		warnings = append(warnings, headers.LimitHeaderSeriesLimitApplied)
	}

	for _, warn := range meta.Warnings {
		warnings = append(warnings, warn.Header())
	}

	w.Header().Set(headers.LimitHeader, strings.Join(warnings, ","))
}
