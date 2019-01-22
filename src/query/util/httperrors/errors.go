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

package httperrors

import (
	"net/http"

	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"
)

type errorWithID struct {
	xhttp.ErrorResponse
	RqID string `json:"rqID"`
}

// ErrorWithReqID writes an xhttp.ErrorResponse with an added request id (RqId) field read from the request
// context. NB: RqID is currently a query specific concept, which is why this doesn't exist in xhttp proper.
// We can add it later if we propagate the request id concept to that package as well.
func ErrorWithReqID(w http.ResponseWriter, r *http.Request, err error, code int) {
	ctx := r.Context()
	w.WriteHeader(code)
	xhttp.WriteJSONResponse(w, errorWithID{
		ErrorResponse: xhttp.ErrorResponse{
			Error: err.Error(),
		},
		RqID: logging.ReadContextID(ctx),
	}, logging.WithContext(ctx))
}
