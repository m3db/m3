// Copyright (c) 2021 Uber Technologies, Inc.
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

package middleware

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/source"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

var errInvalidSourceHeader = xhttp.NewError(
	fmt.Errorf("invalid %s header", headers.SourceHeader),
	http.StatusBadRequest)

// SourceOptions are the options for the source middleware.
type SourceOptions struct {
	Deserializer source.Deserializer
}

// Source adds the headers.SourceHeader value to the request context.
// Installing this middleware function allows application code to access the typed source value using FromContext.
// Additionally a source log field is added to the request scope logger.
func Source(opts Options) mux.MiddlewareFunc {
	return func(base http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hs := r.Header.Get(headers.SourceHeader)
			if len(hs) == 0 {
				// bail early if the header is not set on the request.
				base.ServeHTTP(w, r)
				return
			}
			iOpts := opts.InstrumentOpts
			s := []byte(hs)
			ctx := logging.NewContext(r.Context(), iOpts, zap.ByteString("source", s))
			l := logging.WithContext(ctx, iOpts)
			ctx, err := source.NewContext(ctx, s, opts.Source.Deserializer)
			if err != nil {
				l.Error("failed to deserialize source", zap.Error(err))
				xhttp.WriteError(w, errInvalidSourceHeader)
				return
			}
			base.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
