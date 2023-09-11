// Copyright (c) 2017 Uber Technologies, Inc.
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

package r2

import (
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/ctl/auth"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

type r2HandlerFunc func(http.ResponseWriter, *http.Request) error

type r2Handler struct {
	logger *zap.Logger
	auth   auth.HTTPAuthService
}

func (h r2Handler) wrap(authType auth.AuthorizationType, fn r2HandlerFunc) http.Handler {
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			h.handleError(w, err)
		}
	})
	return h.auth.NewAuthHandler(authType, f, writeAPIResponse)
}

func (h r2Handler) handleError(w http.ResponseWriter, opError error) {
	h.logger.Error(opError.Error())

	var err error
	switch opError.(type) {
	case conflictError:
		err = writeAPIResponse(w, http.StatusConflict, opError.Error())
	case badInputError:
		err = writeAPIResponse(w, http.StatusBadRequest, opError.Error())
	case versionError:
		err = writeAPIResponse(w, http.StatusConflict, opError.Error())
	case notFoundError:
		err = writeAPIResponse(w, http.StatusNotFound, opError.Error())
	case authError:
		err = writeAPIResponse(w, http.StatusUnauthorized, opError.Error())
	default:
		err = writeAPIResponse(w, http.StatusInternalServerError, opError.Error())
	}

	// Getting here means that the error handling failed. Trying to convey what was supposed to happen.
	if err != nil {
		msg := fmt.Sprintf("Could not generate error response for: %s", opError.Error())
		h.logger.Error(msg)
		xhttp.WriteError(w, err)
	}
}
