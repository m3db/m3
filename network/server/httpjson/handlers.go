// Copyright (c) 2016 Uber Technologies, Inc.
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

package httpjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/m3db/m3x/errors"

	"github.com/uber/tchannel-go/thrift"
)

var (
	errRequestMustBePost  = xerrors.NewInvalidParamsError(errors.New("request must be POST"))
	errInvalidRequestBody = xerrors.NewInvalidParamsError(errors.New("request contains an invalid request body"))
	errEncodeResponseBody = errors.New("failed to encode response body")
)

type respSuccess struct {
}

type respErrorResult struct {
	Error respError `json:"error"`
}

type respError struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// RegisterHandlers will register handlers on the HTTP serve mux for a given service and options
func RegisterHandlers(mux *http.ServeMux, service interface{}, opts ServerOptions) error {
	v := reflect.ValueOf(service)
	t := v.Type()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		// Ensure this method is of either:
		// - methodName(RequestObject) error
		// - methodName(RequestObject) (ResultObject, error)

		// TODO(xichen): make HTTP server understand truncate method.
		// TODO(r): make the following work so that health endpoint is registered,
		// also perhaps make these GET?
		// - methodName() error
		// - methodName() (ResultObject, error)
		if method.Type.NumIn() != 3 || !(method.Type.NumOut() == 1 || method.Type.NumOut() == 2) {
			continue
		}

		obj := method.Type.In(0)
		context := method.Type.In(1)
		reqIn := method.Type.In(2)
		var resultOut, resultErr reflect.Type
		if method.Type.NumOut() == 1 {
			resultErr = method.Type.Out(0)
		} else {
			resultOut = method.Type.Out(0)
			resultErr = method.Type.Out(1)
		}

		if obj != t {
			continue
		}

		contextInterfaceType := reflect.TypeOf((*thrift.Context)(nil)).Elem()
		if context.Kind() != reflect.Interface || !context.Implements(contextInterfaceType) {
			continue
		}

		if reqIn.Kind() != reflect.Ptr || reqIn.Elem().Kind() != reflect.Struct {
			continue
		}

		if method.Type.NumOut() == 2 {
			if resultOut.Kind() != reflect.Ptr || resultOut.Elem().Kind() != reflect.Struct {
				continue
			}
		}

		errInterfaceType := reflect.TypeOf((*error)(nil)).Elem()
		if resultErr.Kind() != reflect.Interface || !resultErr.Implements(errInterfaceType) {
			continue
		}

		name := strings.ToLower(method.Name)
		mux.HandleFunc(fmt.Sprintf("/%s", name), func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.ToLower(r.Method) != "post" {
				writeError(w, errRequestMustBePost)
				return
			}

			in := reflect.New(reqIn.Elem()).Interface()
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(in); err != nil {
				writeError(w, errInvalidRequestBody)
				return
			}

			svc := reflect.ValueOf(service)
			callContext, _ := thrift.NewContext(opts.GetRequestTimeout())
			ctx := reflect.ValueOf(callContext)
			ret := method.Func.Call([]reflect.Value{svc, ctx, reflect.ValueOf(in)})
			if method.Type.NumOut() == 1 {
				// Deal with error case
				if !ret[0].IsNil() {
					writeError(w, ret[0].Interface())
					return
				}
				json.NewEncoder(w).Encode(&respSuccess{})
				return
			}

			// Deal with error case
			if !ret[1].IsNil() {
				writeError(w, ret[1].Interface())
				return
			}

			buff := bytes.NewBuffer(nil)
			if err := json.NewEncoder(buff).Encode(ret[0].Interface()); err != nil {
				writeError(w, errEncodeResponseBody)
				return
			}

			w.Write(buff.Bytes())
		})
	}
	return nil
}

func writeError(w http.ResponseWriter, errValue interface{}) {
	result := respErrorResult{respError{}}
	if value, ok := errValue.(error); ok {
		result.Error.Message = value.Error()
	} else if value, ok := errValue.(fmt.Stringer); ok {
		result.Error.Message = value.String()
	}
	result.Error.Data = errValue

	buff := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buff).Encode(&result); err != nil {
		// Not a JSON returnable error
		w.WriteHeader(http.StatusInternalServerError)
		result.Error.Message = fmt.Sprintf("%v", errValue)
		result.Error.Data = nil
		json.NewEncoder(w).Encode(&result)
		return
	}

	if value, ok := errValue.(error); ok && xerrors.IsInvalidParams(value) {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buff.Bytes())
}
