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
	"strconv"
	"strings"

	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/headers"

	apachethrift "github.com/uber/tchannel-go/thirdparty/github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/tchannel-go/thrift"
)

var (
	errRequestMustBeGet  = xerrors.NewInvalidParamsError(errors.New("request without request params must be GET"))
	errRequestMustBePost = xerrors.NewInvalidParamsError(errors.New("request with request params must be POST"))
)

// Error is an HTTP JSON error that also sets a return status code.
type Error interface {
	error

	StatusCode() int
}

type errorType struct {
	error
	statusCode int
}

// NewError creates a new HTTP JSON error which has a specified status code.
func NewError(err error, statusCode int) Error {
	e := errorType{error: err}
	e.statusCode = statusCode
	return e
}

// StatusCode returns the HTTP status code that matches the error.
func (e errorType) StatusCode() int {
	return e.statusCode
}

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
	contextFn := opts.ContextFn()
	postResponseFn := opts.PostResponseFn()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)

		// Ensure this method is of either:
		// - methodName(RequestObject) error
		// - methodName(RequestObject) (ResultObject, error)
		// - methodName() error
		// - methodName() (ResultObject, error)
		if !(method.Type.NumIn() == 2 || method.Type.NumIn() == 3) ||
			!(method.Type.NumOut() == 1 || method.Type.NumOut() == 2) {
			continue
		}

		var reqIn reflect.Type
		obj := method.Type.In(0)
		context := method.Type.In(1)
		if method.Type.NumIn() == 3 {
			reqIn = method.Type.In(2)
		}

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

		if method.Type.NumIn() == 3 {
			if reqIn.Kind() != reflect.Ptr || reqIn.Elem().Kind() != reflect.Struct {
				continue
			}
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

			// Always close the request body
			defer r.Body.Close()

			httpMethod := strings.ToUpper(r.Method)
			if reqIn == nil && httpMethod != "GET" {
				writeError(w, errRequestMustBeGet)
				return
			}
			if reqIn != nil && httpMethod != "POST" {
				writeError(w, errRequestMustBePost)
				return
			}

			httpHeaders := make(map[string]string)
			for key, values := range r.Header {
				if len(values) > 0 {
					httpHeaders[key] = values[0]
				}
			}

			var in interface{}
			if reqIn != nil {
				in = reflect.New(reqIn.Elem()).Interface()
				decoder := json.NewDecoder(r.Body)
				disableDisallowUnknownFields, err := strconv.ParseBool(
					r.Header.Get(headers.JSONDisableDisallowUnknownFields))
				if err != nil || !disableDisallowUnknownFields {
					decoder.DisallowUnknownFields()
				}
				if err := decoder.Decode(in); err != nil {
					err := fmt.Errorf("invalid request body: %v", err)
					writeError(w, xerrors.NewInvalidParamsError(err))
					return
				}
			}

			// Prepare the call context
			callContext, _ := thrift.NewContext(opts.RequestTimeout())
			if contextFn != nil {
				// Allow derivation of context if context fn is set
				callContext = contextFn(callContext, method.Name, httpHeaders)
			}
			// Always set headers finally
			callContext = thrift.WithHeaders(callContext, httpHeaders)

			var (
				svc = reflect.ValueOf(service)
				ctx = reflect.ValueOf(callContext)
				ret []reflect.Value
			)
			if reqIn != nil {
				ret = method.Func.Call([]reflect.Value{svc, ctx, reflect.ValueOf(in)})
			} else {
				ret = method.Func.Call([]reflect.Value{svc, ctx})
			}

			if method.Type.NumOut() == 1 {
				// Ensure we always call the post response fn if set
				if postResponseFn != nil {
					defer postResponseFn(callContext, method.Name, nil)
				}

				// Deal with error case
				if !ret[0].IsNil() {
					writeError(w, ret[0].Interface())
					return
				}
				json.NewEncoder(w).Encode(&respSuccess{})
				return
			}

			// Ensure we always call the post response fn if set
			if postResponseFn != nil {
				defer func() {
					var response apachethrift.TStruct
					if result, ok := ret[0].Interface().(apachethrift.TStruct); ok {
						response = result
					}
					postResponseFn(callContext, method.Name, response)
				}()
			}

			// Deal with error case
			if !ret[1].IsNil() {
				writeError(w, ret[1].Interface())
				return
			}

			buff := bytes.NewBuffer(nil)
			if err := json.NewEncoder(buff).Encode(ret[0].Interface()); err != nil {
				writeError(w, fmt.Errorf("failed to encode response body: %v", err))
				return
			}

			w.WriteHeader(http.StatusOK)
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

	switch v := errValue.(type) {
	case Error:
		w.WriteHeader(v.StatusCode())
	case error:
		if xerrors.IsInvalidParams(v) {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Write(buff.Bytes())
}
