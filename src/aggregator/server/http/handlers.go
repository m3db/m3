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

package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/errors"
)

var (
	errRequestMustBePost  = xerrors.NewInvalidParamsError(errors.New("request must be POST"))
	errInvalidRequestBody = xerrors.NewInvalidParamsError(errors.New("invalid request body"))
)

type respSuccess struct{}
type respError struct {
	Error string `json:"error"`
}

// metricUnionWithPolicies contains a metric union along with applicable policies
type metricUnionWithPolicies struct {
	Metric   unaggregated.MetricUnion `json:"metric"`
	Policies policy.VersionedPolicies `json:"policies"`
}

func registerHandlers(mux *http.ServeMux, aggregator aggregator.Aggregator) error {
	v := reflect.ValueOf(aggregator)
	t := v.Type()
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)

		// Ensure the method is of the following signature:
		// - methodName(param1, param2) error
		if !(method.Type.NumIn() == 3 && method.Type.NumOut() == 1) {
			continue
		}

		obj := method.Type.In(0)
		if obj != t {
			continue
		}
		metric := method.Type.In(1)
		if metric != reflect.TypeOf(unaggregated.MetricUnion{}) {
			continue
		}
		policies := method.Type.In(2)
		if policies != reflect.TypeOf(policy.VersionedPolicies{}) {
			continue
		}
		resultErr := method.Type.Out(0)
		errInterfaceType := reflect.TypeOf((*error)(nil)).Elem()
		if resultErr.Kind() != reflect.Interface || !resultErr.Implements(errInterfaceType) {
			continue
		}

		name := strings.ToLower(method.Name)
		mux.HandleFunc(fmt.Sprintf("/%s", name), func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodPost {
				writeError(w, errRequestMustBePost)
				return
			}

			var payload metricUnionWithPolicies
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				writeError(w, errInvalidRequestBody)
				return
			}

			var (
				agg = reflect.ValueOf(aggregator)
				mu  = reflect.ValueOf(payload.Metric)
				vp  = reflect.ValueOf(payload.Policies)
			)
			ret := method.Func.Call([]reflect.Value{agg, mu, vp})
			if !ret[0].IsNil() {
				writeError(w, ret[0].Interface().(error))
				return
			}
			json.NewEncoder(w).Encode(&respSuccess{})
			return
		})
	}
	return nil
}

func writeError(w http.ResponseWriter, err error) {
	var result respError
	if err != nil {
		result.Error = err.Error()
	}
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(&result); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		result.Error = err.Error()
		json.NewEncoder(w).Encode(&result)
		return
	}
	if xerrors.IsInvalidParams(err) {
		w.WriteHeader(http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(buf.Bytes())
}
