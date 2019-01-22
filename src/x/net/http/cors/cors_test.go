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

// COPIED FROM https://github.com/etcd-io/etcd/tree/v3.2.10/pkg/cors under
// http://www.apache.org/licenses/LICENSE-2.0#redistribution .
// Original copyright follows:

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cors

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestCORSInfo(t *testing.T) {
	tests := []struct {
		s     string
		winfo CORSInfo
		ws    string
	}{
		{"", CORSInfo{}, ""},
		{"http://127.0.0.1", CORSInfo{"http://127.0.0.1": true}, "http://127.0.0.1"},
		{"*", CORSInfo{"*": true}, "*"},
		// with space around
		{" http://127.0.0.1 ", CORSInfo{"http://127.0.0.1": true}, "http://127.0.0.1"},
		// multiple addrs
		{
			"http://127.0.0.1,http://127.0.0.2",
			CORSInfo{"http://127.0.0.1": true, "http://127.0.0.2": true},
			"http://127.0.0.1,http://127.0.0.2",
		},
	}
	for i, tt := range tests {
		info := CORSInfo{}
		if err := info.Set(tt.s); err != nil {
			t.Errorf("#%d: set error = %v, want nil", i, err)
		}
		if !reflect.DeepEqual(info, tt.winfo) {
			t.Errorf("#%d: info = %v, want %v", i, info, tt.winfo)
		}
		if g := info.String(); g != tt.ws {
			t.Errorf("#%d: info string = %s, want %s", i, g, tt.ws)
		}
	}
}

func TestCORSInfoOriginAllowed(t *testing.T) {
	tests := []struct {
		set      string
		origin   string
		wallowed bool
	}{
		{"http://127.0.0.1,http://127.0.0.2", "http://127.0.0.1", true},
		{"http://127.0.0.1,http://127.0.0.2", "http://127.0.0.2", true},
		{"http://127.0.0.1,http://127.0.0.2", "*", false},
		{"http://127.0.0.1,http://127.0.0.2", "http://127.0.0.3", false},
		{"*", "*", true},
		{"*", "http://127.0.0.1", true},
	}
	for i, tt := range tests {
		info := CORSInfo{}
		if err := info.Set(tt.set); err != nil {
			t.Errorf("#%d: set error = %v, want nil", i, err)
		}
		if g := info.OriginAllowed(tt.origin); g != tt.wallowed {
			t.Errorf("#%d: allowed = %v, want %v", i, g, tt.wallowed)
		}
	}
}

func TestCORSHandler(t *testing.T) {
	info := &CORSInfo{}
	if err := info.Set("http://127.0.0.1,http://127.0.0.2"); err != nil {
		t.Fatalf("unexpected set error: %v", err)
	}
	h := &CORSHandler{
		Handler: http.NotFoundHandler(),
		Info:    info,
	}

	header := func(origin string) http.Header {
		return http.Header{
			"Access-Control-Allow-Methods": []string{"POST, GET, OPTIONS, PUT, DELETE"},
			"Access-Control-Allow-Origin":  []string{origin},
			"Access-Control-Allow-Headers": []string{"accept, content-type, authorization"},
		}
	}
	tests := []struct {
		method  string
		origin  string
		wcode   int
		wheader http.Header
	}{
		{"GET", "http://127.0.0.1", http.StatusNotFound, header("http://127.0.0.1")},
		{"GET", "http://127.0.0.2", http.StatusNotFound, header("http://127.0.0.2")},
		{"GET", "http://127.0.0.3", http.StatusNotFound, http.Header{}},
		{"OPTIONS", "http://127.0.0.1", http.StatusOK, header("http://127.0.0.1")},
	}
	for i, tt := range tests {
		rr := httptest.NewRecorder()
		req := &http.Request{
			Method: tt.method,
			Header: http.Header{"Origin": []string{tt.origin}},
		}
		h.ServeHTTP(rr, req)
		if rr.Code != tt.wcode {
			t.Errorf("#%d: code = %v, want %v", i, rr.Code, tt.wcode)
		}
		// it is set by http package, and there is no need to test it
		rr.HeaderMap.Del("Content-Type")
		rr.HeaderMap.Del("X-Content-Type-Options")
		if !reflect.DeepEqual(rr.HeaderMap, tt.wheader) {
			t.Errorf("#%d: header = %+v, want %+v", i, rr.HeaderMap, tt.wheader)
		}
	}
}
