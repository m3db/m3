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

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=id github.com/m3db/m3/src/metrics/metric/id ID,SortedTagIterator | genclean -pkg github.com/m3db/m3/src/metrics/metric/id -out $GOPATH/src/github.com/m3db/m3/src/metrics/metric/id/id_mock.go"
//go:generate sh -c "mockgen -package=matcher github.com/m3db/m3/src/metrics/matcher Matcher | genclean -pkg github.com/m3db/m3/src/metrics/matcher -out $GOPATH/src/github.com/m3db/m3/src/metrics/matcher/matcher_mock.go"
//go:generate sh -c "mockgen -package=protobuf github.com/m3db/m3/src/metrics/encoding/protobuf UnaggregatedEncoder | genclean -pkg github.com/m3db/m3/src/metrics/encoding/protobuf -out $GOPATH/src/github.com/m3db/m3/src/metrics/encoding/protobuf/protobuf_mock.go"
//go:generate sh -c "mockgen -package=rules github.com/m3db/m3/src/metrics/rules Store | genclean -pkg github.com/m3db/m3/src/metrics/rules -out $GOPATH/src/github.com/m3db/m3/src/metrics/rules/rules_mock.go"

package mocks
