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

package convert

import (
	"github.com/m3db/m3db/src/m3nsch"
	proto "github.com/m3db/m3db/src/m3nsch/rpc"

	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
)

// ToProtoStatus converts an API status into a RPC status.
func ToProtoStatus(status m3nsch.Status) proto.Status {
	switch status {
	case m3nsch.StatusUninitialized:
		return proto.Status_UNINITIALIZED
	case m3nsch.StatusInitialized:
		return proto.Status_INITIALIZED
	case m3nsch.StatusRunning:
		return proto.Status_RUNNING
	}
	return proto.Status_UNKNOWN
}

// ToProtoWorkload converts an API Workload into a RPC Workload.
func ToProtoWorkload(mw m3nsch.Workload) proto.Workload {
	var w proto.Workload
	w.BaseTime = &google_protobuf.Timestamp{
		Seconds: mw.BaseTime.Unix(),
		Nanos:   int32(mw.BaseTime.UnixNano()),
	}
	w.Cardinality = int32(mw.Cardinality)
	w.IngressQPS = int32(mw.IngressQPS)
	w.MetricPrefix = mw.MetricPrefix
	w.Namespace = mw.Namespace
	return w
}
