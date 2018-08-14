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
	"fmt"
	"time"

	"github.com/m3db/m3/src/m3nsch"
	proto "github.com/m3db/m3/src/m3nsch/rpc"

	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
)

func toTimeFromProtoTimestamp(t *google_protobuf.Timestamp) time.Time {
	return time.Unix(t.Seconds, int64(t.Nanos))
}

// ToM3nschWorkload converts a rpc Workload into an equivalent API Workload.
func ToM3nschWorkload(workload *proto.Workload) (m3nsch.Workload, error) {
	if workload == nil {
		return m3nsch.Workload{}, fmt.Errorf("invalid workload")
	}

	return m3nsch.Workload{
		BaseTime:     toTimeFromProtoTimestamp(workload.BaseTime),
		MetricPrefix: workload.MetricPrefix,
		Namespace:    workload.Namespace,
		Cardinality:  int(workload.Cardinality),
		IngressQPS:   int(workload.IngressQPS),
	}, nil
}

// ToM3nschStatus converts a rpc Status into an equivalent API Status.
func ToM3nschStatus(status proto.Status) (m3nsch.Status, error) {
	switch status {
	case proto.Status_UNINITIALIZED:
		return m3nsch.StatusUninitialized, nil
	case proto.Status_INITIALIZED:
		return m3nsch.StatusInitialized, nil
	case proto.Status_RUNNING:
		return m3nsch.StatusRunning, nil
	}
	return m3nsch.StatusUninitialized, fmt.Errorf("invalid status: %s", status.String())
}
