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

package client

import (
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
)

type payloadType int

const (
	// nolint
	unknownPayloadType payloadType = iota
	untimedType
	forwardedType
	timedType
	timedWithStagedMetadatasType
	passthroughType
)

type untimedPayload struct {
	metric    unaggregated.MetricUnion
	metadatas metadata.StagedMetadatas
}

type forwardedPayload struct {
	metric   aggregated.ForwardedMetric
	metadata metadata.ForwardMetadata
}

type timedPayload struct {
	metric   aggregated.Metric
	metadata metadata.TimedMetadata
}

type timedWithStagedMetadatas struct {
	metadatas metadata.StagedMetadatas
	metric    aggregated.Metric
}

type passthroughPayload struct {
	metric        aggregated.Metric
	storagePolicy policy.StoragePolicy
}

type payloadUnion struct {
	forwarded                forwardedPayload
	untimed                  untimedPayload
	timed                    timedPayload
	timedWithStagedMetadatas timedWithStagedMetadatas
	passthrough              passthroughPayload
	payloadType              payloadType
}
