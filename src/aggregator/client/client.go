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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
)

var (
	errClientIsInitializedOrClosed   = errors.New("client is already initialized or closed")
	errClientIsUninitializedOrClosed = errors.New("client is uninitialized or closed")
)

// Client is a client capable of writing different types of metrics to the aggregation clients.
type Client interface {
	// Init initializes the client.
	Init() error

	// WriteUntimedCounter writes untimed counter metrics.
	WriteUntimedCounter(
		counter unaggregated.Counter,
		metadatas metadata.StagedMetadatas,
	) error

	// WriteUntimedBatchTimer writes untimed batch timer metrics.
	WriteUntimedBatchTimer(
		batchTimer unaggregated.BatchTimer,
		metadatas metadata.StagedMetadatas,
	) error

	// WriteUntimedGauge writes untimed gauge metrics.
	WriteUntimedGauge(
		gauge unaggregated.Gauge,
		metadatas metadata.StagedMetadatas,
	) error

	// WriteTimed writes timed metrics.
	WriteTimed(
		metric aggregated.Metric,
		metadata metadata.TimedMetadata,
	) error

	// WritePassthrough writes passthrough metrics.
	WritePassthrough(
		metric aggregated.Metric,
		storagePolicy policy.StoragePolicy,
	) error

	// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
	WriteTimedWithStagedMetadatas(
		metric aggregated.Metric,
		metadatas metadata.StagedMetadatas,
	) error

	// Flush flushes any remaining data buffered by the client.
	Flush() error

	// Close closes the client.
	Close() error
}

// AdminClient is an administrative client capable of performing regular client operations
// as well as high-privilege operations such as internal communcations among aggregation
// servers that regular client is not permissioned to do.
type AdminClient interface {
	Client

	// WriteForwarded writes forwarded metrics.
	WriteForwarded(
		metric aggregated.ForwardedMetric,
		metadata metadata.ForwardMetadata,
	) error
}

// NewClient creates a new client.
func NewClient(opts Options) (Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	clientType := opts.AggregatorClientType()
	switch clientType {
	case M3MsgAggregatorClient:
		return NewM3MsgClient(opts)
	case LegacyAggregatorClient:
		fallthrough // LegacyAggregatorClient is an alias
	case TCPAggregatorClient:
		return NewTCPClient(opts)
	}
	return nil, fmt.Errorf("unrecognized client type: %v", clientType)
}
