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

package tchannelthrift

import (
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/storage/limits/permits"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/serialize"
)

// Key is a specific string type for context setting.
type Key string

// EndpointContextKey is the key for setting and retrieving the endpoint from context.
const EndpointContextKey Key = "endpoint"

// Endpoint is a type representing an API endpoint
type Endpoint int

const (
	AggregateRaw Endpoint = iota
	Fetch
	FetchBatchRaw
	FetchBatchRawV2
	FetchTagged
	Query
)

// String returns the string value of Endpoint enum.
func (e Endpoint) String() string {
	switch e {
	case AggregateRaw:
		return "AggregateRaw"
	case Fetch:
		return "Fetch"
	case FetchBatchRaw:
		return "FetchBatchRaw"
	case FetchBatchRawV2:
		return "FetchBatchRawV2"
	case FetchTagged:
		return "FetchTagged"
	case Query:
		return "Query"
	default:
		return "Unknown"
	}
}

// Options controls server behavior
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options

	// SetTopologyInitializer sets the TopologyInitializer.
	SetTopologyInitializer(value topology.Initializer) Options

	// TopologyInitializer returns the TopologyInitializer.
	TopologyInitializer() topology.Initializer

	// SetIdentifierPool sets the identifier pool.
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the identifier pool.
	IdentifierPool() ident.Pool

	// SetBlockMetadataV2Pool sets the block metadata pool.
	SetBlockMetadataV2Pool(value BlockMetadataV2Pool) Options

	// BlockMetadataV2Pool returns the block metadata pool.
	BlockMetadataV2Pool() BlockMetadataV2Pool

	// SetBlockMetadataV2SlicePool sets the block metadata slice pool.
	SetBlockMetadataV2SlicePool(value BlockMetadataV2SlicePool) Options

	// BlockMetadataV2SlicePool returns the block metadata slice pool.
	BlockMetadataV2SlicePool() BlockMetadataV2SlicePool

	// SetTagEncoderPool sets the tag encoder pool.
	SetTagEncoderPool(value serialize.TagEncoderPool) Options

	// TagEncoderPool returns the tag encoder pool.
	TagEncoderPool() serialize.TagEncoderPool

	// SetCheckedBytesWrapperPool sets the checked bytes wrapper pool.
	SetCheckedBytesWrapperPool(value xpool.CheckedBytesWrapperPool) Options

	// CheckedBytesWrapperPool returns the checked bytes wrapper pool.
	CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool

	// SetMaxOutstandingWriteRequests sets the maximum number of allowed
	// outstanding write requests.
	SetMaxOutstandingWriteRequests(value int) Options

	// MaxOutstandingWriteRequests returns the maxinum number of allowed
	// outstanding write requests.
	MaxOutstandingWriteRequests() int

	// SetMaxOutstandingReadRequests sets the maximum number of allowed
	// outstanding read requests.
	SetMaxOutstandingReadRequests(value int) Options

	// MaxOutstandingReadRequests returns the maxinum number of allowed
	// outstanding read requests.
	MaxOutstandingReadRequests() int

	// QueryLimits returns the QueryLimits.
	QueryLimits() limits.QueryLimits

	// SetQueryLimits sets the QueryLimits.
	SetQueryLimits(value limits.QueryLimits) Options

	// PermitsOptions returns the permits options.
	PermitsOptions() permits.Options

	// SetPermitsOptions sets the permits options.
	SetPermitsOptions(value permits.Options) Options

	// SetFetchTaggedSeriesBlocksPerBatch sets the series blocks allowed to be read
	// per permit acquired.
	SetFetchTaggedSeriesBlocksPerBatch(value int) Options
}
