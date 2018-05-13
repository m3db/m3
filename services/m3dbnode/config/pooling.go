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

package config

// PoolingType is a type of pooling, using runtime or mmap'd bytes pooling.
type PoolingType string

const (
	// SimplePooling uses the basic Go runtime to allocate bytes for bytes pools.
	SimplePooling PoolingType = "simple"
	// NativePooling uses a mmap syscall to allocate bytes for bytes pools, take
	// great care when experimenting with this. There's not enough protection
	// even with ref counting that M3DB performs to use this safely in
	// production. Here be dragons and so forth.
	NativePooling PoolingType = "native"
)

const (
	defaultMaxFinalizerCapacity = 4
)

// PoolingPolicy specifies the pooling policy.
type PoolingPolicy struct {
	// The initial alloc size for a block
	BlockAllocSize int `yaml:"blockAllocSize"`

	// The general pool type: simple or native.
	Type PoolingType `yaml:"type"`

	// The Bytes pool buckets to use
	BytesPool BucketPoolPolicy `yaml:"bytesPool"`

	// The policy for the Closers pool
	ClosersPool PoolPolicy `yaml:"closersPool"`

	// The policy for the Context pool
	ContextPool ContextPoolPolicy `yaml:"contextPool"`

	// The policy for the DatabaseSeries pool
	SeriesPool PoolPolicy `yaml:"seriesPool"`

	// The policy for the DatabaseBlock pool
	BlockPool PoolPolicy `yaml:"blockPool"`

	// The policy for the Encoder pool
	EncoderPool PoolPolicy `yaml:"encoderPool"`

	// The policy for the Iterator pool
	IteratorPool PoolPolicy `yaml:"iteratorPool"`

	// The policy for the Segment Reader pool
	SegmentReaderPool PoolPolicy `yaml:"segmentReaderPool"`

	// The policy for the Identifier pool
	IdentifierPool PoolPolicy `yaml:"identifierPool"`

	// The policy for the FetchBlockMetadataResult pool
	FetchBlockMetadataResultsPool CapacityPoolPolicy `yaml:"fetchBlockMetadataResultsPool"`

	// The policy for the FetchBlocksMetadataResults pool
	FetchBlocksMetadataResultsPool CapacityPoolPolicy `yaml:"fetchBlocksMetadataResultsPool"`

	// The policy for the HostBlockMetadataSlice pool
	HostBlockMetadataSlicePool CapacityPoolPolicy `yaml:"hostBlockMetadataSlicePool"`

	// The policy for the BlockMetadat pool
	BlockMetadataPool PoolPolicy `yaml:"blockMetadataPool"`

	// The policy for the BlockMetadataSlice pool
	BlockMetadataSlicePool CapacityPoolPolicy `yaml:"blockMetadataSlicePool"`

	// The policy for the BlocksMetadata pool
	BlocksMetadataPool PoolPolicy `yaml:"blocksMetadataPool"`

	// The policy for the BlocksMetadataSlice pool
	BlocksMetadataSlicePool CapacityPoolPolicy `yaml:"blocksMetadataSlicePool"`

	// The policy for the tags pool
	TagsPool MaxCapacityPoolPolicy `yaml:"tagsPool"`

	// The policy for the tags iterator pool
	TagsIteratorPool PoolPolicy `yaml:"tagIteratorPool"`

	// The policy for the index.ResultsPool
	IndexResultsPool PoolPolicy `yaml:"indexResultsPool"`

	// The policy for the TagEncoderPool
	TagEncoderPool PoolPolicy `yaml:"tagEncoderPool"`

	// The policy for the TagDecoderPool
	TagDecoderPool PoolPolicy `yaml:"tagDecoderPool"`
}

// PoolPolicy specifies a single pool policy.
type PoolPolicy struct {
	// The size of the pool
	Size int `yaml:"size"`

	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}

// CapacityPoolPolicy specifies a single pool policy that has a
// per element capacity.
type CapacityPoolPolicy struct {
	// The size of the pool
	Size int `yaml:"size"`

	// The capacity of items in the pool
	Capacity int `yaml:"capacity"`

	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}

// MaxCapacityPoolPolicy specifies a single pool policy that has a
// per element capacity, and a maximum allowed capacity as well.
type MaxCapacityPoolPolicy struct {
	// The size of the pool
	Size int `yaml:"size"`

	// The capacity of items in the pool
	Capacity int `yaml:"capacity"`

	// The max capacity of items in the pool
	MaxCapacity int `yaml:"maxCapacity"`

	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`
}

// BucketPoolPolicy specifies a bucket pool policy.
type BucketPoolPolicy struct {
	// The pool buckets sizes to use
	Buckets []CapacityPoolPolicy `yaml:"buckets"`
}

// ContextPoolPolicy specifies the policy for the context pool
type ContextPoolPolicy struct {
	// The size of the pool
	Size int `yaml:"size"`

	// The low watermark to start refilling the pool, if zero none
	RefillLowWaterMark float64 `yaml:"lowWatermark" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none
	RefillHighWaterMark float64 `yaml:"highWatermark" validate:"min=0.0,max=1.0"`

	// The maximum allowable size for a slice of finalizers that the
	// pool will allow to be returned (finalizer slices that grow too
	// large during use will be discarded instead of returning to the
	// pool where they would consume more memory.)
	MaxFinalizerCapacity int `yaml:"maxFinalizerCapacity" validate:"min=0"`
}

// PoolPolicy returns the PoolPolicy that is represented by the ContextPoolPolicy
func (c ContextPoolPolicy) PoolPolicy() PoolPolicy {
	return PoolPolicy{
		Size:                c.Size,
		RefillLowWaterMark:  c.RefillLowWaterMark,
		RefillHighWaterMark: c.RefillHighWaterMark,
	}
}

// MaxFinalizerCapacityWithDefault returns the maximum finalizer capacity and
// fallsback to the default value if its not set
func (c ContextPoolPolicy) MaxFinalizerCapacityWithDefault() int {
	if c.MaxFinalizerCapacity == 0 {
		return defaultMaxFinalizerCapacity
	}

	return c.MaxFinalizerCapacity
}
