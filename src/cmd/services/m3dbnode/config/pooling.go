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

import (
	"fmt"

	"github.com/m3db/m3/src/x/pool"
)

// PoolingType is a type of pooling, using runtime or mmap'd bytes pooling.
type PoolingType string

const (
	// SimplePooling uses the basic Go runtime to allocate bytes for bytes pools.
	SimplePooling PoolingType = "simple"

	defaultPoolingType = SimplePooling
)

const (
	defaultBlockAllocSize = 16
)

var (
	defaultThriftBytesPoolAllocSizes = []int{16, 64, 256, 512, 1024, 2048}
)

type poolPolicyDefault struct {
	size                pool.Size
	refillLowWaterMark  float64
	refillHighWaterMark float64

	// Only used for capacity and max capacity pools.
	capacity int
	// Only used for max capacity pools.
	maxCapacity int
}

type bucketPoolPolicyDefault struct {
	buckets []CapacityPoolPolicy
}

var (
	defaultRefillLowWaterMark  = 0.3
	defaultRefillHighWaterMark = 0.6

	defaultPoolPolicy = poolPolicyDefault{
		size:                4096,
		refillLowWaterMark:  0,
		refillHighWaterMark: 0,
	}

	defaultPoolPolicies = map[string]poolPolicyDefault{
		"checkedBytesWrapper": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  0,
			refillHighWaterMark: 0,
		},
		"tagsIterator": defaultPoolPolicy,
		"indexResults": poolPolicyDefault{
			// NB(r): There only needs to be one index results map per
			// concurrent query, so as long as there is no more than
			// the number of concurrent index queries than the size
			// specified here the maps should be recycled.
			size:                256,
			refillLowWaterMark:  0,
			refillHighWaterMark: 0,
		},
		"tagEncoder": poolPolicyDefault{
			// NB(r): Tag encoder is used for every individual time series
			// returned from an index query since it needs to encode the tags
			// back for RPC and has a bytes buffer internally for each time
			// series being returned.
			size:                8192,
			refillLowWaterMark:  0,
			refillHighWaterMark: 0,
		},
		"tagDecoder": defaultPoolPolicy,
		"context": poolPolicyDefault{
			size:                32768,
			refillLowWaterMark:  0,
			refillHighWaterMark: 0,
		},
		"series": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"block": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"encoder": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"closers": poolPolicyDefault{
			// NB(r): Note this has to be bigger than context pool by
			// big fraction (by factor of say 4) since each context
			// usually uses a fair few closers.
			size:                262144,
			refillLowWaterMark:  0,
			refillHighWaterMark: 0,
		},
		"segmentReader": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"iterator": poolPolicyDefault{
			size:                2048,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"blockMetadata": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"blocksMetadata": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"identifier": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"postingsList": poolPolicyDefault{
			// defaultPostingsListPoolSize has a small default pool size since postings
			// lists can frequently reach the size of 4mb each in practice even when
			// reset.
			size:                16,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"bufferBucket": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"bufferBucketVersions": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
		},
		"retrieveRequestPool": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  0,
			refillHighWaterMark: 0,
		},

		// Capacity pools.
		"fetchBlockMetadataResults": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
			capacity:            32,
		},
		"fetchBlocksMetadataResults": poolPolicyDefault{
			size:                32,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
			capacity:            4096,
		},
		"replicaMetadataSlice": poolPolicyDefault{
			size:                131072,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
			capacity:            3,
		},
		"blockMetadataSlice": poolPolicyDefault{
			size:                65536,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
			capacity:            32,
		},
		"blocksMetadataSlice": poolPolicyDefault{
			size:                32,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
			capacity:            4096,
		},

		// Max capacity pools.
		"tags": poolPolicyDefault{
			size:                4096,
			refillLowWaterMark:  defaultRefillLowWaterMark,
			refillHighWaterMark: defaultRefillHighWaterMark,
			capacity:            0,
			maxCapacity:         16,
		},
	}

	defaultBucketPoolPolicies = map[string]bucketPoolPolicyDefault{
		"bytes": bucketPoolPolicyDefault{
			buckets: []CapacityPoolPolicy{
				{
					Capacity: intPtr(16),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(524288),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
				{
					Capacity: intPtr(32),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(262144),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
				{
					Capacity: intPtr(64),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(131072),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
				{
					Capacity: intPtr(128),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(65536),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
				{
					Capacity: intPtr(256),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(65536),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
				{
					Capacity: intPtr(1440),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(16384),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
				{
					Capacity: intPtr(4096),
					PoolPolicy: PoolPolicy{
						Size:                poolSizePtr(8192),
						RefillLowWaterMark:  &defaultRefillLowWaterMark,
						RefillHighWaterMark: &defaultRefillHighWaterMark,
					},
				},
			},
		},
	}
)

// PoolingPolicy specifies the pooling policy. To add a new pool, follow these steps:
//
//    1. Add the pool to the struct below.
//    2. Add the default values to the defaultPoolPolicies or defaultBucketPoolPolicies map.
//    3. Add a call to initDefaultsAndValidate() for the new pool in the
//       PoolingPolicy.InitDefaultsAndValidate() method.
type PoolingPolicy struct {
	// The initial alloc size for a block.
	BlockAllocSize *int `yaml:"blockAllocSize"`

	// The thrift bytes pool max bytes slice allocation for a single binary field.
	ThriftBytesPoolAllocSize *int `yaml:"thriftBytesPoolAllocSize"`

	// The general pool type (currently only supported: simple).
	Type *PoolingType `yaml:"type"`

	// The Bytes pool buckets to use.
	BytesPool BucketPoolPolicy `yaml:"bytesPool"`

	// The policy for the checked bytes wrapper pool.
	CheckedBytesWrapperPool PoolPolicy `yaml:"checkedBytesWrapperPool"`

	// The policy for the Closers pool.
	ClosersPool PoolPolicy `yaml:"closersPool"`

	// The policy for the Context pool.
	ContextPool PoolPolicy `yaml:"contextPool"`

	// The policy for the DatabaseSeries pool.
	SeriesPool PoolPolicy `yaml:"seriesPool"`

	// The policy for the DatabaseBlock pool.
	BlockPool PoolPolicy `yaml:"blockPool"`

	// The policy for the Encoder pool.
	EncoderPool PoolPolicy `yaml:"encoderPool"`

	// The policy for the Iterator pool.
	IteratorPool PoolPolicy `yaml:"iteratorPool"`

	// The policy for the Segment Reader pool.
	SegmentReaderPool PoolPolicy `yaml:"segmentReaderPool"`

	// The policy for the Identifier pool.
	IdentifierPool PoolPolicy `yaml:"identifierPool"`

	// The policy for the FetchBlockMetadataResult pool.
	FetchBlockMetadataResultsPool CapacityPoolPolicy `yaml:"fetchBlockMetadataResultsPool"`

	// The policy for the FetchBlocksMetadataResults pool.
	FetchBlocksMetadataResultsPool CapacityPoolPolicy `yaml:"fetchBlocksMetadataResultsPool"`

	// The policy for the ReplicaMetadataSlicePool pool.
	ReplicaMetadataSlicePool CapacityPoolPolicy `yaml:"replicaMetadataSlicePool"`

	// The policy for the BlockMetadat pool.
	BlockMetadataPool PoolPolicy `yaml:"blockMetadataPool"`

	// The policy for the BlockMetadataSlice pool.
	BlockMetadataSlicePool CapacityPoolPolicy `yaml:"blockMetadataSlicePool"`

	// The policy for the BlocksMetadata pool.
	BlocksMetadataPool PoolPolicy `yaml:"blocksMetadataPool"`

	// The policy for the BlocksMetadataSlice pool.
	BlocksMetadataSlicePool CapacityPoolPolicy `yaml:"blocksMetadataSlicePool"`

	// The policy for the tags pool.
	TagsPool MaxCapacityPoolPolicy `yaml:"tagsPool"`

	// The policy for the tags iterator pool.
	TagsIteratorPool PoolPolicy `yaml:"tagIteratorPool"`

	// The policy for the index.ResultsPool.
	IndexResultsPool PoolPolicy `yaml:"indexResultsPool"`

	// The policy for the TagEncoderPool.
	TagEncoderPool PoolPolicy `yaml:"tagEncoderPool"`

	// The policy for the TagDecoderPool.
	TagDecoderPool PoolPolicy `yaml:"tagDecoderPool"`

	// The policy for the WriteBatchPool.
	WriteBatchPool WriteBatchPoolPolicy `yaml:"writeBatchPool"`

	// The policy for the BufferBucket pool.
	BufferBucketPool PoolPolicy `yaml:"bufferBucketPool"`

	// The policy for the BufferBucketVersions pool.
	BufferBucketVersionsPool PoolPolicy `yaml:"bufferBucketVersionsPool"`

	// The policy for the RetrieveRequestPool pool.
	RetrieveRequestPool PoolPolicy `yaml:"retrieveRequestPool"`

	// The policy for the PostingsListPool.
	PostingsListPool PoolPolicy `yaml:"postingsListPool"`
}

// InitDefaultsAndValidate initializes all default values and validates the configuration
func (p *PoolingPolicy) InitDefaultsAndValidate() error {
	if err := p.CheckedBytesWrapperPool.initDefaultsAndValidate("checkedBytesWrapper"); err != nil {
		return err
	}
	if err := p.ClosersPool.initDefaultsAndValidate("closers"); err != nil {
		return err
	}
	if err := p.ContextPool.initDefaultsAndValidate("context"); err != nil {
		return err
	}
	if err := p.SeriesPool.initDefaultsAndValidate("series"); err != nil {
		return err
	}
	if err := p.BlockPool.initDefaultsAndValidate("block"); err != nil {
		return err
	}
	if err := p.EncoderPool.initDefaultsAndValidate("encoder"); err != nil {
		return err
	}
	if err := p.IteratorPool.initDefaultsAndValidate("iterator"); err != nil {
		return err
	}
	if err := p.SegmentReaderPool.initDefaultsAndValidate("segmentReader"); err != nil {
		return err
	}
	if err := p.IdentifierPool.initDefaultsAndValidate("identifier"); err != nil {
		return err
	}
	if err := p.FetchBlockMetadataResultsPool.initDefaultsAndValidate("fetchBlockMetadataResults"); err != nil {
		return err
	}
	if err := p.FetchBlocksMetadataResultsPool.initDefaultsAndValidate("fetchBlocksMetadataResults"); err != nil {
		return err
	}
	if err := p.ReplicaMetadataSlicePool.initDefaultsAndValidate("replicaMetadataSlice"); err != nil {
		return err
	}
	if err := p.BlockMetadataPool.initDefaultsAndValidate("blockMetadata"); err != nil {
		return err
	}
	if err := p.BlockMetadataSlicePool.initDefaultsAndValidate("blockMetadataSlice"); err != nil {
		return err
	}
	if err := p.BlocksMetadataPool.initDefaultsAndValidate("blocksMetadata"); err != nil {
		return err
	}
	if err := p.BlocksMetadataSlicePool.initDefaultsAndValidate("blocksMetadataSlice"); err != nil {
		return err
	}
	if err := p.TagsPool.initDefaultsAndValidate("tags"); err != nil {
		return err
	}
	if err := p.TagsIteratorPool.initDefaultsAndValidate("tagsIterator"); err != nil {
		return err
	}
	if err := p.IndexResultsPool.initDefaultsAndValidate("indexResults"); err != nil {
		return err
	}
	if err := p.TagEncoderPool.initDefaultsAndValidate("tagEncoder"); err != nil {
		return err
	}
	if err := p.TagDecoderPool.initDefaultsAndValidate("tagDecoder"); err != nil {
		return err
	}
	if err := p.PostingsListPool.initDefaultsAndValidate("postingsList"); err != nil {
		return err
	}
	if err := p.BytesPool.initDefaultsAndValidate("bytes"); err != nil {
		return err
	}
	if err := p.BufferBucketPool.initDefaultsAndValidate("bufferBucket"); err != nil {
		return err
	}
	if err := p.BufferBucketVersionsPool.initDefaultsAndValidate("bufferBucketVersions"); err != nil {
		return err
	}
	if err := p.RetrieveRequestPool.initDefaultsAndValidate("retrieveRequestPool"); err != nil {
		return err
	}
	return nil
}

// BlockAllocSizeOrDefault returns the configured block alloc size if provided,
// or a default value otherwise.
func (p *PoolingPolicy) BlockAllocSizeOrDefault() int {
	if p.BlockAllocSize != nil {
		return *p.BlockAllocSize
	}

	return defaultBlockAllocSize
}

// ThriftBytesPoolAllocSizesOrDefault returns the configured thrift bytes pool
// max alloc size if provided, or a default value otherwise.
func (p *PoolingPolicy) ThriftBytesPoolAllocSizesOrDefault() []int {
	if p.ThriftBytesPoolAllocSize != nil {
		return []int{*p.ThriftBytesPoolAllocSize}
	}

	return defaultThriftBytesPoolAllocSizes
}

// TypeOrDefault returns the configured pooling type if provided, or a default
// value otherwise.
func (p *PoolingPolicy) TypeOrDefault() PoolingType {
	if p.Type != nil {
		return *p.Type
	}

	return defaultPoolingType
}

// PoolPolicy specifies a single pool policy.
type PoolPolicy struct {
	// The size of the pool.
	Size *pool.Size `yaml:"size"`

	// The low watermark to start refilling the pool, if zero none.
	RefillLowWaterMark *float64 `yaml:"lowWatermark"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWaterMark *float64 `yaml:"highWatermark"`
}

func (p *PoolPolicy) initDefaultsAndValidate(poolName string) error {
	defaults, ok := defaultPoolPolicies[poolName]
	if !ok {
		return fmt.Errorf("no default values for pool: %s", poolName)
	}

	if p.Size == nil {
		p.Size = &defaults.size
	}
	if p.RefillLowWaterMark == nil {
		p.RefillLowWaterMark = &defaults.refillLowWaterMark
	}
	if p.RefillHighWaterMark == nil {
		p.RefillHighWaterMark = &defaults.refillHighWaterMark
	}

	if *p.RefillLowWaterMark < 0 || *p.RefillLowWaterMark > 1 {
		return fmt.Errorf(
			"invalid lowWatermark value for %s pool, should be >= 0 and <= 1", poolName)
	}

	if *p.RefillHighWaterMark < 0 || *p.RefillHighWaterMark > 1 {
		return fmt.Errorf(
			"invalid lowWatermark value for %s pool, should be >= 0 and <= 1", poolName)
	}

	return nil
}

// SizeOrDefault returns the configured size if present, or a default value otherwise.
func (p *PoolPolicy) SizeOrDefault() pool.Size {
	return *p.Size
}

// RefillLowWaterMarkOrDefault returns the configured refill low water mark if present,
// or a default value otherwise.
func (p *PoolPolicy) RefillLowWaterMarkOrDefault() float64 {
	return *p.RefillLowWaterMark
}

// RefillHighWaterMarkOrDefault returns the configured refill high water mark if present,
// or a default value otherwise.
func (p *PoolPolicy) RefillHighWaterMarkOrDefault() float64 {
	return *p.RefillHighWaterMark
}

// CapacityPoolPolicy specifies a single pool policy that has a
// per element capacity.
type CapacityPoolPolicy struct {
	PoolPolicy `yaml:",inline"`

	// The capacity of items in the pool.
	Capacity *int `yaml:"capacity"`
}

// Validate validates the capacity pool policy config.
func (p *CapacityPoolPolicy) initDefaultsAndValidate(poolName string) error {
	if err := p.PoolPolicy.initDefaultsAndValidate(poolName); err != nil {
		return err
	}

	defaults, ok := defaultPoolPolicies[poolName]
	if !ok {
		return fmt.Errorf("no default values for pool: %s", poolName)
	}

	if p.Capacity == nil {
		p.Capacity = &defaults.capacity
	}

	if *p.Capacity < 0 {
		return fmt.Errorf("capacity of %s pool must be >= 0", poolName)
	}

	return nil
}

// CapacityOrDefault returns the configured capacity if present, or a default value otherwise.
func (p *CapacityPoolPolicy) CapacityOrDefault() int {
	return *p.Capacity
}

// MaxCapacityPoolPolicy specifies a single pool policy that has a
// per element capacity, and a maximum allowed capacity as well.
type MaxCapacityPoolPolicy struct {
	CapacityPoolPolicy `yaml:",inline"`

	// The max capacity of items in the pool.
	MaxCapacity *int `yaml:"maxCapacity"`
}

func (p *MaxCapacityPoolPolicy) initDefaultsAndValidate(poolName string) error {
	if err := p.CapacityPoolPolicy.initDefaultsAndValidate(poolName); err != nil {
		return err
	}

	defaults, ok := defaultPoolPolicies[poolName]
	if !ok {
		return fmt.Errorf("no default values for pool: %s", poolName)
	}

	if p.MaxCapacity == nil {
		p.MaxCapacity = &defaults.maxCapacity
	}

	if *p.MaxCapacity < 0 {
		return fmt.Errorf("maxCapacity of %s pool must be >= 0", poolName)
	}

	return nil
}

// MaxCapacityOrDefault returns the configured maximum capacity if present, or a
// default value otherwise.
func (p *MaxCapacityPoolPolicy) MaxCapacityOrDefault() int {
	return *p.MaxCapacity
}

// BucketPoolPolicy specifies a bucket pool policy.
type BucketPoolPolicy struct {
	// The pool buckets sizes to use
	Buckets []CapacityPoolPolicy `yaml:"buckets"`
}

func (p *BucketPoolPolicy) initDefaultsAndValidate(poolName string) error {
	defaults, ok := defaultBucketPoolPolicies[poolName]
	if !ok {
		return fmt.Errorf("no default values for pool: %s", poolName)
	}

	if p.Buckets == nil {
		p.Buckets = defaults.buckets
	}

	for i, bucket := range p.Buckets {
		// If the user provided buckets, but no values for refill low/high watermarks
		// then set them to default values.
		if bucket.RefillLowWaterMark == nil {
			p.Buckets[i].RefillLowWaterMark = &defaultRefillLowWaterMark
		}
		if bucket.RefillHighWaterMark == nil {
			p.Buckets[i].RefillHighWaterMark = &defaultRefillHighWaterMark
		}
	}

	for _, bucket := range p.Buckets {
		if bucket.Size == nil {
			return fmt.Errorf("bucket size for pool %s cannot be nil", poolName)
		}
		if bucket.Capacity == nil {
			return fmt.Errorf("bucket capacity for pool %s cannot be nil", poolName)
		}
	}
	return nil
}

// WriteBatchPoolPolicy specifies the pooling policy for the WriteBatch pool.
type WriteBatchPoolPolicy struct {
	// The size of the pool.
	Size *int `yaml:"size"`

	// InitialBatchSize controls the initial batch size for each WriteBatch when
	// the pool is being constructed / refilled.
	InitialBatchSize *int `yaml:"initialBatchSize"`

	// MaxBatchSize controls the maximum size that a pooled WriteBatch can grow to
	// and still remain in the pool.
	MaxBatchSize *int `yaml:"maxBatchSize"`
}

func intPtr(x int) *int {
	return &x
}

func poolSizePtr(x int) *pool.Size {
	sz := pool.Size(x)
	return &sz
}
