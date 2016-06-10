package memtsdb

// PoolAllocator allocates an object for a pool.
type PoolAllocator func() interface{}

// EncoderAllocate allocates an encoder for a pool.
type EncoderAllocate func() Encoder

// IteratorAllocate allocates an iterator for a pool.
type IteratorAllocate func() Iterator

// ObjectPool provides a pool for objects
type ObjectPool interface {
	// Init initializes the pool.
	Init(alloc PoolAllocator)

	// Get provides an object from the pool
	Get() interface{}

	// Put returns an object to the pool
	Put(obj interface{})
}

// BytesPool provides a pool for variable size buffers
type BytesPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a buffer from the pool
	Get(capacity int) []byte

	// Put returns a buffer to the pool
	Put(buffer []byte)
}

// ContextPool provides a pool for contexts
type ContextPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a context from the pool
	Get() Context

	// Put returns a context to the pool
	Put(ctx Context)
}

// EncoderPool provides a pool for encoders
type EncoderPool interface {
	// Init initializes the pool.
	Init(alloc EncoderAllocate)

	// Get provides an encoder from the pool
	Get() Encoder

	// Put returns an encoder to the pool
	Put(encoder Encoder)
}

// SegmentReaderPool provides a pool for segment readers
type SegmentReaderPool interface {
	// Init initializes the pool.
	Init()

	// Get provides a segment reader from the pool
	Get() SegmentReader

	// Put returns a segment reader to the pool
	Put(reader SegmentReader)
}

// IteratorPool provides a pool for iterators
type IteratorPool interface {
	// Init initializes the pool.
	Init(alloc IteratorAllocate)

	// Get provides an iterator from the pool
	Get() Iterator

	// Put returns an iterator to the pool
	Put(iter Iterator)
}

// PoolBucket specifies a pool bucket
type PoolBucket struct {
	// Capacity is the size of each element in the bucket
	Capacity int

	// Count is the number of fixed elements in the bucket
	Count int
}

// PoolBucketByCapacity is a sortable collection of pool buckets
type PoolBucketByCapacity []PoolBucket

func (x PoolBucketByCapacity) Len() int {
	return len(x)
}

func (x PoolBucketByCapacity) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x PoolBucketByCapacity) Less(i, j int) bool {
	return x[i].Capacity < x[j].Capacity
}
