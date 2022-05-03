package utils

import (
	"sort"
	"sync"
)

var (
	sliceCapacitiesForPools []int
	bytesPools              []*sync.Pool
)

// SetMaxBytesPoolAlloc sets the capacities of byte slices that are pooled for binary thrift
// fields and must be called before any thrift binary protocols are used
// since it is a global and is not thread safe to edit.
func SetMaxBytesPoolAlloc(capacities ...int) {
	initPools(capacities)
}

func init() {
	initPools([]int{1024})
}

func initPools(capacities []int) {
	// Make a defensive copy.
	sliceCapacitiesForPools = append([]int{}, capacities...)

	sort.Ints(sliceCapacitiesForPools)

	bytesPools = make([]*sync.Pool, 0)
	for _, capacity := range sliceCapacitiesForPools {
		bytesPools = append(bytesPools, newPool(capacity))
	}
}

func newPool(capacity int) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			element := bytesWrapperPool.Get().(*bytesWrapper)
			element.value = make([]byte, capacity)
			return element
		},
	}
}

// BytesPoolPut is a public func to call to return pooled bytes to, each
// the capacity of BytesPoolAlloc.  TBinaryProtocol.ReadBinary uses this pool
// to allocate from if the size of the bytes required to return is is equal or
// less than BytesPoolAlloc.
func BytesPoolPut(b []byte) bool {
	for i, capacity := range sliceCapacitiesForPools {
		if capacity == cap(b) {
			element := bytesWrapperPool.Get().(*bytesWrapper)
			element.value = b
			bytesPools[i].Put(element)
			return true
		}
	}
	return false
}

// BytesPoolGet returns a pooled byte slice of capacity BytesPoolAlloc.
func BytesPoolGet(size int) []byte {
	for i, capacity := range sliceCapacitiesForPools {
		if size <= capacity {
			element := bytesPools[i].Get().(*bytesWrapper)
			result := element.value
			element.value = nil
			bytesWrapperPool.Put(element)
			return result[:size]
		}
	}

	return make([]byte, size)
}

// bytesWrapper is used to wrap a byte slice to avoid allocing a interface{}
// when wrapping a byte slice which is usually passed on the stack
type bytesWrapper struct {
	value []byte
}

var bytesWrapperPool = sync.Pool{
	New: func() interface{} {
		return &bytesWrapper{}
	},
}
