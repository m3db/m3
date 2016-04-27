package tsz

import (
	"math"
)

const (
	opcodeZeroValueXOR         = 0x0 // xor is zero
	opcodeContainedValueXOR    = 0x1 // meaningful bits in the current xor is contained
	opcodeNotContainedValueXOR = 0x3 // meaningful bits in the current xor is not contained
	eosMarker                  = 0x0 // marks the end of stream
)

var (
	numBitsPerRange = []int{7, 9, 12}
	zeroDoDRange    *dodRange
	dodRanges       []*dodRange
	defaultDoDRange *dodRange
)

type bit byte

type dodRange struct {
	min           int64
	max           int64
	opcode        uint64
	numOpcodeBits int
	numDoDBits    int
}

func init() {
	zeroDoDRange = &dodRange{0, 0, 0x0, 1, 0}
	numRanges := len(numBitsPerRange)
	dodRanges = make([]*dodRange, numRanges)
	numOpcodeBits := 1
	i := 0
	for i < numRanges {
		dodRanges[i] = &dodRange{
			min:           -(1 << uint(numBitsPerRange[i]-1)),
			max:           (1 << uint(numBitsPerRange[i]-1)) - 1,
			opcode:        uint64((1 << uint(i+1)) - 1),
			numOpcodeBits: numOpcodeBits + 1,
			numDoDBits:    numBitsPerRange[i],
		}
		i++
		numOpcodeBits++
	}
	defaultDoDRange = &dodRange{math.MinInt64, math.MaxInt64, (1 << uint(i+1)) - 1, numOpcodeBits, 32}
}
