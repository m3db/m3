package tagfiltertree

import "math/bits"

type PointerSet struct {
	bits [2]uint64 // Using 2 uint64 gives us 128 bits (0 to 127).
}

// Set adds a pointer at index i (0 <= i < 127).
func (ps *PointerSet) Set(i byte) {
	if i < 64 {
		ps.bits[0] |= (1 << i)
	} else {
		ps.bits[1] |= (1 << (i - 64))
	}
}

// IsSet checks if a pointer is present at index i.
func (ps *PointerSet) IsSet(i byte) bool {
	if i < 64 {
		return ps.bits[0]&(1<<i) != 0
	}
	return ps.bits[1]&(1<<(i-64)) != 0
}

// CountSetBitsUntil counts how many bits are set to 1 up to index i (inclusive).
func (ps *PointerSet) CountSetBitsUntil(i byte) int {
	if i < 64 {
		// Count bits in the first uint64 up to index i.
		return bits.OnesCount64(ps.bits[0] & ((1 << (i + 1)) - 1))
	} else {
		// Count all bits in the first uint64.
		count := bits.OnesCount64(ps.bits[0])
		// Count bits in the second uint64 up to index i - 64.
		count += bits.OnesCount64(ps.bits[1] & ((1 << (i - 64 + 1)) - 1))
		return count
	}
}
