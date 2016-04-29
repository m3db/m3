package tsz

func leadingAndTrailingZeros(v uint64) (int, int) {
	if v == 0 {
		return 64, 0
	}

	numTrailing := 0
	for tmp := v; (tmp & 1) == 0; tmp >>= 1 {
		numTrailing++
	}

	numLeading := 0
	for tmp := v; (tmp & (1 << 63)) == 0; tmp <<= 1 {
		numLeading++
	}

	return numLeading, numTrailing
}

// signExtend sign extends the highest bit of v which has numBits (<=64)
func signExtend(v uint64, numBits int) int64 {
	shift := uint(64 - numBits)
	return (int64(v) << shift) >> shift
}
