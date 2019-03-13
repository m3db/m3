// Copyright (c) 2019 Uber Technologies, Inc.
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

package encoding

const (
	TSZOpcodeZeroValueXOR        = 0x0
	TSZOpcodeContainedValueXOR   = 0x2
	TSZOpcodeUncontainedValueXOR = 0x3

	TSZOpcodeNoUpdateSig = 0x0
	TSZOpcodeUpdateSig   = 0x1
	TSZOpcodeZeroSig     = 0x0
	TSZOpcodeNonZeroSig  = 0x1

	TSZNumSigBits = 6

	sigDiffThreshold   = uint8(3)
	sigRepeatThreshold = uint8(5)
)

// WriteTSZXOR writes the TSZ XOR into the provided stream given the previous
// XOR and the current XOR.
func WriteTSZXOR(
	stream OStream,
	prevXOR, curXOR uint64) {
	if curXOR == 0 {
		stream.WriteBits(TSZOpcodeZeroValueXOR, 1)
		return
	}

	// NB(xichen): can be further optimized by keeping track of leading and trailing zeros in enc.
	prevLeading, prevTrailing := LeadingAndTrailingZeros(prevXOR)
	curLeading, curTrailing := LeadingAndTrailingZeros(curXOR)
	if curLeading >= prevLeading && curTrailing >= prevTrailing {
		stream.WriteBits(TSZOpcodeContainedValueXOR, 2)
		stream.WriteBits(curXOR>>uint(prevTrailing), 64-prevLeading-prevTrailing)
		return
	}

	stream.WriteBits(TSZOpcodeUncontainedValueXOR, 2)
	stream.WriteBits(uint64(curLeading), 6)
	numMeaningfulBits := 64 - curLeading - curTrailing
	// numMeaningfulBits is at least 1, so we can subtract 1 from it and encode it in 6 bits
	stream.WriteBits(uint64(numMeaningfulBits-1), 6)
	stream.WriteBits(curXOR>>uint(curTrailing), numMeaningfulBits)
}

// WriteIntSig writes the number of significant bits of the diff if it has changed and
// updates the IntSigBitsTracker.
func WriteIntSig(os OStream, sigTracker *IntSigBitsTracker, sig uint8) {
	if sigTracker.NumSig != sig {
		os.WriteBit(TSZOpcodeUpdateSig)
		if sig == 0 {
			os.WriteBit(TSZOpcodeZeroSig)
		} else {
			os.WriteBit(TSZOpcodeNonZeroSig)
			os.WriteBits(uint64(sig-1), TSZNumSigBits)
		}
	} else {
		os.WriteBit(TSZOpcodeNoUpdateSig)
	}

	sigTracker.NumSig = sig
}

// IntSigBitsTracker is used to track the number of significant bits
// which should be used to encode the delta between two integers.
type IntSigBitsTracker struct {
	NumSig             uint8 // current largest number of significant places for int diffs
	CurHighestLowerSig uint8
	NumLowerSig        uint8
}

// TrackNewSig gets the new number of significant bits given the
// number of significant bits of the current diff. It takes into
// account thresholds to try and find a value that's best for the
// current data
func (t *IntSigBitsTracker) TrackNewSig(numSig uint8) uint8 {
	newSig := t.NumSig

	if numSig > t.NumSig {
		newSig = numSig
	} else if t.NumSig-numSig >= sigDiffThreshold {
		if t.NumLowerSig == 0 {
			t.CurHighestLowerSig = numSig
		} else if numSig > t.CurHighestLowerSig {
			t.CurHighestLowerSig = numSig
		}

		t.NumLowerSig++
		if t.NumLowerSig >= sigRepeatThreshold {
			newSig = t.CurHighestLowerSig
			t.NumLowerSig = 0
		}

	} else {
		t.NumLowerSig = 0
	}

	return newSig
}

// Reset resets the IntSigBitsTracker for reuse.
func (t *IntSigBitsTracker) Reset() {
	t.NumSig = 0
	t.CurHighestLowerSig = 0
	t.NumLowerSig = 0
}
