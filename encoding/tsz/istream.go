package tsz

import (
	"bufio"
	"io"
	"math"
)

// istream encapsulates a readable stream.
type istream struct {
	r         *bufio.Reader // encoded stream
	err       error         // error encountered
	current   byte          // current byte we are working off of
	remaining int           // bits remaining in current to be read
}

func newIStream(reader io.Reader) *istream {
	return &istream{r: bufio.NewReader(reader)}
}

func (is *istream) ReadBit() (bit, error) {
	if is.err != nil {
		return 0, is.err
	}
	if is.remaining == 0 {
		if err := is.readByteFromStream(); err != nil {
			return 0, err
		}
	}
	return bit(is.consumeBuffer(1)), nil
}

func (is *istream) ReadByte() (byte, error) {
	if is.err != nil {
		return 0, is.err
	}
	remaining := is.remaining
	res := is.consumeBuffer(remaining)
	if remaining == 8 {
		return res, nil
	}
	if err := is.readByteFromStream(); err != nil {
		return 0, err
	}
	res = (res << uint(8-remaining)) | is.consumeBuffer(8-remaining)
	return res, nil
}

func (is *istream) ReadBits(numBits int) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	var res uint64
	for numBits >= 8 {
		byteRead, err := is.ReadByte()
		if err != nil {
			return 0, err
		}
		res = (res << 8) | uint64(byteRead)
		numBits -= 8
	}
	for numBits > 0 {
		bitRead, err := is.ReadBit()
		if err != nil {
			return 0, err
		}
		res = (res << 1) | uint64(bitRead)
		numBits--
	}
	return res, nil
}

func (is *istream) PeekBits(numBits int) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	// check the last byte first
	if numBits <= is.remaining {
		return uint64(readBitsInByte(is.current, numBits)), nil
	}
	// now check the bytes buffered and read more if necessary.
	numBitsRead := is.remaining
	res := uint64(readBitsInByte(is.current, is.remaining))
	numBytesToRead := int(math.Ceil(float64(numBits-numBitsRead) / 8))
	bytesRead, err := is.r.Peek(numBytesToRead)
	if err != nil {
		return 0, err
	}
	for i := 0; i < numBytesToRead-1; i++ {
		res = (res << 8) | uint64(bytesRead[i])
		numBitsRead += 8
	}
	remainder := readBitsInByte(bytesRead[numBytesToRead-1], numBits-numBitsRead)
	res = (res << uint(numBits-numBitsRead)) | uint64(remainder)
	return res, nil
}

// readBitsInByte reads numBits in byte b.
func readBitsInByte(b byte, numBits int) byte {
	return b >> uint(8-numBits)
}

// consumeBuffer consumes numBits in is.current.
func (is *istream) consumeBuffer(numBits int) byte {
	res := readBitsInByte(is.current, numBits)
	is.current <<= uint(numBits)
	is.remaining -= numBits
	return res
}

func (is *istream) readByteFromStream() error {
	is.current, is.err = is.r.ReadByte()
	is.remaining = 8
	return is.err
}

func (is *istream) Reset(r io.Reader) {
	is.r.Reset(r)
	is.err = nil
	is.current = 0
	is.remaining = 0
}
