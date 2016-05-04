package tsz

import (
	"bufio"
	"io"
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
	res = (is.consumeBuffer(8-remaining) << uint(remaining)) | res
	return res, nil
}

func (is *istream) ReadBits(numBits int) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	var res uint64
	var shift uint
	for numBits >= 8 {
		byteRead, err := is.ReadByte()
		if err != nil {
			return 0, err
		}
		res = (uint64(byteRead) << shift) | res
		shift += 8
		numBits -= 8
	}
	for numBits > 0 {
		bitRead, err := is.ReadBit()
		if err != nil {
			return 0, err
		}
		res = (uint64(bitRead) << shift) | res
		shift++
		numBits--
	}
	return res, nil
}

// consumeBuffer consumes numBits in is.current.
func (is *istream) consumeBuffer(numBits int) byte {
	mask := (1 << uint(numBits)) - 1
	res := is.current & byte(mask)
	is.current >>= uint(numBits)
	is.remaining -= numBits
	return res
}

func (is *istream) readByteFromStream() error {
	is.current, is.err = is.r.ReadByte()
	is.remaining = 8
	return is.err
}

func (is *istream) Reset(r io.Reader) {
	is.r = bufio.NewReader(r)
	is.err = nil
	is.current = 0
	is.remaining = 0
}
