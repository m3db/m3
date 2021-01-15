package serialize

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/metrics/metric/id"
)

type decoderFast struct {
	length    int
	remaining int

	encodedTags []byte

	currentName, currentValue []byte

	err error
}

func NewFastTagDecoder() id.SortedTagIterator {
	return &decoderFast{}
}

func (d *decoderFast) Reset(encodedTags []byte) {
	header := byteOrder.Uint16(encodedTags[:2])
	encodedTags = encodedTags[2:]
	if header != headerMagicNumber {
		d.err = errors.New("")
		return
	}

	d.length = int(byteOrder.Uint16(encodedTags[:2]))
	d.encodedTags = encodedTags[2:]
	d.remaining = d.length
}

func (d *decoderFast) Next() bool {
	if d.remaining == 0 {
		return false
	}

	d.remaining--

	if len(d.encodedTags) < 2 {
		d.err = fmt.Errorf("missing size for tag name: index=%d", d.length-d.remaining)
		return false
	}
	numBytesName := int(byteOrder.Uint16(d.encodedTags[:2]))
	if numBytesName == 0 {
		d.err = errors.New("")
		return false
	}
	d.encodedTags = d.encodedTags[2:]

	d.currentName = d.encodedTags[:numBytesName]
	d.encodedTags = d.encodedTags[numBytesName:]

	if len(d.encodedTags) < 2 {
		d.err = fmt.Errorf("missing size for tag value: index=%d", d.length-d.remaining)
		return false
	}

	numBytesValue := int(byteOrder.Uint16(d.encodedTags[:2]))
	d.encodedTags = d.encodedTags[2:]

	d.currentValue = d.encodedTags[:numBytesValue]
	d.encodedTags = d.encodedTags[numBytesValue:]
	return true
}

func (d *decoderFast) Current() ([]byte, []byte) {
	return d.currentName, d.currentValue
}

func (d *decoderFast) Err() error {
	return d.err
}

func (d *decoderFast) Close() {
}

func (d *decoderFast) NumTags() int {
	return d.length
}
