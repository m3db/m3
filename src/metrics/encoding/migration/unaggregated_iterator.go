// Copyright (c) 2018 Uber Technologies, Inc.
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

package migration

import (
	"fmt"

	"github.com/m3db/m3metrics/encoding"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3metrics/encoding/protobuf"
)

// UnaggregatedIterator decodes unaggregated metrics.
type UnaggregatedIterator interface {
	// Next returns true if there are more items to decode.
	Next() bool

	// Current returns the current decoded value.
	Current() encoding.UnaggregatedMessageUnion

	// Err returns the error encountered during decoding, if any.
	Err() error

	// Close closes the iterator.
	Close()
}

type unaggregatedIterator struct {
	reader encoding.ByteReadScanner

	msgpackIt  msgpack.UnaggregatedIterator
	protobufIt protobuf.UnaggregatedIterator
	msg        encoding.UnaggregatedMessageUnion
	closed     bool
	err        error
}

// NewUnaggregatedIterator creates a new unaggregated iterator.
func NewUnaggregatedIterator(
	reader encoding.ByteReadScanner,
	msgpackItOpts msgpack.UnaggregatedIteratorOptions,
	protobufItOpts protobuf.UnaggregatedOptions,
) UnaggregatedIterator {
	msgpackIt := msgpack.NewUnaggregatedIterator(reader, msgpackItOpts)
	protobufIt := protobuf.NewUnaggregatedIterator(reader, protobufItOpts)
	return &unaggregatedIterator{
		reader:     reader,
		msgpackIt:  msgpackIt,
		protobufIt: protobufIt,
	}
}

func (it *unaggregatedIterator) Next() bool {
	if it.closed || it.Err() != nil {
		return false
	}
	protocol, err := it.decodeProtocolType()
	if err != nil {
		return false
	}
	switch protocol {
	case msgpackProtocol:
		if !it.msgpackIt.Next() {
			return false
		}
		metric := it.msgpackIt.Metric()
		policiesList := it.msgpackIt.PoliciesList()
		msg, err := toUnaggregatedMessageUnion(metric, policiesList)
		if err != nil {
			it.err = err
			return false
		}
		it.msg = msg
		return true
	case protobufProtocol:
		if !it.protobufIt.Next() {
			return false
		}
		it.msg = it.protobufIt.Current()
		return true
	default:
		it.err = fmt.Errorf("unexpected protocol type: %v", protocol)
		return false
	}
}

func (it *unaggregatedIterator) Current() encoding.UnaggregatedMessageUnion { return it.msg }

func (it *unaggregatedIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if err := it.msgpackIt.Err(); err != nil {
		return err
	}
	return it.protobufIt.Err()
}

func (it *unaggregatedIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.msgpackIt.Close()
	it.msgpackIt = nil
	it.protobufIt.Close()
	it.protobufIt = nil
	it.msg = encoding.UnaggregatedMessageUnion{}
	it.err = nil
}

func (it *unaggregatedIterator) decodeProtocolType() (protocolType, error) {
	// Peek the first byte of the message.
	b, err := it.reader.ReadByte()
	if err != nil {
		it.err = err
		return unknownProtocolType, err
	}

	// Put the byte back.
	if err := it.reader.UnreadByte(); err != nil {
		it.err = err
		return unknownProtocolType, err
	}

	// NB: currently the msgpack encoder always encodes the version first as the first byte
	// of the msgpack message, and since we are currently at version 1, the first byte will
	// always be an odd number (i.e., 1). On the other hand, the protobuf encoder always
	// encodes the message length as a varint, in which case the first byte will always
	// be an even number. As such we can use the last bit of the byte to distinguish the
	// different protocols.
	protocol := msgpackProtocol
	if (b & 1) == 0 {
		protocol = protobufProtocol
	}
	return protocol, nil
}

type protocolType int

const (
	unknownProtocolType protocolType = iota
	msgpackProtocol
	protobufProtocol
)
