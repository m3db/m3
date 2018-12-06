// Copyright (c) 2018 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package msgpack

import (
	"errors"
	"fmt"
	"math"

	"github.com/m3db/m3/src/dbnode/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2/codes"
)

// DecodeLogEntryFast encodes a commit log entry with no buffering and using optimized helper
// functions that bypass the msgpack encoding library by manually inlining the equivalent code.
//
// The reason we had to bypass the msgpack encoding library is that during perf testing we found that
// this function was spending most of its time setting up stack frames for function calls. While
// the overhead of a function call in Golang is small, when every helper function does nothing more
// than write a few bytes to an in-memory array the function call overhead begins to dominate,
// especially when each call to this function results in dozens of such helper function calls.
//
// Manually inlining the msgpack encoding results in a lot of code duplication for this one path, but
// we pay the price because this is the most frequently called function in M3DB and it indirectly
// applies back-pressure on every other part of the system via the commitlog queue. As a result, almost
// any performance gains that can be had in this function are worth it.
//
// Before modifying this function, please run the BenchmarkLogEntryEncoderFast benchmark as a small
// degradation in this functions performance can have a substantial impact on M3DB.
func DecodeLogEntryFast(b []byte) (schema.LogEntry, error) {
	schema := schema.LogEntry{}
	b = b[len(logEntryHeader):]

	var err error
	schema.Index, b, err = decodeUint(b)
	if err != nil {
		return schema, err
	}

	schema.Create, b, err = decodeInt(b)
	if err != nil {
		return schema, err
	}
	schema.Metadata, b, err = decodeBytes(b)
	if err != nil {
		return schema, err
	}

	schema.Timestamp, b, err = decodeInt(b)
	if err != nil {
		return schema, err
	}

	schema.Value, b, err = decodeFloat64(b)
	if err != nil {
		return schema, err
	}

	unit, b, err := decodeUint(b)
	if err != nil {
		return schema, err
	}
	schema.Unit = uint32(unit)

	schema.Annotation, b, err = decodeBytes(b)
	if err != nil {
		return schema, err
	}

	return schema, nil
}

// DecodeLogMetadataFast is the same as EncodeLogEntryFast except for the metadata
// entries instead of the data entries.
func DecodeLogMetadataFast(b []byte) (schema.LogMetadata, error) {
	metadata := schema.LogMetadata{}

	b = b[len(logMetadataHeader):]

	id, b, err := decodeBytes(b)
	if err != nil {
		return metadata, err
	}
	metadata.ID = id

	metadata.Namespace, b, err = decodeBytes(b)
	if err != nil {
		return metadata, err
	}

	shard, b, err := decodeUint(b)
	if err != nil {
		return metadata, err
	}
	metadata.Shard = uint32(shard)

	metadata.EncodedTags, b, err = decodeBytes(b)
	if err != nil {
		return metadata, err
	}

	return metadata, nil
}

func decodeArrayLen(b []byte) (int, []byte, error) {
	c := b[0]
	if c == codes.Nil {
		return -1, b[1:], nil
	}

	if c >= codes.FixedArrayLow && c <= codes.FixedArrayHigh {
		return int(c & codes.FixedArrayMask), b[1:], nil
	}

	v, b, err := decodeInt(b)
	return int(v), b, err
}

func decodeInt(b []byte) (int64, []byte, error) {
	c := b[0]
	b = b[1:]

	if c == codes.Nil {
		return 0, b, nil
	}

	if codes.IsFixedNum(c) {
		return int64(int8(c)), b, nil
	}

	switch c {
	case codes.Uint8:
		// Zero?
		return int64(b[0]), b[1:], nil
	case codes.Int8:
		return int64(int8(b[0])), b[1:], nil
	case codes.Uint16:
		return int64((uint16(b[0]) << 8) | uint16(b[1])), b[2:], nil
	case codes.Int16:
		return int64(int16((uint16(b[0]) << 8) | uint16(b[1]))), b[2:], nil
	case codes.Uint32:
		return int64((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3])), b[4:], nil
	case codes.Int32:
		return int64(int32((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3]))), b[4:], nil
	case codes.Uint64, codes.Int64:
		return int64((uint64(b[0]) << 56) |
			(uint64(b[1]) << 48) |
			(uint64(b[2]) << 40) |
			(uint64(b[3]) << 32) |
			(uint64(b[4]) << 24) |
			(uint64(b[5]) << 16) |
			(uint64(b[6]) << 8) |
			uint64(b[7])), b[8:], nil
	default:
		return 0, nil, errors.New("invalid code")
	}
}

func decodeUint(b []byte) (uint64, []byte, error) {
	c := b[0]
	b = b[1:]

	if c == codes.Nil {
		return 0, b, nil
	}

	if codes.IsFixedNum(c) {
		return uint64(int8(c)), b, nil
	}

	switch c {
	case codes.Uint8:
		return uint64(b[0]), b[1:], nil
	case codes.Int8:
		return uint64(int8(b[0])), b[1:], nil
	case codes.Uint16:
		return uint64((uint16(b[0]) << 8) | uint16(b[1])), b[2:], nil
	case codes.Int16:
		return uint64(int16((uint16(b[0]) << 8) | uint16(b[1]))), b[2:], nil
	case codes.Uint32:
		return uint64((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3])), b[4:], nil
	case codes.Int32:
		return uint64(int32((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3]))), b[4:], nil
	case codes.Uint64, codes.Int64:
		return uint64((uint64(b[0]) << 56) |
			(uint64(b[1]) << 48) |
			(uint64(b[2]) << 40) |
			(uint64(b[3]) << 32) |
			(uint64(b[4]) << 24) |
			(uint64(b[5]) << 16) |
			(uint64(b[6]) << 8) |
			uint64(b[7])), b[8:], nil
	default:
		return 0, nil, errors.New("invalid code")
	}
}

func decodeFloat64(b []byte) (float64, []byte, error) {
	c := b[0]
	b = b[1:]

	if c == codes.Float {
		i := (uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3])
		return float64(math.Float32frombits(i)), b[4:], nil
	}

	if c == codes.Double {
		i := (uint64(b[0]) << 56) |
			(uint64(b[1]) << 48) |
			(uint64(b[2]) << 40) |
			(uint64(b[3]) << 32) |
			(uint64(b[4]) << 24) |
			(uint64(b[5]) << 16) |
			(uint64(b[6]) << 8) |
			uint64(b[7])
		return float64(math.Float64frombits(i)), b[8:], nil
	}

	return 0, b, fmt.Errorf("invalid code: %d decoding float64", c)
}

func decodeBytesLen(b []byte) (int, []byte, error) {
	c := b[0]
	b = b[1:]

	if c == codes.Nil {
		return -1, b, nil
	} else if codes.IsFixedString(c) {
		return int(c & codes.FixedStrMask), b, nil
	}
	switch c {
	case codes.Str8, codes.Bin8:
		return int(b[0]), b[1:], nil
	case codes.Str16, codes.Bin16:
		return int((uint16(b[0]) << 8) | uint16(b[1])), b[2:], nil
	case codes.Str32, codes.Bin32:
		return int(int32((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3]))), b[4:], nil
	}
	return -1, b, fmt.Errorf("invalid code: %d decoding bytes length", c)
}

func decodeBytes(b []byte) ([]byte, []byte, error) {
	bytesLen, b, err := decodeBytesLen(b)
	if err != nil {
		return nil, nil, err
	}

	if bytesLen == -1 {
		return nil, nil, nil
	}

	return b[:bytesLen], b[bytesLen:], nil
}
