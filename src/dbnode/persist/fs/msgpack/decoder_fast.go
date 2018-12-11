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
	"fmt"
	"math"

	"github.com/m3db/m3/src/dbnode/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2/codes"
)

const (
	decodeLogEntryFuncName    = "decodeLogEntry"
	decodeLogMetadataFuncName = "decodeLogMetadata"
	decodeArrayLenFuncName    = "decodeArrayLen"
	decodeIntFuncName         = "decodeInt"
	decodeUIntFuncName        = "decodeUInt"
	decodeFloat64FuncName     = "decodeFloat64"
	decodeBytesLenFuncName    = "decodeBytesLen"
	decodeBytesFuncName       = "decodeBytes"
)

// DecodeLogEntryFast decodes a commit log entry with no buffering and using optimized helper
// functions that bypass the msgpack decoding library by manually inlining the equivalent code.
//
// The reason we had to bypass the msgpack decoding library is that during perf testing we found that
// this function was spending most of its time setting up stack frames for function calls. While
// the overhead of a function call in Golang is small, when every helper function does nothing more
// than read a few bytes from an in-memory array the function call overhead begins to dominate,
// especially when each call to this function results in dozens of such helper function calls.
//
// Manually inlining the msgpack decoding results in a lot of code duplication for this one path, but
// we pay the price because this codepath is one of the primary bottlenecks influencing how fast we
// can bootstrap M3DB from the commitlog. As a result, almost any performance gains that can be had in
// this function are worth it.
//
// Before modifying this function, please run the BenchmarkLogEntryDecodeFast benchmark.
//
// Also note that there are extensive prop tests for this function in the encoder_decoder_prop_test.go
// file which verify its correctness, as well as its resilience to arbitrary data corruption and truncation.
func DecodeLogEntryFast(b []byte) (schema.LogEntry, error) {
	schema := schema.LogEntry{}

	if len(b) < len(logEntryHeader) {
		return schema, notEnoughBytesError(
			decodeLogEntryFuncName, len(logEntryHeader), len(b))
	}
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

// DecodeLogMetadataFast is the same as DecodeLogEntryFast except for the metadata
// entries instead of the data entries.
func DecodeLogMetadataFast(b []byte) (schema.LogMetadata, error) {
	metadata := schema.LogMetadata{}

	if len(b) < len(logMetadataHeader) {
		return metadata, notEnoughBytesError(
			decodeLogMetadataFuncName, len(logMetadataHeader), len(b))
	}
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
	if len(b) < 1 {
		return 0, nil, notEnoughBytesError(decodeArrayLenFuncName, 1, len(b))
	}

	c := b[0]
	if c == codes.Nil {
		return -1, b[1:], nil
	}

	if len(b) < 2 {
		return 0, nil, notEnoughBytesError(decodeArrayLenFuncName, 1, len(b))
	}
	if c >= codes.FixedArrayLow && c <= codes.FixedArrayHigh {
		return int(c & codes.FixedArrayMask), b[1:], nil
	}

	v, b, err := decodeInt(b)
	return int(v), b, err
}

func decodeInt(b []byte) (int64, []byte, error) {
	if len(b) < 1 {
		return 0, nil, notEnoughBytesError(decodeIntFuncName, 1, len(b))
	}

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
		if len(b) < 1 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 1, len(b))
		}

		return int64(b[0]), b[1:], nil
	case codes.Int8:
		if len(b) < 1 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 1, len(b))
		}

		return int64(int8(b[0])), b[1:], nil
	case codes.Uint16:
		if len(b) < 2 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 2, len(b))
		}

		return int64((uint16(b[0]) << 8) | uint16(b[1])), b[2:], nil
	case codes.Int16:
		if len(b) < 2 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 2, len(b))
		}

		return int64(int16((uint16(b[0]) << 8) | uint16(b[1]))), b[2:], nil
	case codes.Uint32:
		if len(b) < 4 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 4, len(b))
		}

		return int64((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3])), b[4:], nil
	case codes.Int32:
		if len(b) < 4 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 4, len(b))
		}

		return int64(int32((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3]))), b[4:], nil
	case codes.Uint64, codes.Int64:
		if len(b) < 8 {
			return 0, nil, notEnoughBytesError(decodeIntFuncName, 8, len(b))
		}

		return int64((uint64(b[0]) << 56) |
			(uint64(b[1]) << 48) |
			(uint64(b[2]) << 40) |
			(uint64(b[3]) << 32) |
			(uint64(b[4]) << 24) |
			(uint64(b[5]) << 16) |
			(uint64(b[6]) << 8) |
			uint64(b[7])), b[8:], nil
	default:
		return 0, nil, fmt.Errorf("error decoding int: invalid code: %d", c)
	}
}

func decodeUint(b []byte) (uint64, []byte, error) {
	if len(b) < 1 {
		return 0, nil, notEnoughBytesError(decodeUIntFuncName, 1, len(b))
	}

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
		if len(b) < 1 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 1, len(b))
		}

		return uint64(b[0]), b[1:], nil
	case codes.Int8:
		if len(b) < 1 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 1, len(b))
		}

		return uint64(int8(b[0])), b[1:], nil
	case codes.Uint16:
		if len(b) < 2 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 2, len(b))
		}

		return uint64((uint16(b[0]) << 8) | uint16(b[1])), b[2:], nil
	case codes.Int16:
		if len(b) < 2 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 2, len(b))
		}

		return uint64(int16((uint16(b[0]) << 8) | uint16(b[1]))), b[2:], nil
	case codes.Uint32:
		if len(b) < 4 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 4, len(b))
		}

		return uint64((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3])), b[4:], nil
	case codes.Int32:
		if len(b) < 4 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 4, len(b))
		}

		return uint64(int32((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3]))), b[4:], nil
	case codes.Uint64, codes.Int64:
		if len(b) < 8 {
			return 0, nil, notEnoughBytesError(decodeUIntFuncName, 8, len(b))
		}

		return uint64((uint64(b[0]) << 56) |
			(uint64(b[1]) << 48) |
			(uint64(b[2]) << 40) |
			(uint64(b[3]) << 32) |
			(uint64(b[4]) << 24) |
			(uint64(b[5]) << 16) |
			(uint64(b[6]) << 8) |
			uint64(b[7])), b[8:], nil
	default:
		return 0, nil, fmt.Errorf("error decoding uint: invalid code: %d", c)
	}
}

func decodeFloat64(b []byte) (float64, []byte, error) {
	if len(b) < 5 {
		return 0, nil, notEnoughBytesError(decodeFloat64FuncName, 5, len(b))
	}

	c := b[0]
	b = b[1:]

	if c == codes.Float {
		i := (uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3])
		return float64(math.Float32frombits(i)), b[4:], nil
	}

	if len(b) < 8 {
		return 0, nil, notEnoughBytesError(decodeFloat64FuncName, 8, len(b))
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

	return 0, b, fmt.Errorf("error decoding float64: invalid code: %d", c)
}

func decodeBytesLen(b []byte) (int, []byte, error) {
	if len(b) < 1 {
		return 0, nil, notEnoughBytesError(decodeBytesLenFuncName, 1, len(b))
	}

	c := b[0]
	b = b[1:]

	if c == codes.Nil {
		return -1, b, nil
	} else if codes.IsFixedString(c) {
		return int(c & codes.FixedStrMask), b, nil
	}

	switch c {
	case codes.Str8, codes.Bin8:
		if len(b) < 1 {
			return 0, nil, notEnoughBytesError(decodeBytesLenFuncName, 1, len(b))
		}

		return int(b[0]), b[1:], nil
	case codes.Str16, codes.Bin16:
		if len(b) < 2 {
			return 0, nil, notEnoughBytesError(decodeBytesLenFuncName, 2, len(b))
		}

		return int((uint16(b[0]) << 8) | uint16(b[1])), b[2:], nil
	case codes.Str32, codes.Bin32:
		if len(b) < 4 {
			return 0, nil, notEnoughBytesError(decodeBytesLenFuncName, 4, len(b))
		}

		return int(int32((uint32(b[0]) << 24) |
			(uint32(b[1]) << 16) |
			(uint32(b[2]) << 8) |
			uint32(b[3]))), b[4:], nil
	}
	return -1, nil, fmt.Errorf("error decoding bytes len: invalid code: %d", c)
}

func decodeBytes(b []byte) ([]byte, []byte, error) {
	bytesLen, b, err := decodeBytesLen(b)
	if err != nil {
		return nil, nil, err
	}

	if bytesLen == -1 {
		return nil, b, nil
	}

	// Smaller than zero check to handle corrupt data
	if len(b) < bytesLen || bytesLen < 0 {
		return nil, nil, notEnoughBytesError(decodeBytesFuncName, bytesLen, len(b))
	}

	return b[:bytesLen], b[bytesLen:], nil
}

func notEnoughBytesError(funcName string, expected, actual int) error {
	return fmt.Errorf(
		"not enough bytes for msgpack decode in %s, expected %d but had %d",
		funcName, expected, actual)
}
