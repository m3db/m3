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
	// decodeRootObject
	//     version
	_, b, err := decodeInt(b)
	if err != nil {
		return schema, err
	}
	// decodeRootObject
	//     numFieldsToSkip
	//          numObjectFields
	_, b, err = decodeArrayLen(b)
	if err != nil {
		return schema, err
	}
	// decodeRootObject
	//     numFieldsToSkip
	//        objectType
	_, b, err = decodeInt(b)

	// decodeLogEntry
	//     index int
	schema.Index, b, err = decodeUint(b)
	// decodeLogEntry
	//     create int
	schema.Create, b, err = decodeInt(b)
	// decodeLogEntry
	//     metadata (bytes)
	// decodeLogEntry
	//     value (float64)
	// decodeLogEntry
	//     unit (int)
	// decodeLogEntry
	//     annotation (bytes)
	panic(objectType(ot))

	return schema.LogEntry{}, nil
}

// DecodeLogMetadataFast is the same as EncodeLogEntryFast except for the metadata
// entries instead of the data entries.
func DecodeLogMetadataFast(b []byte, entry schema.LogMetadata) ([]byte, error) {
	if logMetadataHeaderErr != nil {
		return nil, logMetadataHeaderErr
	}

	// TODO(rartoul): Can optimize this further by storing the version as part of the
	// info for the commit log file itself, instead of in every entry.
	// https://github.com/m3db/m3/issues/1161
	b = append(b, logMetadataHeader...)
	b = encodeBytes(b, entry.ID)
	b = encodeBytes(b, entry.Namespace)
	b = encodeVarUint64(b, uint64(entry.Shard))
	b = encodeBytes(b, entry.EncodedTags)

	return b, nil
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
		return int64(b[1]), b[1:], nil
	case codes.Int8:
		return int64(int8(b[1])), b[1:], nil
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
		return uint64(b[1]), b[1:], nil
	case codes.Int8:
		return uint64(int8(b[1])), b[1:], nil
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

func decodeBytes(b []byte) ([]byte, int, int) {
	// If we need to allocate new space for decoded byte slice, we delegate it to msgpack
	// API which allocates a new slice under the hood, otherwise we simply locate the byte
	// slice as part of the encoded byte stream and return it
	var (
		bytesLen     = dec.decodeBytesLen()
		backingBytes = dec.reader.Bytes()
		numBytes     = len(backingBytes)
		currPos      = int(int64(numBytes) - dec.reader.Remaining())
	)

	if dec.err != nil {
		return nil, -1, -1
	}
	// NB(xichen): DecodeBytesLen() returns -1 if the byte slice is nil
	if bytesLen == -1 {
		return nil, -1, -1
	}

	targetPos := currPos + bytesLen
	if bytesLen < 0 || currPos < 0 || targetPos > numBytes {
		dec.err = fmt.Errorf("invalid currPos %d, bytesLen %d, numBytes %d", currPos, bytesLen, numBytes)
		return nil, -1, -1
	}
	if err := dec.reader.Skip(int64(bytesLen)); err != nil {
		dec.err = err
		return nil, -1, -1
	}
	value = backingBytes[currPos:targetPos]
	return value, currPos, bytesLen
}
