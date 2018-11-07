// Copyright (c) 2016 Uber Technologies, Inc
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
	"bytes"
	"math"

	"github.com/m3db/m3/src/dbnode/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2"
	"gopkg.in/vmihailenco/msgpack.v2/codes"
)

type encodeVersionFn func(value int)
type encodeNumObjectFieldsForFn func(value objectType)
type encodeVarintFn func(value int64)
type encodeVarUintFn func(value uint64)
type encodeFloat64Fn func(value float64)
type encodeBytesFn func(value []byte)
type encodeArrayLenFn func(value int)

// Encoder encodes data in msgpack format for persistence
type Encoder struct {
	buf *bytes.Buffer
	enc *msgpack.Encoder
	err error

	encodeVersionFn            encodeVersionFn
	encodeNumObjectFieldsForFn encodeNumObjectFieldsForFn
	encodeVarintFn             encodeVarintFn
	encodeVarUintFn            encodeVarUintFn
	encodeFloat64Fn            encodeFloat64Fn
	encodeBytesFn              encodeBytesFn
	encodeArrayLenFn           encodeArrayLenFn

	legacy legacyEncodingOptions
}

type legacyEncodingOptions struct {
	encodeLegacyV1IndexInfo  bool
	encodeLegacyV1IndexEntry bool
	decodeLegacyV1IndexInfo  bool
	decodeLegacyV1IndexEntry bool
}

var defaultlegacyEncodingOptions = legacyEncodingOptions{
	encodeLegacyV1IndexInfo:  false,
	encodeLegacyV1IndexEntry: false,
	decodeLegacyV1IndexInfo:  false,
	decodeLegacyV1IndexEntry: false,
}

// NewEncoder creates a new encoder
func NewEncoder() *Encoder {
	return newEncoder(defaultlegacyEncodingOptions)
}

func newEncoder(legacy legacyEncodingOptions) *Encoder {
	buf := bytes.NewBuffer(nil)
	enc := &Encoder{
		buf: buf,
		enc: msgpack.NewEncoder(buf),
	}

	enc.encodeVersionFn = enc.encodeVersion
	enc.encodeNumObjectFieldsForFn = enc.encodeNumObjectFieldsFor
	enc.encodeVarintFn = enc.encodeVarint
	enc.encodeVarUintFn = enc.encodeVarUint
	enc.encodeFloat64Fn = enc.encodeFloat64
	enc.encodeBytesFn = enc.encodeBytes
	enc.encodeArrayLenFn = enc.encodeArrayLen

	// Used primarily for testing
	enc.legacy = legacy

	return enc
}

// Reset resets the buffer
func (enc *Encoder) Reset() {
	enc.buf.Truncate(0)
	enc.err = nil
}

// Bytes returns the encoded bytes
func (enc *Encoder) Bytes() []byte { return enc.buf.Bytes() }

// EncodeIndexInfo encodes index info
func (enc *Encoder) EncodeIndexInfo(info schema.IndexInfo) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexInfoVersion, indexInfoType)
	if enc.legacy.encodeLegacyV1IndexInfo {
		enc.encodeIndexInfoV1(info)
	} else {
		enc.encodeIndexInfoV2(info)
	}
	return enc.err
}

// EncodeIndexEntry encodes index entry
func (enc *Encoder) EncodeIndexEntry(entry schema.IndexEntry) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexEntryVersion, indexEntryType)
	if enc.legacy.encodeLegacyV1IndexEntry {
		enc.encodeIndexEntryV1(entry)
	} else {
		enc.encodeIndexEntryV2(entry)
	}
	return enc.err
}

// EncodeIndexSummary encodes index summary
func (enc *Encoder) EncodeIndexSummary(summary schema.IndexSummary) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexSummaryVersion, indexSummaryType)
	enc.encodeIndexSummary(summary)
	return enc.err
}

// EncodeLogInfo encodes commit log info
func (enc *Encoder) EncodeLogInfo(info schema.LogInfo) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logInfoVersion, logInfoType)
	enc.encodeLogInfo(info)
	return enc.err
}

// EncodeLogEntry encodes commit log entry
func (enc *Encoder) EncodeLogEntry(entry schema.LogEntry) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logEntryVersion, logEntryType)
	enc.encodeLogEntry(entry)
	return enc.err
}

// EncodeLogMetadata encodes commit log metadata
func (enc *Encoder) EncodeLogMetadata(entry schema.LogMetadata) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logMetadataVersion, logMetadataType)
	enc.encodeLogMetadata(entry)
	return enc.err
}

// We only keep this method around for the sake of testing
// backwards-compatbility
func (enc *Encoder) encodeIndexInfoV1(info schema.IndexInfo) {
	// Manually encode num fields for testing purposes
	enc.encodeArrayLenFn(6) // v1 had 6 fields
	enc.encodeVarintFn(info.BlockStart)
	enc.encodeVarintFn(info.BlockSize)
	enc.encodeVarintFn(info.Entries)
	enc.encodeVarintFn(info.MajorVersion)
	enc.encodeIndexSummariesInfo(info.Summaries)
	enc.encodeIndexBloomFilterInfo(info.BloomFilter)
}

func (enc *Encoder) encodeIndexInfoV2(info schema.IndexInfo) {
	enc.encodeNumObjectFieldsForFn(indexInfoType)
	enc.encodeVarintFn(info.BlockStart)
	enc.encodeVarintFn(info.BlockSize)
	enc.encodeVarintFn(info.Entries)
	enc.encodeVarintFn(info.MajorVersion)
	enc.encodeIndexSummariesInfo(info.Summaries)
	enc.encodeIndexBloomFilterInfo(info.BloomFilter)
	enc.encodeVarintFn(info.SnapshotTime)
	enc.encodeVarintFn(int64(info.FileType))
}

func (enc *Encoder) encodeIndexSummariesInfo(info schema.IndexSummariesInfo) {
	enc.encodeNumObjectFieldsForFn(indexSummariesInfoType)
	enc.encodeVarintFn(info.Summaries)
}

func (enc *Encoder) encodeIndexBloomFilterInfo(info schema.IndexBloomFilterInfo) {
	enc.encodeNumObjectFieldsForFn(indexBloomFilterInfoType)
	enc.encodeVarintFn(info.NumElementsM)
	enc.encodeVarintFn(info.NumHashesK)
}

// We only keep this method around for the sake of testing
// backwards-compatbility
func (enc *Encoder) encodeIndexEntryV1(entry schema.IndexEntry) {
	// Manually encode num fields for testing purposes
	enc.encodeArrayLenFn(5) // v1 had 5 fields
	enc.encodeVarintFn(entry.Index)
	enc.encodeBytesFn(entry.ID)
	enc.encodeVarintFn(entry.Size)
	enc.encodeVarintFn(entry.Offset)
	enc.encodeVarintFn(entry.Checksum)
}

func (enc *Encoder) encodeIndexEntryV2(entry schema.IndexEntry) {
	enc.encodeNumObjectFieldsForFn(indexEntryType)
	enc.encodeVarintFn(entry.Index)
	enc.encodeBytesFn(entry.ID)
	enc.encodeVarintFn(entry.Size)
	enc.encodeVarintFn(entry.Offset)
	enc.encodeVarintFn(entry.Checksum)
	enc.encodeBytesFn(entry.EncodedTags)
}

func (enc *Encoder) encodeIndexSummary(summary schema.IndexSummary) {
	enc.encodeNumObjectFieldsForFn(indexSummaryType)
	enc.encodeVarintFn(summary.Index)
	enc.encodeBytesFn(summary.ID)
	enc.encodeVarintFn(summary.IndexEntryOffset)
}

func (enc *Encoder) encodeLogInfo(info schema.LogInfo) {
	enc.encodeNumObjectFieldsForFn(logInfoType)
	enc.encodeVarintFn(info.Start)
	enc.encodeVarintFn(info.Duration)
	enc.encodeVarintFn(info.Index)
}

func (enc *Encoder) encodeLogEntry(entry schema.LogEntry) {
	enc.encodeNumObjectFieldsForFn(logEntryType)
	// Encode the index first because the commitlog reader needs this information first
	// to distribute the rest of the decoding to a group of workers.
	enc.encodeVarUintFn(entry.Index)
	enc.encodeVarintFn(entry.Create)
	enc.encodeBytesFn(entry.Metadata)
	enc.encodeVarintFn(entry.Timestamp)
	enc.encodeFloat64Fn(entry.Value)
	enc.encodeVarUintFn(uint64(entry.Unit))
	enc.encodeBytesFn(entry.Annotation)
}

func (enc *Encoder) encodeLogMetadata(metadata schema.LogMetadata) {
	enc.encodeNumObjectFieldsForFn(logMetadataType)
	enc.encodeBytesFn(metadata.ID)
	enc.encodeBytesFn(metadata.Namespace)
	enc.encodeVarUintFn(uint64(metadata.Shard))
	enc.encodeBytesFn(metadata.EncodedTags)
}

func (enc *Encoder) encodeRootObject(version int, objType objectType) {
	enc.encodeVersionFn(version)
	enc.encodeNumObjectFieldsForFn(rootObjectType)
	enc.encodeObjectType(objType)
}

func (enc *Encoder) encodeVersion(version int) {
	enc.encodeVarintFn(int64(version))
}

func (enc *Encoder) encodeNumObjectFieldsFor(objType objectType) {
	_, curr := numFieldsForType(objType)
	enc.encodeArrayLenFn(curr)
}

func (enc *Encoder) encodeObjectType(objType objectType) {
	enc.encodeVarintFn(int64(objType))
}

func (enc *Encoder) encodeVarint(value int64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeInt64(value)
}

func (enc *Encoder) encodeVarUint(value uint64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeUint64(value)
}

func (enc *Encoder) encodeFloat64(value float64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeFloat64(value)
}

func (enc *Encoder) encodeBytes(value []byte) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeBytes(value)
}

func (enc *Encoder) encodeArrayLen(value int) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeArrayLen(value)
}

// EncodeLogEntryFast encodes a commit log entry with no buffering and using optimized helper
// functions that bypass the msgpack encoding library. This results in a lot of code duplication
// for this one path, but we pay the price because this is the most frequently called function
// in M3DB and it directly applies back-pressure on every part of the system. As a result, almost
// any performance gains that can be had in this function are worth it. Please run all the benchmarks
// in encoder_bench.go before making any changes.
func EncodeLogEntryFast(b []byte, entry schema.LogEntry) ([]byte, error) {
	if logEntryHeaderErr != nil {
		return nil, logEntryHeaderErr
	}

	b = append(b, logEntryHeader...)
	b = encodeVarUint64(b, entry.Index)
	b = encodeVarInt64(b, entry.Create)
	b = encodeBytes(b, entry.Metadata)
	b = encodeVarInt64(b, entry.Timestamp)
	b = encodeFloat64(b, entry.Value)
	b = encodeVarUint64(b, uint64(entry.Unit))
	b = encodeBytes(b, entry.Annotation)

	return b, nil
}

// encodeVarUint64 is used in EncodeLogEntryFast which is a very hot path.
// As a result, many of the function calls in this function have been
// manually inlined to reduce function call overhead.
func encodeVarUint64(b []byte, v uint64) []byte {
	if v <= math.MaxInt8 {
		b, buf := growAndReturn(b, 1)
		buf[0] = byte(v)
		return b
	}

	if v <= math.MaxUint8 {
		// Equivalent to: return write1(b, codes.Uint8, v)
		b, buf := growAndReturn(b, 2)
		buf[0] = codes.Uint8
		buf[1] = byte(v)
		return b
	}

	if v <= math.MaxUint16 {
		// Equivalent to: return write2(b, codes.Uint16, v)
		b, buf := growAndReturn(b, 3)
		buf[0] = codes.Uint16
		buf[1] = byte(v >> 8)
		buf[2] = byte(v)
		return b
	}

	if v <= math.MaxUint32 {
		// Equivalent to: return write4(b, codes.Uint32, v)
		b, buf := growAndReturn(b, 5)
		buf[0] = codes.Uint32
		buf[1] = byte(v >> 24)
		buf[2] = byte(v >> 16)
		buf[3] = byte(v >> 8)
		buf[4] = byte(v)
		return b
	}

	// Equivalent to: return write8(b, codes.Uint64, v)
	b, buf := growAndReturn(b, 9)
	buf[0] = codes.Uint64
	buf[1] = byte(v >> 56)
	buf[2] = byte(v >> 48)
	buf[3] = byte(v >> 40)
	buf[4] = byte(v >> 32)
	buf[5] = byte(v >> 24)
	buf[6] = byte(v >> 16)
	buf[7] = byte(v >> 8)
	buf[8] = byte(v)

	return b
}

// encodeVarInt64 is used in EncodeLogEntryFast which is a very hot path.
// As a result, many of the function calls in this function have been
// manually inlined to reduce function call overhead.
func encodeVarInt64(b []byte, v int64) []byte {
	if v >= 0 {
		// Equivalent to: return encodeVarUint64(b, uint64(v))
		if v <= math.MaxInt8 {
			b, buf := growAndReturn(b, 1)
			buf[0] = byte(v)
			return b
		}

		if v <= math.MaxUint8 {
			// Equivalent to: return write1(b, codes.Uint8, v)
			b, buf := growAndReturn(b, 2)
			buf[0] = codes.Uint8
			buf[1] = byte(v)
			return b
		}

		if v <= math.MaxUint16 {
			// Equivalent to: return write2(b, codes.Uint16, v)
			b, buf := growAndReturn(b, 3)
			buf[0] = codes.Uint16
			buf[1] = byte(v >> 8)
			buf[2] = byte(v)
			return b
		}

		if v <= math.MaxUint32 {
			// Equivalent to: return write4(b, codes.Uint32, v)
			b, buf := growAndReturn(b, 5)
			buf[0] = codes.Uint32
			buf[1] = byte(v >> 24)
			buf[2] = byte(v >> 16)
			buf[3] = byte(v >> 8)
			buf[4] = byte(v)
			return b
		}

		// Equivalent to: return write8(b, codes.Uint64, v)
		b, buf := growAndReturn(b, 9)
		buf[0] = codes.Uint64
		buf[1] = byte(v >> 56)
		buf[2] = byte(v >> 48)
		buf[3] = byte(v >> 40)
		buf[4] = byte(v >> 32)
		buf[5] = byte(v >> 24)
		buf[6] = byte(v >> 16)
		buf[7] = byte(v >> 8)
		buf[8] = byte(v)
		return b
	}

	if v >= int64(int8(codes.NegFixedNumLow)) {
		b, buff := growAndReturn(b, 1)
		buff[0] = byte(v)
		return b
	}

	if v >= math.MinInt8 {
		// Equivalent to: return write1(b, codes.Int8, uint64(v))
		b, buf := growAndReturn(b, 2)
		buf[0] = codes.Int8
		buf[1] = byte(uint64(v))
		return b
	}

	if v >= math.MinInt16 {
		// Equivalent to: return write2(b, codes.Int16, uint64(v))
		b, buf := growAndReturn(b, 3)
		n := uint64(v)
		buf[0] = codes.Int16
		buf[1] = byte(uint64(n) >> 8)
		buf[2] = byte(uint64(n))
		return b
	}

	if v >= math.MinInt32 {
		// Equivalent to: return write4(b, codes.Int32, uint64(v))
		b, buf := growAndReturn(b, 5)
		n := uint64(v)
		buf[0] = codes.Int32
		buf[1] = byte(n >> 24)
		buf[2] = byte(n >> 16)
		buf[3] = byte(n >> 8)
		buf[4] = byte(n)
		return b
	}

	// Equivalent to: return write8(b, codes.Int64, uint64(v))
	b, buf := growAndReturn(b, 9)
	n := uint64(v)
	buf[0] = codes.Int64
	buf[1] = byte(n >> 56)
	buf[2] = byte(n >> 48)
	buf[3] = byte(n >> 40)
	buf[4] = byte(n >> 32)
	buf[5] = byte(n >> 24)
	buf[6] = byte(n >> 16)
	buf[7] = byte(n >> 8)
	buf[8] = byte(n)
	return b
}

// encodeFloat64 is used in EncodeLogEntryFast which is a very hot path.
// As a result, many of the function calls in this function have been
// manually inlined to reduce function call overhead.
func encodeFloat64(b []byte, v float64) []byte {
	// Equivalent to: return write8(b, codes.Double, math.Float64bits(n))
	b, buf := growAndReturn(b, 9)
	buf[0] = codes.Double
	n := math.Float64bits(v)
	buf[1] = byte(n >> 56)
	buf[2] = byte(n >> 48)
	buf[3] = byte(n >> 40)
	buf[4] = byte(n >> 32)
	buf[5] = byte(n >> 24)
	buf[6] = byte(n >> 16)
	buf[7] = byte(n >> 8)
	buf[8] = byte(n)
	return b
}

// encodeBytes is used in EncodeLogEntryFast which is a very hot path.
// As a result, many of the function calls in this function have been
// manually inlined to reduce function call overhead.
func encodeBytes(b []byte, data []byte) []byte {
	if data == nil {
		b, buf := growAndReturn(b, 1)
		buf[0] = codes.Nil
		return b
	}

	// Equivalent to: encodeBytesLen(b, len(data))
	var (
		l   = len(data)
		v   = uint64(l)
		buf []byte
	)
	if l < 256 {
		b, buf = growAndReturn(b, 2)
		buf[0] = codes.Bin8
		buf[1] = byte(uint64(l))
	} else if l < 65536 {
		b, buf = growAndReturn(b, 3)
		buf[0] = codes.Bin16
		buf[1] = byte(v >> 8)
		buf[2] = byte(v)
		return b
	} else {
		b, buf = growAndReturn(b, 5)
		buf[0] = codes.Bin32
		buf[1] = byte(v >> 24)
		buf[2] = byte(v >> 16)
		buf[3] = byte(v >> 8)
		buf[4] = byte(v)
	}

	b = append(b, data...)
	return b
}

// encodeBytesLen is not used, it is left here to demonstrate what a manually inlined
// version of this function should look like.
func encodeBytesLen(b []byte, l int) []byte {
	if l < 256 {
		return write1(b, codes.Bin8, uint64(l))
	}
	if l < 65536 {
		return write2(b, codes.Bin16, uint64(l))
	}
	return write4(b, codes.Bin32, uint64(l))
}

// write1 is not used, it is left here to demonstrate what a manually inlined
// version of this function should look like.
func write1(b []byte, code byte, n uint64) []byte {
	b, buf := growAndReturn(b, 2)
	buf[0] = code
	buf[1] = byte(n)
	return b
}

// write2 is not used, it is left here to demonstrate what a manually inlined
// version of this function should look like.
func write2(b []byte, code byte, n uint64) []byte {
	b, buf := growAndReturn(b, 3)
	buf[0] = code
	buf[1] = byte(n >> 8)
	buf[2] = byte(n)
	return b
}

// write4 is not used, it is left here to demonstrate what a manually inlined
// version of this function should look like.
func write4(b []byte, code byte, n uint64) []byte {
	b, buf := growAndReturn(b, 5)
	buf[0] = code
	buf[1] = byte(n >> 24)
	buf[2] = byte(n >> 16)
	buf[3] = byte(n >> 8)
	buf[4] = byte(n)
	return b
}

// write8 is not used, it is left here to demonstrate what a manually inlined
// version of this function should look like.
func write8(b []byte, code byte, n uint64) []byte {
	b, buf := growAndReturn(b, 9)
	buf[0] = code
	buf[1] = byte(n >> 56)
	buf[2] = byte(n >> 48)
	buf[3] = byte(n >> 40)
	buf[4] = byte(n >> 32)
	buf[5] = byte(n >> 24)
	buf[6] = byte(n >> 16)
	buf[7] = byte(n >> 8)
	buf[8] = byte(n)
	return b
}

func growAndReturn(b []byte, n int) ([]byte, []byte) {
	if cap(b)-len(b) < n {
		newCapacity := 2 * (len(b) + n)
		newBuff := make([]byte, len(b), newCapacity)
		copy(newBuff, b)
		b = newBuff
	}
	ret := b[:len(b)+n]
	return ret, ret[len(b):]
}
