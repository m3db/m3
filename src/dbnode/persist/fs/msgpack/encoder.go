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

	"github.com/m3db/m3/src/dbnode/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type encodeVersionFn func(value int)
type encodeNumObjectFieldsForFn func(value objectType)
type encodeVarintFn func(value int64)
type encodeVarUintFn func(value uint64)
type encodeFloat64Fn func(value float64)
type encodeBytesFn func(value []byte)
type encodeArrayLenFn func(value int)

// Encoder encodes data in msgpack format for persistence.
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

type legacyEncodingIndexInfoVersion int

const (
	legacyEncodingIndexVersionCurrent                                = legacyEncodingIndexVersionV3
	legacyEncodingIndexVersionV1      legacyEncodingIndexInfoVersion = iota
	legacyEncodingIndexVersionV2
	legacyEncodingIndexVersionV3
)

type legacyEncodingOptions struct {
	encodeLegacyIndexInfoVersion legacyEncodingIndexInfoVersion
	decodeLegacyIndexInfoVersion legacyEncodingIndexInfoVersion

	encodeLegacyV1IndexEntry bool
	decodeLegacyV1IndexEntry bool
}

var defaultlegacyEncodingOptions = legacyEncodingOptions{
	encodeLegacyIndexInfoVersion: legacyEncodingIndexVersionCurrent,
	decodeLegacyIndexInfoVersion: legacyEncodingIndexVersionCurrent,

	encodeLegacyV1IndexEntry: false,
	decodeLegacyV1IndexEntry: false,
}

// NewEncoder creates a new encoder.
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

	// Used primarily for testing.
	enc.legacy = legacy

	return enc
}

// Reset resets the buffer.
func (enc *Encoder) Reset() {
	enc.buf.Truncate(0)
	enc.err = nil
}

// Bytes returns the encoded bytes.
func (enc *Encoder) Bytes() []byte { return enc.buf.Bytes() }

// EncodeIndexInfo encodes index info.
func (enc *Encoder) EncodeIndexInfo(info schema.IndexInfo) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexInfoVersion, indexInfoType)
	if enc.legacy.encodeLegacyIndexInfoVersion == legacyEncodingIndexVersionV1 {
		enc.encodeIndexInfoV1(info)
	} else if enc.legacy.encodeLegacyIndexInfoVersion == legacyEncodingIndexVersionV2 {
		enc.encodeIndexInfoV2(info)
	} else {
		enc.encodeIndexInfoV3(info)
	}
	return enc.err
}

// EncodeIndexEntry encodes index entry.
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

// EncodeIndexSummary encodes index summary.
func (enc *Encoder) EncodeIndexSummary(summary schema.IndexSummary) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexSummaryVersion, indexSummaryType)
	enc.encodeIndexSummary(summary)
	return enc.err
}

// EncodeLogInfo encodes commit log info.
func (enc *Encoder) EncodeLogInfo(info schema.LogInfo) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logInfoVersion, logInfoType)
	enc.encodeLogInfo(info)
	return enc.err
}

// EncodeLogEntry encodes commit log entry.
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
// backwards-compatbility.
func (enc *Encoder) encodeIndexInfoV1(info schema.IndexInfo) {
	// Manually encode num fields for testing purposes.
	enc.encodeArrayLenFn(6) // V1 had 6 fields.
	enc.encodeVarintFn(info.BlockStart)
	enc.encodeVarintFn(info.BlockSize)
	enc.encodeVarintFn(info.Entries)
	enc.encodeVarintFn(info.MajorVersion)
	enc.encodeIndexSummariesInfo(info.Summaries)
	enc.encodeIndexBloomFilterInfo(info.BloomFilter)
}

// We only keep this method around for the sake of testing
// backwards-compatbility.
func (enc *Encoder) encodeIndexInfoV2(info schema.IndexInfo) {
	// Manually encode num fields for testing purposes.
	enc.encodeNumObjectFieldsForFn(8) // V2 had 8 fields.
	enc.encodeVarintFn(info.BlockStart)
	enc.encodeVarintFn(info.BlockSize)
	enc.encodeVarintFn(info.Entries)
	enc.encodeVarintFn(info.MajorVersion)
	enc.encodeIndexSummariesInfo(info.Summaries)
	enc.encodeIndexBloomFilterInfo(info.BloomFilter)
	enc.encodeVarintFn(info.SnapshotTime)
	enc.encodeVarintFn(int64(info.FileType))
}

func (enc *Encoder) encodeIndexInfoV3(info schema.IndexInfo) {
	enc.encodeNumObjectFieldsForFn(indexInfoType)
	enc.encodeVarintFn(info.BlockStart)
	enc.encodeVarintFn(info.BlockSize)
	enc.encodeVarintFn(info.Entries)
	enc.encodeVarintFn(info.MajorVersion)
	enc.encodeIndexSummariesInfo(info.Summaries)
	enc.encodeIndexBloomFilterInfo(info.BloomFilter)
	enc.encodeVarintFn(info.SnapshotTime)
	enc.encodeVarintFn(int64(info.FileType))
	enc.encodeBytesFn(info.SnapshotID)
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
// backwards-compatbility.
func (enc *Encoder) encodeIndexEntryV1(entry schema.IndexEntry) {
	// Manually encode num fields for testing purposes.
	enc.encodeArrayLenFn(5) // V1 had 5 fields.
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
