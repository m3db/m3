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

	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type encodeVersionFn func(value int)
type encodeNumObjectFieldsForFn func(value objectType)
type encodeVarintFn func(value int64)
type encodeVarUintFn func(value uint64)
type encodeFloat64Fn func(value float64)
type encodeBytesFn func(value []byte)
type encodeArrayLenFn func(value int)

type encoder struct {
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
}

// NewEncoder creates a new encoder
func NewEncoder() encoding.Encoder {
	buf := bytes.NewBuffer(nil)
	enc := &encoder{
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

	return enc
}

func (enc *encoder) Reset() {
	enc.buf.Truncate(0)
	enc.err = nil
}

func (enc *encoder) Bytes() []byte { return enc.buf.Bytes() }

func (enc *encoder) EncodeIndexInfo(info schema.IndexInfo) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexInfoVersion, indexInfoType)
	enc.encodeIndexInfo(info)
	return enc.err
}

func (enc *encoder) EncodeIndexEntry(entry schema.IndexEntry) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(indexEntryVersion, indexEntryType)
	enc.encodeIndexEntry(entry)
	return enc.err
}

func (enc *encoder) EncodeLogInfo(info schema.LogInfo) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logInfoVersion, logInfoType)
	enc.encodeLogInfo(info)
	return enc.err
}

func (enc *encoder) EncodeLogEntry(entry schema.LogEntry) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logEntryVersion, logEntryType)
	enc.encodeLogEntry(entry)
	return enc.err
}

func (enc *encoder) EncodeLogMetadata(entry schema.LogMetadata) error {
	if enc.err != nil {
		return enc.err
	}
	enc.encodeRootObject(logMetadataVersion, logMetadataType)
	enc.encodeLogMetadata(entry)
	return enc.err
}

func (enc *encoder) encodeIndexInfo(info schema.IndexInfo) {
	enc.encodeNumObjectFieldsForFn(indexInfoType)
	enc.encodeVarintFn(info.Start)
	enc.encodeVarintFn(info.BlockSize)
	enc.encodeVarintFn(info.Entries)
}

func (enc *encoder) encodeIndexEntry(entry schema.IndexEntry) {
	enc.encodeNumObjectFieldsForFn(indexEntryType)
	enc.encodeVarintFn(entry.Index)
	enc.encodeBytesFn(entry.ID)
	enc.encodeVarintFn(entry.Size)
	enc.encodeVarintFn(entry.Offset)
	enc.encodeVarintFn(entry.Checksum)
}

func (enc *encoder) encodeLogInfo(info schema.LogInfo) {
	enc.encodeNumObjectFieldsForFn(logInfoType)
	enc.encodeVarintFn(info.Start)
	enc.encodeVarintFn(info.Duration)
	enc.encodeVarintFn(info.Index)
}

func (enc *encoder) encodeLogEntry(entry schema.LogEntry) {
	enc.encodeNumObjectFieldsForFn(logEntryType)
	enc.encodeVarintFn(entry.Create)
	enc.encodeVarUintFn(entry.Index)
	enc.encodeBytesFn(entry.Metadata)
	enc.encodeVarintFn(entry.Timestamp)
	enc.encodeFloat64Fn(entry.Value)
	enc.encodeVarUintFn(uint64(entry.Unit))
	enc.encodeBytesFn(entry.Annotation)
}

func (enc *encoder) encodeLogMetadata(metadata schema.LogMetadata) {
	enc.encodeNumObjectFieldsForFn(logMetadataType)
	enc.encodeBytesFn(metadata.ID)
	enc.encodeBytesFn(metadata.Namespace)
	enc.encodeVarUintFn(uint64(metadata.Shard))
}

func (enc *encoder) encodeRootObject(version int, objType objectType) {
	enc.encodeVersionFn(version)
	enc.encodeNumObjectFieldsForFn(rootObjectType)
	enc.encodeObjectType(objType)
}

func (enc *encoder) encodeVersion(version int) {
	enc.encodeVarintFn(int64(version))
}

func (enc *encoder) encodeNumObjectFieldsFor(objType objectType) {
	enc.encodeArrayLenFn(numFieldsForType(objType))
}

func (enc *encoder) encodeObjectType(objType objectType) {
	enc.encodeVarintFn(int64(objType))
}

func (enc *encoder) encodeVarint(value int64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeInt64(value)
}

func (enc *encoder) encodeVarUint(value uint64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeUint64(value)
}

func (enc *encoder) encodeFloat64(value float64) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeFloat64(value)
}

func (enc *encoder) encodeBytes(value []byte) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeBytes(value)
}

func (enc *encoder) encodeArrayLen(value int) {
	if enc.err != nil {
		return
	}
	enc.err = enc.enc.EncodeArrayLen(value)
}
