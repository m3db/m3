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
	"fmt"
	"io"

	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyIndexInfo   schema.IndexInfo
	emptyIndexEntry  schema.IndexEntry
	emptyLogInfo     schema.LogInfo
	emptyLogEntry    schema.LogEntry
	emptyLogMetadata schema.LogMetadata
)

type decoder struct {
	allocDecodedBytes bool
	data              []byte
	reader            *bytes.Reader
	dec               *msgpack.Decoder
	err               error
}

// NewDecoder creates a new decoder
func NewDecoder(opts DecodingOptions) encoding.Decoder {
	if opts == nil {
		opts = NewDecodingOptions()
	}
	reader := bytes.NewReader(nil)
	return &decoder{
		allocDecodedBytes: opts.AllocDecodedBytes(),
		reader:            reader,
		dec:               msgpack.NewDecoder(reader),
	}
}

func (dec *decoder) Reset(data []byte) {
	dec.data = data
	dec.reader.Reset(data)
	dec.dec.Reset(dec.reader)
	dec.err = nil
}

func (dec *decoder) DecodeIndexInfo() (schema.IndexInfo, error) {
	if dec.err != nil {
		return emptyIndexInfo, dec.err
	}
	numFieldsToSkip := dec.decodeRootObject(indexInfoVersion, indexInfoType)
	indexInfo := dec.decodeIndexInfo()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexInfo, dec.err
	}
	return indexInfo, nil
}

func (dec *decoder) DecodeIndexEntry() (schema.IndexEntry, error) {
	if dec.err != nil {
		return emptyIndexEntry, dec.err
	}
	numFieldsToSkip := dec.decodeRootObject(indexEntryVersion, indexEntryType)
	indexEntry := dec.decodeIndexEntry()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexEntry, dec.err
	}
	return indexEntry, nil
}

func (dec *decoder) DecodeLogInfo() (schema.LogInfo, error) {
	if dec.err != nil {
		return emptyLogInfo, dec.err
	}
	numFieldsToSkip := dec.decodeRootObject(logInfoVersion, logInfoType)
	logInfo := dec.decodeLogInfo()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogInfo, dec.err
	}
	return logInfo, nil
}

func (dec *decoder) DecodeLogEntry() (schema.LogEntry, error) {
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}
	numFieldsToSkip := dec.decodeRootObject(logEntryVersion, logEntryType)
	logEntry := dec.decodeLogEntry()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}
	return logEntry, nil
}

func (dec *decoder) DecodeLogMetadata() (schema.LogMetadata, error) {
	if dec.err != nil {
		return emptyLogMetadata, dec.err
	}
	numFieldsToSkip := dec.decodeRootObject(logMetadataVersion, logMetadataType)
	logMetadata := dec.decodeLogMetadata()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogMetadata, dec.err
	}
	return logMetadata, nil
}

func (dec *decoder) decodeIndexInfo() schema.IndexInfo {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexInfoType)
	if !ok {
		return emptyIndexInfo
	}
	var indexInfo schema.IndexInfo
	indexInfo.Start = dec.decodeVarint()
	indexInfo.BlockSize = dec.decodeVarint()
	indexInfo.Entries = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexInfo
	}
	return indexInfo
}

func (dec *decoder) decodeIndexEntry() schema.IndexEntry {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexEntryType)
	if !ok {
		return emptyIndexEntry
	}
	var indexEntry schema.IndexEntry
	indexEntry.Index = dec.decodeVarint()
	indexEntry.ID = dec.decodeBytes()
	indexEntry.Size = dec.decodeVarint()
	indexEntry.Offset = dec.decodeVarint()
	indexEntry.Checksum = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexEntry
	}
	return indexEntry
}

func (dec *decoder) decodeLogInfo() schema.LogInfo {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(logInfoType)
	if !ok {
		return emptyLogInfo
	}
	var logInfo schema.LogInfo
	logInfo.Start = dec.decodeVarint()
	logInfo.Duration = dec.decodeVarint()
	logInfo.Index = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogInfo
	}
	return logInfo
}

func (dec *decoder) decodeLogEntry() schema.LogEntry {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(logEntryType)
	if !ok {
		return emptyLogEntry
	}
	var logEntry schema.LogEntry
	logEntry.Create = dec.decodeVarint()
	logEntry.Index = dec.decodeVarUint()
	logEntry.Metadata = dec.decodeBytes()
	logEntry.Timestamp = dec.decodeVarint()
	logEntry.Value = dec.decodeFloat64()
	logEntry.Unit = uint32(dec.decodeVarUint())
	logEntry.Annotation = dec.decodeBytes()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogEntry
	}
	return logEntry
}

func (dec *decoder) decodeLogMetadata() schema.LogMetadata {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(logMetadataType)
	if !ok {
		return emptyLogMetadata
	}
	var logMetadata schema.LogMetadata
	logMetadata.ID = dec.decodeBytes()
	logMetadata.Namespace = dec.decodeBytes()
	logMetadata.Shard = uint32(dec.decodeVarUint())
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogMetadata
	}
	return logMetadata
}

func (dec *decoder) decodeRootObject(expectedVersion int, expectedType objectType) int {
	dec.checkVersion(expectedVersion)
	if dec.err != nil {
		return 0
	}
	numFieldsToSkip, ok := dec.checkNumFieldsFor(rootObjectType)
	if !ok {
		return 0
	}
	actualType := dec.decodeObjectType()
	if dec.err != nil {
		return 0
	}
	if expectedType != actualType {
		dec.err = fmt.Errorf("object type mismatch: expected %v actual %v", expectedType, actualType)
		return 0
	}
	return numFieldsToSkip
}

func (dec *decoder) checkVersion(expected int) {
	version := int(dec.decodeVarint())
	if dec.err != nil {
		return
	}
	if version > expected {
		dec.err = fmt.Errorf("version mismatch: expected %v actual %v", expected, version)
	}
}

func (dec *decoder) checkNumFieldsFor(objType objectType) (int, bool) {
	actual := dec.decodeNumObjectFields()
	if dec.err != nil {
		return 0, false
	}
	expected := numFieldsForType(objType)
	if expected > actual {
		dec.err = fmt.Errorf("number of fields mismatch: expected %d actual %d", expected, actual)
		return 0, false
	}
	return actual - expected, true
}

func (dec *decoder) skip(numFields int) {
	if dec.err != nil {
		return
	}
	if numFields < 0 {
		dec.err = fmt.Errorf("number of fields to skip is %d", numFields)
		return
	}
	for i := 0; i < numFields; i++ {
		if err := dec.dec.Skip(); err != nil {
			dec.err = err
			return
		}
	}
}

func (dec *decoder) decodeNumObjectFields() int {
	return int(dec.decodeArrayLen())
}

func (dec *decoder) decodeObjectType() objectType {
	return objectType(dec.decodeVarint())
}

func (dec *decoder) decodeVarint() int64 {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeInt64()
	dec.err = err
	return value
}

func (dec *decoder) decodeVarUint() uint64 {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeUint64()
	dec.err = err
	return value
}

func (dec *decoder) decodeFloat64() float64 {
	if dec.err != nil {
		return 0.0
	}
	value, err := dec.dec.DecodeFloat64()
	dec.err = err
	return value
}

func (dec *decoder) decodeBytes() []byte {
	if dec.err != nil {
		return nil
	}
	// If we need to allocate new space for decoded byte slice, we delegate it to msgpack
	// API which allocates a new slice under the hood, otherwise we simply locate the byte
	// slice as part of the encoded byte stream and return it
	var value []byte
	if dec.allocDecodedBytes {
		value, dec.err = dec.dec.DecodeBytes()
	} else {
		bytesLen := dec.decodeBytesLen()
		if dec.err != nil {
			return nil
		}
		// NB(xichen): DecodeBytesLen() returns -1 if the byte slice is nil
		if bytesLen == -1 {
			return nil
		}
		var (
			numBytes  = len(dec.data)
			currPos   = numBytes - dec.reader.Len()
			targetPos = currPos + bytesLen
		)
		if bytesLen < 0 || currPos < 0 || targetPos > numBytes {
			dec.err = fmt.Errorf("invalid currPos %d, bytesLen %d, numBytes %d", currPos, bytesLen, numBytes)
			return nil
		}
		_, err := dec.reader.Seek(int64(targetPos), io.SeekStart)
		if err != nil {
			dec.err = err
			return nil
		}
		value = dec.data[currPos:targetPos]
	}
	return value
}

func (dec *decoder) decodeArrayLen() int {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeArrayLen()
	dec.err = err
	return value
}

func (dec *decoder) decodeBytesLen() int {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeBytesLen()
	dec.err = err
	return value
}
