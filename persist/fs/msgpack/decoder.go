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
	"errors"
	"fmt"

	"github.com/m3db/m3db/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyIndexInfo            schema.IndexInfo
	emptyIndexSummariesInfo   schema.IndexSummariesInfo
	emptyIndexBloomFilterInfo schema.IndexBloomFilterInfo
	emptyIndexEntry           schema.IndexEntry
	emptyIndexSummary         schema.IndexSummary
	emptyIndexSummaryToken    IndexSummaryToken
	emptyLogInfo              schema.LogInfo
	emptyLogEntry             schema.LogEntry
	emptyLogMetadata          schema.LogMetadata
)

// Decoder decodes persisted msgpack-encoded data
type Decoder struct {
	allocDecodedBytes bool
	reader            DecoderStream
	dec               *msgpack.Decoder
	err               error
}

// NewDecoder creates a new decoder
func NewDecoder(opts DecodingOptions) *Decoder {
	if opts == nil {
		opts = NewDecodingOptions()
	}
	reader := NewDecoderStream(nil)
	return &Decoder{
		allocDecodedBytes: opts.AllocDecodedBytes(),
		reader:            reader,
		dec:               msgpack.NewDecoder(reader),
	}
}

// Reset resets the data stream to decode from
func (dec *Decoder) Reset(stream DecoderStream) {
	dec.reader = stream
	dec.dec.Reset(dec.reader)
	dec.err = nil
}

// DecodeIndexInfo decodes the index info
func (dec *Decoder) DecodeIndexInfo() (schema.IndexInfo, error) {
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

// DecodeIndexEntry decodes index entry
func (dec *Decoder) DecodeIndexEntry() (schema.IndexEntry, error) {
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

// DecodeIndexSummary decodes index summary
func (dec *Decoder) DecodeIndexSummary() (
	schema.IndexSummary, IndexSummaryToken, error) {
	if dec.err != nil {
		return emptyIndexSummary, emptyIndexSummaryToken, dec.err
	}
	numFieldsToSkip := dec.decodeRootObject(indexSummaryVersion, indexSummaryType)
	indexSummary, indexSummaryMetadata := dec.decodeIndexSummary()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexSummary, emptyIndexSummaryToken, dec.err
	}
	return indexSummary, indexSummaryMetadata, nil
}

// DecodeLogInfo decodes commit log info
func (dec *Decoder) DecodeLogInfo() (schema.LogInfo, error) {
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

// DecodeLogEntry decodes commit log entry
func (dec *Decoder) DecodeLogEntry() (schema.LogEntry, error) {
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

// DecodeLogEntryUniqueIndex decodes a log entry as much as is required to return
// the series unique index. Call DecodeLogEntryRemaining afterwards to decode the
// remaining fields.
func (dec *Decoder) DecodeLogEntryUniqueIndex() (int64, uint64, error) {
	if dec.err != nil {
		return 0, 0, dec.err
	}

	dec.decodeRootObject(logEntryVersion, logEntryType)
	numFieldsToSkip, ok := dec.checkNumFieldsFor(logEntryType)
	if !ok {
		return 0, 0, errors.New("wtf")
	}
	create := dec.decodeVarint()
	idx := dec.decodeVarUint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return 0, 0, dec.err
	}

	return create, idx, nil
}

// DecodeLogEntryRemaining can only be called after DecodeLogEntryUniqueIndex,
// and it returns a complete schema.LogEntry.
func (dec *Decoder) DecodeLogEntryRemaining() (schema.LogEntry, error) {
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}
	var logEntry schema.LogEntry
	logEntry.Metadata, _, _ = dec.decodeBytes()
	logEntry.Timestamp = dec.decodeVarint()
	logEntry.Value = dec.decodeFloat64()
	logEntry.Unit = uint32(dec.decodeVarUint())
	logEntry.Annotation, _, _ = dec.decodeBytes()
	return logEntry, nil
}

// DecodeLogMetadata decodes commit log metadata
func (dec *Decoder) DecodeLogMetadata() (schema.LogMetadata, error) {
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

func (dec *Decoder) decodeIndexInfo() schema.IndexInfo {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexInfoType)
	if !ok {
		return emptyIndexInfo
	}
	var indexInfo schema.IndexInfo
	indexInfo.Start = dec.decodeVarint()
	indexInfo.BlockSize = dec.decodeVarint()
	indexInfo.Entries = dec.decodeVarint()
	indexInfo.MajorVersion = dec.decodeVarint()
	indexInfo.Summaries = dec.decodeIndexSummariesInfo()
	indexInfo.BloomFilter = dec.decodeIndexBloomFilterInfo()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexInfo
	}
	return indexInfo
}

func (dec *Decoder) decodeIndexSummariesInfo() schema.IndexSummariesInfo {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexSummariesInfoType)
	if !ok {
		return emptyIndexSummariesInfo
	}
	var indexSummariesInfo schema.IndexSummariesInfo
	indexSummariesInfo.Summaries = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexSummariesInfo
	}
	return indexSummariesInfo
}

func (dec *Decoder) decodeIndexBloomFilterInfo() schema.IndexBloomFilterInfo {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexBloomFilterInfoType)
	if !ok {
		return emptyIndexBloomFilterInfo
	}
	var indexBloomFilterInfo schema.IndexBloomFilterInfo
	indexBloomFilterInfo.NumElementsM = dec.decodeVarint()
	indexBloomFilterInfo.NumHashesK = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexBloomFilterInfo
	}
	return indexBloomFilterInfo
}

func (dec *Decoder) decodeIndexEntry() schema.IndexEntry {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexEntryType)
	if !ok {
		return emptyIndexEntry
	}
	var indexEntry schema.IndexEntry
	indexEntry.Index = dec.decodeVarint()
	indexEntry.ID, _, _ = dec.decodeBytes()
	indexEntry.Size = dec.decodeVarint()
	indexEntry.Offset = dec.decodeVarint()
	indexEntry.Checksum = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexEntry
	}
	return indexEntry
}

func (dec *Decoder) decodeIndexSummary() (schema.IndexSummary, IndexSummaryToken) {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(indexSummaryType)
	if !ok {
		return emptyIndexSummary, emptyIndexSummaryToken
	}
	var (
		indexSummary       schema.IndexSummary
		idBytesStartOffset int
		idBytesLength      int
	)
	indexSummary.Index = dec.decodeVarint()
	// Keep track of the offset in the byte stream before we decode the bytes so
	// that we know exactly where to jump to if we want to just grab the ID itself
	indexSummary.ID, idBytesStartOffset, idBytesLength = dec.decodeBytes()
	indexSummary.IndexEntryOffset = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexSummary, emptyIndexSummaryToken
	}

	// Downscaling to uint32 is fine because summary files and ID length should
	// be well below the max value of a uint32
	indexSummaryToken := NewIndexSummaryToken(
		uint32(idBytesStartOffset), uint32(idBytesLength),
	)
	return indexSummary, indexSummaryToken
}

func (dec *Decoder) decodeLogInfo() schema.LogInfo {
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

func (dec *Decoder) decodeLogEntry() schema.LogEntry {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(logEntryType)
	if !ok {
		return emptyLogEntry
	}
	var logEntry schema.LogEntry
	logEntry.Create = dec.decodeVarint()
	logEntry.Index = dec.decodeVarUint()
	logEntry.Metadata, _, _ = dec.decodeBytes()
	logEntry.Timestamp = dec.decodeVarint()
	logEntry.Value = dec.decodeFloat64()
	logEntry.Unit = uint32(dec.decodeVarUint())
	logEntry.Annotation, _, _ = dec.decodeBytes()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogEntry
	}
	return logEntry
}

func (dec *Decoder) decodeLogMetadata() schema.LogMetadata {
	numFieldsToSkip, ok := dec.checkNumFieldsFor(logMetadataType)
	if !ok {
		return emptyLogMetadata
	}
	var logMetadata schema.LogMetadata
	logMetadata.ID, _, _ = dec.decodeBytes()
	logMetadata.Namespace, _, _ = dec.decodeBytes()
	logMetadata.Shard = uint32(dec.decodeVarUint())
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogMetadata
	}
	return logMetadata
}

func (dec *Decoder) decodeRootObject(expectedVersion int, expectedType objectType) int {
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

func (dec *Decoder) checkVersion(expected int) {
	version := int(dec.decodeVarint())
	if dec.err != nil {
		return
	}
	if version > expected {
		dec.err = fmt.Errorf("version mismatch: expected %v actual %v", expected, version)
	}
}

func (dec *Decoder) checkNumFieldsFor(objType objectType) (int, bool) {
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

func (dec *Decoder) skip(numFields int) {
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

func (dec *Decoder) decodeNumObjectFields() int {
	return dec.decodeArrayLen()
}

func (dec *Decoder) decodeObjectType() objectType {
	return objectType(dec.decodeVarint())
}

func (dec *Decoder) decodeVarint() int64 {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeInt64()
	dec.err = err
	return value
}

func (dec *Decoder) decodeVarUint() uint64 {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeUint64()
	dec.err = err
	return value
}

func (dec *Decoder) decodeFloat64() float64 {
	if dec.err != nil {
		return 0.0
	}
	value, err := dec.dec.DecodeFloat64()
	dec.err = err
	return value
}

func (dec *Decoder) decodeBytes() ([]byte, int, int) {
	if dec.err != nil {
		return nil, -1, -1
	}
	// If we need to allocate new space for decoded byte slice, we delegate it to msgpack
	// API which allocates a new slice under the hood, otherwise we simply locate the byte
	// slice as part of the encoded byte stream and return it
	var value []byte
	if dec.allocDecodedBytes {
		value, dec.err = dec.dec.DecodeBytes()
		return value, -1, -1
	}

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

func (dec *Decoder) decodeArrayLen() int {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeArrayLen()
	dec.err = err
	return value
}

func (dec *Decoder) decodeBytesLen() int {
	if dec.err != nil {
		return 0
	}
	value, err := dec.dec.DecodeBytesLen()
	dec.err = err
	return value
}
