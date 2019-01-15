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
	"io"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/schema"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyIndexInfo              schema.IndexInfo
	emptyIndexSummariesInfo     schema.IndexSummariesInfo
	emptyIndexBloomFilterInfo   schema.IndexBloomFilterInfo
	emptyIndexEntry             schema.IndexEntry
	emptyIndexSummary           schema.IndexSummary
	emptyIndexSummaryToken      IndexSummaryToken
	emptyLogInfo                schema.LogInfo
	emptyLogEntry               schema.LogEntry
	emptyLogMetadata            schema.LogMetadata
	emptyLogEntryRemainingToken DecodeLogEntryRemainingToken
)

var errorUnableToDetermineNumFieldsToSkip = errors.New("unable to determine num fields to skip")

// Decoder decodes persisted msgpack-encoded data
type Decoder struct {
	reader            DecoderStreamDecoder
	dec               *msgpack.Decoder
	err               error
	allocDecodedBytes bool

	legacy legacyEncodingOptions
}

type DecoderStreamDecoder interface {
	io.Reader

	// ReadByte reads the next byte.
	ReadByte() (byte, error)

	// UnreadByte unreads the last read byte or returns error if none read
	// yet. Only a single byte can be unread at a time, a consecutive call
	// to UnreadByte will result in an error.
	UnreadByte() error

	// Skip progresses the reader by a certain amount of bytes, useful
	// when taking a ref to some of the bytes and progressing the reader
	// itself.
	Skip(length int64) error
}

// NewDecoder creates a new decoder
func NewDecoder(opts DecodingOptions) *Decoder {
	return newDecoder(defaultlegacyEncodingOptions, opts)
}

func newDecoder(legacy legacyEncodingOptions, opts DecodingOptions) *Decoder {
	if opts == nil {
		opts = NewDecodingOptions()
	}
	reader := NewDecoderStream(nil)
	return &Decoder{
		allocDecodedBytes: opts.AllocDecodedBytes(),
		reader:            reader,
		dec:               msgpack.NewDecoder(reader),
		legacy:            legacy,
	}
}

// Reset resets the data stream to decode from
func (dec *Decoder) Reset(stream DecoderStreamDecoder) {
	dec.reader = stream
	dec.dec.Reset(dec.reader)
	dec.err = nil
}

// DecodeIndexInfo decodes the index info
func (dec *Decoder) DecodeIndexInfo() (schema.IndexInfo, error) {
	if dec.err != nil {
		return emptyIndexInfo, dec.err
	}

	_, numFieldsToSkip := dec.decodeRootObject(indexInfoVersion, indexInfoType)
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
		fmt.Println("1: ", dec.err)
		return emptyIndexEntry, dec.err
	}
	_, numFieldsToSkip := dec.decodeRootObject(indexEntryVersion, indexEntryType)
	indexEntry := dec.decodeIndexEntry()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		fmt.Println("2: ", dec.err)
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
	_, numFieldsToSkip := dec.decodeRootObject(indexSummaryVersion, indexSummaryType)
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
	_, numFieldsToSkip := dec.decodeRootObject(logInfoVersion, logInfoType)
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
	_, numFieldsToSkip := dec.decodeRootObject(logEntryVersion, logEntryType)
	logEntry := dec.decodeLogEntry()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}
	return logEntry, nil
}

// DecodeLogEntryRemainingToken contains all the information that DecodeLogEntryRemaining
// requires to continue decoding a log entry after a call to DecodeLogEntryUniqueIndex.
type DecodeLogEntryRemainingToken struct {
	numFieldsToSkip1 int
	numFieldsToSkip2 int
}

// DecodeLogEntryUniqueIndex decodes a log entry as much as is required to return
// the series unique index. Call DecodeLogEntryRemaining afterwards to decode the
// remaining fields.
func (dec *Decoder) DecodeLogEntryUniqueIndex() (DecodeLogEntryRemainingToken, uint64, error) {
	if dec.err != nil {
		return emptyLogEntryRemainingToken, 0, dec.err
	}

	_, numFieldsToSkip1 := dec.decodeRootObject(logEntryVersion, logEntryType)
	numFieldsToSkip2, _, ok := dec.checkNumFieldsFor(logEntryType, checkNumFieldsOptions{})
	if !ok {
		return emptyLogEntryRemainingToken, 0, errorUnableToDetermineNumFieldsToSkip
	}
	idx := dec.decodeVarUint()

	token := DecodeLogEntryRemainingToken{
		numFieldsToSkip1: numFieldsToSkip1,
		numFieldsToSkip2: numFieldsToSkip2,
	}
	return token, idx, nil
}

// DecodeLogEntryRemaining can only be called after DecodeLogEntryUniqueIndex,
// and it returns a complete schema.LogEntry.
func (dec *Decoder) DecodeLogEntryRemaining(token DecodeLogEntryRemainingToken, index uint64) (schema.LogEntry, error) {
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}

	var logEntry schema.LogEntry
	logEntry.Index = index
	logEntry.Create = dec.decodeVarint()
	logEntry.Metadata, _, _ = dec.decodeBytes()
	logEntry.Timestamp = dec.decodeVarint()
	logEntry.Value = dec.decodeFloat64()
	logEntry.Unit = uint32(dec.decodeVarUint())
	logEntry.Annotation, _, _ = dec.decodeBytes()

	dec.skip(token.numFieldsToSkip1)
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}
	dec.skip(token.numFieldsToSkip2)
	if dec.err != nil {
		return emptyLogEntry, dec.err
	}

	return logEntry, nil
}

// DecodeLogMetadata decodes commit log metadata
func (dec *Decoder) DecodeLogMetadata() (schema.LogMetadata, error) {
	if dec.err != nil {
		return emptyLogMetadata, dec.err
	}
	_, numFieldsToSkip := dec.decodeRootObject(logMetadataVersion, logMetadataType)
	logMetadata := dec.decodeLogMetadata()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogMetadata, dec.err
	}
	return logMetadata, nil
}

func (dec *Decoder) decodeIndexInfo() schema.IndexInfo {
	var opts checkNumFieldsOptions

	if dec.legacy.decodeLegacyIndexInfoVersion == legacyEncodingIndexVersionV1 {
		// V1 had 6 fields.
		opts.override = true
		opts.numExpectedMinFields = 6
		opts.numExpectedCurrFields = 6
	} else if dec.legacy.decodeLegacyIndexInfoVersion == legacyEncodingIndexVersionV2 {
		// V2 had 8 fields.
		opts.override = true
		opts.numExpectedMinFields = 8
		opts.numExpectedCurrFields = 8
	}

	numFieldsToSkip, actual, ok := dec.checkNumFieldsFor(indexInfoType, opts)
	if !ok {
		return emptyIndexInfo
	}

	var indexInfo schema.IndexInfo
	indexInfo.BlockStart = dec.decodeVarint()
	indexInfo.BlockSize = dec.decodeVarint()
	indexInfo.Entries = dec.decodeVarint()
	indexInfo.MajorVersion = dec.decodeVarint()
	indexInfo.Summaries = dec.decodeIndexSummariesInfo()
	indexInfo.BloomFilter = dec.decodeIndexBloomFilterInfo()

	// At this point if its a V1 file we've decoded all the available fields.
	if dec.legacy.decodeLegacyIndexInfoVersion == legacyEncodingIndexVersionV1 || actual < 8 {
		dec.skip(numFieldsToSkip)
		return indexInfo
	}

	// Decode fields added in V2.
	indexInfo.SnapshotTime = dec.decodeVarint()
	indexInfo.FileType = persist.FileSetType(dec.decodeVarint())

	// At this point if its a V2 file we've decoded all the available fields.
	if dec.legacy.decodeLegacyIndexInfoVersion == legacyEncodingIndexVersionV2 || actual < 9 {
		dec.skip(numFieldsToSkip)
		return indexInfo
	}

	// Decode fields added in V3.
	indexInfo.SnapshotID, _, _ = dec.decodeBytes()

	dec.skip(numFieldsToSkip)
	return indexInfo
}

func (dec *Decoder) decodeIndexSummariesInfo() schema.IndexSummariesInfo {
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(indexSummariesInfoType, checkNumFieldsOptions{})
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
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(indexBloomFilterInfoType, checkNumFieldsOptions{})
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
	var opts checkNumFieldsOptions
	if dec.legacy.decodeLegacyV1IndexEntry {
		// V1 had 5 fields.
		opts.override = true
		opts.numExpectedMinFields = 5
		opts.numExpectedCurrFields = 5
	}
	numFieldsToSkip, actual, ok := dec.checkNumFieldsFor(indexEntryType, opts)
	if !ok {
		return emptyIndexEntry
	}

	var indexEntry schema.IndexEntry
	indexEntry.Index = dec.decodeVarint()
	indexEntry.ID, _, _ = dec.decodeBytes()
	indexEntry.Size = dec.decodeVarint()
	indexEntry.Offset = dec.decodeVarint()
	indexEntry.Checksum = dec.decodeVarint()

	if dec.legacy.decodeLegacyV1IndexEntry || actual < 6 {
		dec.skip(numFieldsToSkip)
		return indexEntry
	}

	indexEntry.EncodedTags, _, _ = dec.decodeBytes()

	dec.skip(numFieldsToSkip)
	return indexEntry
}

func (dec *Decoder) decodeIndexSummary() (schema.IndexSummary, IndexSummaryToken) {
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(indexSummaryType, checkNumFieldsOptions{})
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
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(logInfoType, checkNumFieldsOptions{})
	if !ok {
		return emptyLogInfo
	}
	var logInfo schema.LogInfo

	// Deprecated, have to decode anyways for backwards compatibility, but we ignore the values.
	logInfo.DeprecatedDoNotUseStart = dec.decodeVarint()
	logInfo.DeprecatedDoNotUseDuration = dec.decodeVarint()

	logInfo.Index = dec.decodeVarint()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogInfo
	}
	return logInfo
}

func (dec *Decoder) decodeLogEntry() schema.LogEntry {
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(logEntryType, checkNumFieldsOptions{})
	if !ok {
		return emptyLogEntry
	}
	var logEntry schema.LogEntry
	logEntry.Index = dec.decodeVarUint()
	logEntry.Create = dec.decodeVarint()
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
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(logMetadataType, checkNumFieldsOptions{})
	if !ok {
		return emptyLogMetadata
	}
	var logMetadata schema.LogMetadata
	logMetadata.ID, _, _ = dec.decodeBytes()
	logMetadata.Namespace, _, _ = dec.decodeBytes()
	logMetadata.Shard = uint32(dec.decodeVarUint())
	logMetadata.EncodedTags, _, _ = dec.decodeBytes()
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyLogMetadata
	}
	return logMetadata
}

func (dec *Decoder) decodeRootObject(expectedVersion int, expectedType objectType) (version int, numFieldsToSkip int) {
	version = dec.checkVersion(expectedVersion)
	if dec.err != nil {
		return 0, 0
	}
	numFieldsToSkip, _, ok := dec.checkNumFieldsFor(rootObjectType, checkNumFieldsOptions{})
	if !ok {
		return 0, 0
	}
	actualType := dec.decodeObjectType()
	if dec.err != nil {
		return 0, 0
	}
	if expectedType != actualType {
		dec.err = fmt.Errorf("object type mismatch: expected %v actual %v", expectedType, actualType)
		return 0, 0
	}
	return version, numFieldsToSkip
}

func (dec *Decoder) checkVersion(expected int) int {
	version := int(dec.decodeVarint())
	if dec.err != nil {
		return 0
	}
	if version > expected {
		dec.err = fmt.Errorf("version mismatch: expected %v actual %v", expected, version)
		return 0
	}

	return version
}

type checkNumFieldsOptions struct {
	override              bool
	numExpectedMinFields  int
	numExpectedCurrFields int
}

func (dec *Decoder) checkNumFieldsFor(
	objType objectType,
	opts checkNumFieldsOptions,
) (numToSkip int, actual int, ok bool) {
	actual = dec.decodeNumObjectFields()
	if dec.err != nil {
		return 0, 0, false
	}
	min, curr := numFieldsForType(objType)
	if opts.override {
		min = opts.numExpectedMinFields
		curr = opts.numExpectedCurrFields
	}
	if min > actual {
		dec.err = fmt.Errorf("number of fields mismatch: expected minimum of %d actual %d", min, actual)
		return 0, 0, false
	}

	numToSkip = actual - curr
	if numToSkip < 0 {
		numToSkip = 0
	}
	return numToSkip, actual, true
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
	var byteProvider interface {
		Bytes() []byte
		Remaining() int64
	}
	var ok bool
	byteProvider, ok = dec.reader.(interface {
		Bytes() []byte
		Remaining() int64
	})

	if dec.allocDecodedBytes || !ok {
		value, dec.err = dec.dec.DecodeBytes()
		return value, -1, -1
	}

	var (
		bytesLen     = dec.decodeBytesLen()
		backingBytes = byteProvider.Bytes()
		numBytes     = len(backingBytes)
		currPos      = int(int64(numBytes) - byteProvider.Remaining())
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
