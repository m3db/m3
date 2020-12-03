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
	"errors"
	"fmt"
	"io"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/pool"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	emptyIndexInfo              schema.IndexInfo
	emptyIndexSummariesInfo     schema.IndexSummariesInfo
	emptyIndexBloomFilterInfo   schema.IndexBloomFilterInfo
	emptyIndexEntry             schema.IndexEntry
	emptyWideEntry              schema.WideEntry
	emptyIndexSummary           schema.IndexSummary
	emptyIndexSummaryToken      IndexSummaryToken
	emptyLogInfo                schema.LogInfo
	emptyLogEntry               schema.LogEntry
	emptyLogMetadata            schema.LogMetadata
	emptyLogEntryRemainingToken DecodeLogEntryRemainingToken

	errorUnableToDetermineNumFieldsToSkip          = errors.New("unable to determine num fields to skip")
	errorCalledDecodeBytesWithoutByteStreamDecoder = errors.New("called decodeBytes without byte stream decoder")
	errorIndexEntryChecksumMismatch                = errors.New("decode index entry encountered checksum mismatch")
)

// WideEntryLookupStatus is the status for a wide entry lookup.
type WideEntryLookupStatus byte

const (
	// ErrorLookupStatus indicates an error state.
	ErrorLookupStatus WideEntryLookupStatus = iota
	// MatchedLookupStatus indicates the current entry ID matches the requested ID.
	MatchedLookupStatus
	// MismatchLookupStatus indicates the current entry ID preceeds the requested ID.
	MismatchLookupStatus
	// NotFoundLookupStatus indicates the current entry ID is lexicographically larger than
	// the requested ID; since the index file is in sorted order, this means the
	// ID does does not exist in the file.
	NotFoundLookupStatus
)

// Decoder decodes persisted msgpack-encoded data
type Decoder struct {
	reader DecoderStream
	// Will only be set if the Decoder is Reset() with a DecoderStream
	// that also implements ByteStream.
	byteReader ByteStream
	// Wraps original reader with reader that can calculate digest. Digest calculation must be enabled,
	// otherwise it defaults to off.
	readerWithDigest  *decoderStreamWithDigest
	hasher            schema.IndexEntryHasher
	dec               *msgpack.Decoder
	err               error
	allocDecodedBytes bool

	legacy LegacyEncodingOptions
}

// NewDecoder creates a new decoder
func NewDecoder(opts DecodingOptions) *Decoder {
	return newDecoder(DefaultLegacyEncodingOptions, opts)
}

func newDecoder(legacy LegacyEncodingOptions, opts DecodingOptions) *Decoder {
	if opts == nil {
		opts = NewDecodingOptions()
	}
	reader := NewByteDecoderStream(nil)
	return &Decoder{
		allocDecodedBytes: opts.AllocDecodedBytes(),
		reader:            reader,
		dec:               msgpack.NewDecoder(reader),
		legacy:            legacy,
		hasher:            opts.IndexEntryHasher(),
		readerWithDigest:  newDecoderStreamWithDigest(nil),
	}
}

// Reset resets the data stream to decode from
func (dec *Decoder) Reset(stream DecoderStream) {
	dec.reader = stream

	// Do the type assertion upfront so that we don't have to do it
	// repeatedly later.
	if byteStream, ok := stream.(ByteStream); ok {
		dec.byteReader = byteStream
	} else {
		dec.byteReader = nil
	}

	dec.readerWithDigest.reset(dec.reader)
	dec.dec.Reset(dec.readerWithDigest)
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

// DecodeIndexEntry decodes index entry.
func (dec *Decoder) DecodeIndexEntry(bytesPool pool.BytesPool) (schema.IndexEntry, error) {
	if dec.err != nil {
		return emptyIndexEntry, dec.err
	}
	dec.readerWithDigest.setDigestReaderEnabled(true)
	_, numFieldsToSkip := dec.decodeRootObject(indexEntryVersion, indexEntryType)
	indexEntry := dec.decodeIndexEntry(bytesPool)
	dec.readerWithDigest.setDigestReaderEnabled(false)
	dec.skip(numFieldsToSkip)
	if dec.err != nil {
		return emptyIndexEntry, dec.err
	}
	return indexEntry, nil
}

// DecodeToWideEntry decodes an index entry into a wide entry.
func (dec *Decoder) DecodeToWideEntry(
	compareID []byte,
	bytesPool pool.BytesPool,
) (schema.WideEntry, WideEntryLookupStatus, error) {
	if dec.err != nil {
		return emptyWideEntry, NotFoundLookupStatus, dec.err
	}
	dec.readerWithDigest.setDigestReaderEnabled(true)
	_, numFieldsToSkip := dec.decodeRootObject(indexEntryVersion, indexEntryType)
	entry, status := dec.decodeWideEntry(compareID, bytesPool)
	dec.readerWithDigest.setDigestReaderEnabled(false)
	dec.skip(numFieldsToSkip)
	if status != MatchedLookupStatus || dec.err != nil {
		return emptyWideEntry, status, dec.err
	}

	return entry, status, nil
}

// DecodeIndexSummary decodes index summary.
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

// DecodeLogInfo decodes commit log info.
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

// DecodeLogEntry decodes commit log entry.
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

// DecodeLogMetadata decodes commit log metadata.
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

	// Overrides only used to test forwards compatibility.
	switch dec.legacy.DecodeLegacyIndexInfoVersion {
	case LegacyEncodingIndexVersionV1:
		// V1 had 6 fields.
		opts.override = true
		opts.numExpectedMinFields = 6
		opts.numExpectedCurrFields = 6
	case LegacyEncodingIndexVersionV2:
		// V2 had 8 fields.
		opts.override = true
		opts.numExpectedMinFields = 6
		opts.numExpectedCurrFields = 8
	case LegacyEncodingIndexVersionV3:
		// V3 had 9 fields.
		opts.override = true
		opts.numExpectedMinFields = 6
		opts.numExpectedCurrFields = 9
	case LegacyEncodingIndexVersionV4:
		// V4 had 10 fields.
		opts.override = true
		opts.numExpectedMinFields = 6
		opts.numExpectedCurrFields = 10
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
	if dec.legacy.DecodeLegacyIndexInfoVersion == LegacyEncodingIndexVersionV1 || actual < 8 {
		dec.skip(numFieldsToSkip)
		return indexInfo
	}

	// Decode fields added in V2.
	indexInfo.SnapshotTime = dec.decodeVarint()
	indexInfo.FileType = persist.FileSetType(dec.decodeVarint())

	// At this point if its a V2 file we've decoded all the available fields.
	if dec.legacy.DecodeLegacyIndexInfoVersion == LegacyEncodingIndexVersionV2 || actual < 9 {
		dec.skip(numFieldsToSkip)
		return indexInfo
	}

	// Decode fields added in V3.
	indexInfo.SnapshotID, _, _ = dec.decodeBytes()

	// At this point if its a V3 file we've decoded all the available fields.
	if dec.legacy.DecodeLegacyIndexInfoVersion == LegacyEncodingIndexVersionV3 || actual < 10 {
		dec.skip(numFieldsToSkip)
		return indexInfo
	}

	// Decode fields added in V4.
	indexInfo.VolumeIndex = int(dec.decodeVarint())

	// At this point if its a V4 file we've decoded all the available fields.
	if dec.legacy.DecodeLegacyIndexInfoVersion == LegacyEncodingIndexVersionV4 || actual < 11 {
		dec.skip(numFieldsToSkip)
		return indexInfo
	}

	// Decode fields added in V5.
	indexInfo.MinorVersion = dec.decodeVarint()

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

func (dec *Decoder) checkNumIndexEntryFields() (int, int, bool) {
	var opts checkNumFieldsOptions
	switch dec.legacy.DecodeLegacyIndexEntryVersion {
	case LegacyEncodingIndexEntryVersionV1:
		// V1 had 5 fields.
		opts.override = true
		opts.numExpectedMinFields = 5
		opts.numExpectedCurrFields = 5
	case LegacyEncodingIndexEntryVersionV2:
		// V2 had 6 fields.
		opts.override = true
		opts.numExpectedMinFields = 5
		opts.numExpectedCurrFields = 6
	case LegacyEncodingIndexEntryVersionCurrent:
		// V3 is current version, no overrides needed
		break
	default:
		dec.err = fmt.Errorf("invalid legacyEncodingIndexEntryVersion provided: %v",
			dec.legacy.DecodeLegacyIndexEntryVersion)
		return 0, 0, false
	}

	return dec.checkNumFieldsFor(indexEntryType, opts)
}

func (dec *Decoder) decodeIndexEntry(bytesPool pool.BytesPool) schema.IndexEntry {
	numFieldsToSkip, actual, ok := dec.checkNumIndexEntryFields()
	if !ok {
		return emptyIndexEntry
	}

	var indexEntry schema.IndexEntry
	indexEntry.Index = dec.decodeVarint()

	if bytesPool == nil {
		indexEntry.ID, _, _ = dec.decodeBytes()
	} else {
		indexEntry.ID = dec.decodeBytesWithPool(bytesPool)
	}

	indexEntry.Size = dec.decodeVarint()
	indexEntry.Offset = dec.decodeVarint()
	indexEntry.DataChecksum = dec.decodeVarint()

	// At this point, if its a V1 file, we've decoded all the available fields.
	if dec.legacy.DecodeLegacyIndexEntryVersion == LegacyEncodingIndexEntryVersionV1 || actual < 6 {
		dec.skip(numFieldsToSkip)
		return indexEntry
	}

	// Decode fields added in V2
	if bytesPool == nil {
		indexEntry.EncodedTags, _, _ = dec.decodeBytes()
	} else {
		indexEntry.EncodedTags = dec.decodeBytesWithPool(bytesPool)
	}

	// At this point, if its a V2 file, we've decoded all the available fields.
	if dec.legacy.DecodeLegacyIndexEntryVersion == LegacyEncodingIndexEntryVersionV2 || actual < 7 {
		dec.skip(numFieldsToSkip)
		return indexEntry
	}

	// NB(nate): Any new fields should be parsed here.

	// Intentionally skip any extra fields here as we've stipulated that from V3 onward, IndexEntryChecksum will be the
	// final field on index entries
	dec.skip(numFieldsToSkip)

	// Retrieve actual checksum value here. Attempting to retrieve after decoding the upcoming expected checksum field
	// would include value in actual checksum calculation which would cause a mismatch
	actualChecksum := dec.readerWithDigest.digest().Sum32()

	// Decode checksum field originally added in V3
	indexEntry.IndexChecksum = dec.decodeVarint()
	if dec.err != nil {
		dec.err = fmt.Errorf("decode index entry encountered error: %s", dec.err)
		return emptyIndexEntry
	}

	if indexEntry.IndexChecksum != int64(actualChecksum) {
		dec.err = errorIndexEntryChecksumMismatch
	}

	return indexEntry
}

func (dec *Decoder) decodeWideEntry(
	compareID []byte,
	bytesPool pool.BytesPool,
) (schema.WideEntry, WideEntryLookupStatus) {
	entry := dec.decodeIndexEntry(bytesPool)
	if dec.err != nil {
		return emptyWideEntry, ErrorLookupStatus
	}

	if entry.EncodedTags == nil {
		if bytesPool != nil {
			bytesPool.Put(entry.ID)
		}

		dec.err = fmt.Errorf("decode wide index requires files V1+")
		return emptyWideEntry, ErrorLookupStatus
	}

	compare := bytes.Compare(compareID, entry.ID)
	var checksum int64
	if compare == 0 {
		// NB: need to compute hash before freeing entry bytes.
		checksum = dec.hasher.HashIndexEntry(entry.ID, entry.EncodedTags, entry.DataChecksum)
		return schema.WideEntry{
			IndexEntry:       entry,
			MetadataChecksum: checksum,
		}, MatchedLookupStatus
	}

	if bytesPool != nil {
		bytesPool.Put(entry.ID)
		bytesPool.Put(entry.EncodedTags)
	}

	if compare > 0 {
		// compareID can still exist after the current entry.ID
		return emptyWideEntry, MismatchLookupStatus
	}

	// compareID must have been before the current entry.ID, so this
	// ID will not be matched.
	return emptyWideEntry, NotFoundLookupStatus
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
) (int, int, bool) {
	actual := dec.decodeNumObjectFields()
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

	numToSkip := actual - curr
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

// Should only be called if dec.byteReader != nil.
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

	if dec.byteReader == nil {
		// If we're not allowing the msgpack library to allocate the bytes and we haven't been
		// provided a byte decoder stream, then we've reached an invalid state as its not
		// possible for us to decode the bytes in an alloc-less way.
		dec.err = errorCalledDecodeBytesWithoutByteStreamDecoder
		return nil, -1, -1
	}

	var (
		bytesLen     = dec.decodeBytesLen()
		backingBytes = dec.byteReader.Bytes()
		numBytes     = len(backingBytes)
		currPos      = int(int64(numBytes) - dec.byteReader.Remaining())
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
	if err := dec.byteReader.Skip(int64(bytesLen)); err != nil {
		dec.err = err
		return nil, -1, -1
	}
	value = backingBytes[currPos:targetPos]
	if err := dec.readerWithDigest.capture(value); err != nil {
		dec.err = err
		return nil, -1, -1
	}

	return value, currPos, bytesLen
}

func (dec *Decoder) decodeBytesWithPool(bytesPool pool.BytesPool) []byte {
	if dec.err != nil {
		return nil
	}

	bytesLen := dec.decodeBytesLen()
	if dec.err != nil {
		return nil
	}
	if bytesLen < 0 {
		return nil
	}

	bytes := bytesPool.Get(bytesLen)[:bytesLen]
	n, err := io.ReadFull(dec.readerWithDigest, bytes)
	if err != nil {
		dec.err = err
		bytesPool.Put(bytes)
		return nil
	}
	if n != bytesLen {
		// This check is redundant because io.ReadFull will return an error if
		// its not able to read the specified number of bytes, but we keep it
		// in for posterity.
		dec.err = fmt.Errorf(
			"tried to decode checked bytes of length: %d, but read: %d",
			bytesLen, n)
		bytesPool.Put(bytes)
		return nil
	}

	return bytes
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
