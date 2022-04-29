//go:build big
// +build big

// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/mmap"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/assert"
)

func TestIndexLookupWriteRead(t *testing.T) {
	// Define property test function which will be passed various propTestInputs
	propertyFunc := func(input propTestInput) (bool, error) {
		// Filter out duplicate IDs
		writes := []generatedWrite{}
		unique := map[string]struct{}{}
		for _, write := range input.realWrites {
			s := string(write.id.Bytes())
			if _, ok := unique[s]; ok {
				continue
			}
			unique[s] = struct{}{}
			writes = append(writes, write)
		}

		// Create a temporary directory for each test run
		dir, err := ioutil.TempDir("", "testdb")
		if err != nil {
			return false, err
		}
		filePathPrefix := filepath.Join(dir, "")
		defer os.RemoveAll(dir)

		// NB(r): Use testDefaultOpts to avoid allocing pools each
		// time we derive options
		options := testDefaultOpts.
			// Make sure that every index entry is also in the summaries file for the
			// sake of verifying behavior
			SetIndexSummariesPercent(1).
			SetFilePathPrefix(filePathPrefix).
			SetWriterBufferSize(testWriterBufferSize)
		shard := input.shard

		// Instantiate a writer and write the test data
		w, err := NewWriter(options)
		if err != nil {
			return false, fmt.Errorf("err creating writer: %v, ", err)
		}
		writerOpts := DataWriterOpenOptions{
			BlockSize: testBlockSize,
			Identifier: FileSetFileIdentifier{
				Namespace:  testNs1ID,
				Shard:      shard,
				BlockStart: testWriterStart,
			},
		}
		err = w.Open(writerOpts)
		if err != nil {
			return false, fmt.Errorf("err opening writer: %v, ", err)
		}
		shardDirPath := ShardDataDirPath(filePathPrefix, testNs1ID, shard)
		err = writeTestSummariesData(w, writes)
		if err != nil {
			return false, fmt.Errorf("err writing test summaries data: %v, ", err)
		}

		// Figure out the offsets for the writes so we have something to compare
		// our results against
		expectedIndexFileOffsets, err := readIndexFileOffsets(
			shardDirPath, len(writes), testWriterStart)
		if err != nil {
			return false, fmt.Errorf("err reading index file offsets: %v", err)
		}

		// Read the summaries file into memory
		summariesFilePath := dataFilesetPathFromTimeAndIndex(
			shardDirPath, testWriterStart, 0, summariesFileSuffix, false)
		summariesFile, err := os.Open(summariesFilePath)
		if err != nil {
			return false, fmt.Errorf("err opening summaries file: %v, ", err)
		}
		summariesFdWithDigest := digest.NewFdWithDigestReader(options.InfoReaderBufferSize())
		summariesFdWithDigest.Reset(summariesFile)
		expectedSummariesDigest := calculateExpectedChecksum(t, summariesFilePath)
		decoder := msgpack.NewDecoder(options.DecodingOptions())
		decoderStream := msgpack.NewByteDecoderStream(nil)
		indexLookup, err := newNearestIndexOffsetLookupFromSummariesFile(
			summariesFdWithDigest, expectedSummariesDigest,
			decoder, decoderStream, len(writes), input.forceMmapMemory, mmap.ReporterOptions{})
		if err != nil {
			return false, fmt.Errorf("err reading index lookup from summaries file: %v, ", err)
		}

		// Make sure it returns the correct index offset for every ID.
		resources := newTestReusableSeekerResources()
		for id, expectedOffset := range expectedIndexFileOffsets {
			foundOffset, err := indexLookup.getNearestIndexFileOffset(ident.StringID(id), resources)
			if err != nil {
				return false, fmt.Errorf("err locating index file offset for: %s, err: %v", id, err)
			}
			if expectedOffset != foundOffset {
				return false, fmt.Errorf(
					"offsets for: %s do not match, expected: %d, got: %d",
					id, expectedOffset, foundOffset)
			}
		}

		return true, nil
	}

	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(123456789)
	parameters.MinSuccessfulTests = 100
	props := gopter.NewProperties(parameters)

	props.Property(
		"Index lookup can properly lookup index offsets",
		prop.ForAll(propertyFunc, genPropTestInputs()),
	)

	props.TestingRun(t)
}

func calculateExpectedChecksum(t *testing.T, filePath string) uint32 {
	fileBytes, err := ioutil.ReadFile(filePath)
	assert.NoError(t, err)
	return digest.Checksum(fileBytes)
}

func writeTestSummariesData(w DataFileSetWriter, writes []generatedWrite) error {
	for _, write := range writes {
		metadata := persist.NewMetadataFromIDAndTags(write.id, write.tags,
			persist.MetadataOptions{})
		err := w.Write(metadata, write.data, write.checksum)
		if err != nil {
			return err
		}
	}
	return w.Close()
}

type propTestInput struct {
	// IDs to write and assert against
	realWrites []generatedWrite
	// Shard number to use for the files
	shard uint32
	// Whether the summaries file bytes should be mmap'd as an
	// anonymous region or file.
	forceMmapMemory bool
}

type generatedWrite struct {
	id       ident.ID
	tags     ident.Tags
	data     checked.Bytes
	checksum uint32
}

func genPropTestInputs() gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(0, 1000),
	).FlatMap(func(input interface{}) gopter.Gen {
		inputs := input.([]interface{})
		numRealWrites := inputs[0].(int)
		return genPropTestInput(numRealWrites)
	}, reflect.TypeOf(propTestInput{}))
}

func genPropTestInput(numRealWrites int) gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOfN(numRealWrites, genWrite()),
		gen.UInt32(),
		gen.Bool(),
	).Map(func(vals []interface{}) propTestInput {
		return propTestInput{
			realWrites:      vals[0].([]generatedWrite),
			shard:           vals[1].(uint32),
			forceMmapMemory: vals[2].(bool),
		}
	})
}

func genWrite() gopter.Gen {
	return gopter.CombineGens(
		// gopter will generate random strings, but some of them may be duplicates
		// (which can't normally happen for IDs and breaks this codepath), so we
		// filter down to unique inputs
		// ID
		gen.AnyString(),
		// Tag 1
		genTagIdent(),
		genTagIdent(),
		// Tag 2
		genTagIdent(),
		genTagIdent(),
		// Data
		gen.SliceOfN(100, gen.UInt8()),
	).Map(func(vals []interface{}) generatedWrite {
		id := vals[0].(string)
		tags := []ident.Tag{
			ident.StringTag(vals[1].(string), vals[2].(string)),
			ident.StringTag(vals[3].(string), vals[4].(string)),
		}
		data := vals[5].([]byte)

		return generatedWrite{
			id:       ident.StringID(id),
			tags:     ident.NewTags(tags...),
			data:     bytesRefd(data),
			checksum: digest.Checksum(data),
		}
	})
}

func genTagIdent() gopter.Gen {
	return gopter.CombineGens(
		gen.AlphaChar(),
		gen.AnyString(),
	).Map(func(vals []interface{}) string {
		return string(vals[0].(rune)) + vals[1].(string)
	})
}

func readIndexFileOffsets(shardDirPath string, numEntries int,
	start xtime.UnixNano) (map[string]int64, error) {
	indexFilePath := dataFilesetPathFromTimeAndIndex(shardDirPath, start, 0, indexFileSuffix, false)
	buf, err := ioutil.ReadFile(indexFilePath)
	if err != nil {
		return nil, fmt.Errorf("err reading index file: %v, ", err)
	}

	decoderStream := msgpack.NewByteDecoderStream(buf)
	decoder := msgpack.NewDecoder(testDefaultOpts.DecodingOptions())
	decoder.Reset(decoderStream)

	summariesOffsets := map[string]int64{}
	for read := 0; read < numEntries; read++ {
		offset := int64(len(buf)) - (decoderStream.Remaining())
		entry, err := decoder.DecodeIndexEntry(nil)
		if err != nil {
			return nil, fmt.Errorf("err decoding index entry: %v", err)
		}
		summariesOffsets[string(entry.ID)] = offset
	}
	return summariesOffsets, nil
}
