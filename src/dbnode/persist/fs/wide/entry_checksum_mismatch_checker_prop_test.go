// +build big
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package wide

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

func generateRawChecksums(size int, opts Options) []xio.IndexChecksum {
	checksums := make([]xio.IndexChecksum, size)
	indexHasher := opts.DecodingOptions().IndexEntryHasher()
	for i := range checksums {
		idStr := fmt.Sprintf("id-%03d", i)
		tags := []byte(fmt.Sprintf("tags-%03d", i))

		entry := schema.IndexEntry{
			ID:          []byte(idStr),
			EncodedTags: tags,
		}

		checksums[i] = xio.IndexChecksum{
			ID:               ident.StringID(idStr),
			EncodedTags:      checked.NewBytes(tags, checked.NewBytesOptions()),
			MetadataChecksum: indexHasher.HashIndexEntry(entry),
		}
	}

	return checksums
}

type generatedEntries struct {
	taking  []bool
	entries []xio.IndexChecksum
}

// genEntryTestInput creates a list of indexChecksums,
// dropping a certain percentage.
func genEntryTestInput(size int, opts Options) gopter.Gen {
	entries := generateRawChecksums(size, opts)

	return gopter.CombineGens(
		// NB: This generator controls if the element should be removed
		gen.SliceOfN(len(entries), gen.IntRange(0, 100)),
	).Map(func(val []interface{}) generatedEntries {
		var (
			dropChancePercent = val[0].([]int)

			taking       []bool
			takenEntries []xio.IndexChecksum
		)

		for i, chance := range dropChancePercent {
			shouldKeep := chance <= 80
			taking = append(taking, shouldKeep)
			if shouldKeep {
				takenEntries = append(takenEntries, entries[i])
			}
		}

		return generatedEntries{taking: taking, entries: takenEntries}
	})
}

type generatedChecksums struct {
	taking     []bool
	blockBatch []IndexChecksumBlockBatch
}

// genChecksumTestInput creates index checksum blockBatch of randomized sizes,
// dropping a certain percentage of index checksums.
func genChecksumTestInput(size int, opts Options) gopter.Gen {
	entries := generateRawChecksums(size, opts)

	return gopter.CombineGens(
		// NB: This generator controls if the element should be removed
		gen.SliceOfN(len(entries), gen.IntRange(0, 100)),
		// NB: This generator controls how large each batch will be
		gen.SliceOfN(len(entries), gen.IntRange(1, len(entries))),
	).Map(func(val []interface{}) generatedChecksums {
		var (
			dropChancePercent = val[0].([]int)
			blockSizes        = val[1].([]int)

			taking         []bool
			takenChecksums []xio.IndexChecksum
			checksumBlocks []IndexChecksumBlockBatch
		)

		for i, chance := range dropChancePercent {
			shouldKeep := chance <= 80
			taking = append(taking, shouldKeep)
			if shouldKeep {
				takenChecksums = append(takenChecksums, xio.IndexChecksum{
					ID:               entries[i].ID,
					MetadataChecksum: entries[i].MetadataChecksum,
				})
			}
		}

		for _, blockSize := range blockSizes {
			remaining := len(takenChecksums)
			if remaining == 0 {
				break
			}

			take := blockSize
			if remaining < take {
				take = remaining
			}

			block := IndexChecksumBlockBatch{
				Checksums: make([]int64, 0, take),
			}

			for i := 0; i < take; i++ {
				block.Checksums = append(block.Checksums, takenChecksums[i].MetadataChecksum)
				block.EndMarker = takenChecksums[i].ID.Bytes()
			}

			takenChecksums = takenChecksums[take:]
			checksumBlocks = append(checksumBlocks, block)
		}

		return generatedChecksums{taking: taking, blockBatch: checksumBlocks}
	})
}

type mismatchChecksumBatch struct {
	lastElementMarker bool
	mismatches        []mismatchChecksum
}

func (b *mismatchChecksumBatch) gatherContiguousMismatchValues() {
	var (
		checksumSet         bool
		hasEntryMismatch    bool
		hasChecksumMismatch bool
		contiguousCount     int
		nextContiguous      int64
	)

	for idx, mismatchChecksum := range b.mismatches {
		var (
			lastIsContiguous bool

			checksum = mismatchChecksum.checksum
			isLast   = idx == len(b.mismatches)-1
		)

		// NB: gather the number of contiguous mismatches. Mismatches are contiguous
		// if they appear one after another, with no matching entries between them.
		if !checksumSet || checksum == nextContiguous {
			checksumSet = true

			if mismatchChecksum.entryMismatch {
				hasEntryMismatch = true
			} else {
				hasChecksumMismatch = true
			}

			contiguousCount++
			if !isLast {
				// If this is not the last mismatch, increase the contiguous length.
				nextContiguous = checksum + 1
				continue
			} else {
				lastIsContiguous = true
			}
		}

		// A contiguous set of mismatches should be sorted IFF:
		//  - at least 2 values
		//  - contiguous set contains both entry and checksum mismatches
		// After sorting, all entry mismatches should appear first, in
		// increasing order, followed by index mismatches in increasing order.
		// NB: if the last element of a batch is a mismatch, it is fixed and should
		// not be sorted.
		if contiguousCount > 1 && hasEntryMismatch && hasChecksumMismatch {
			firstContiguous := idx - contiguousCount
			lastContiguous := idx
			if lastIsContiguous {
				firstContiguous++
				if !b.lastElementMarker {
					lastContiguous++
				}
			}

			contiguousSlice := b.mismatches[firstContiguous:lastContiguous]
			sort.Slice(contiguousSlice, func(i, j int) bool {
				iEntry, jEntry := contiguousSlice[i], contiguousSlice[j]
				if iEntry.entryMismatch {
					if !jEntry.entryMismatch {
						// entry mismatches always come before checksum mismatches.
						return true
					}

					// these should be sorted by lex order
					return iEntry.checksum < jEntry.checksum
				}

				if jEntry.entryMismatch {
					// checksum mismatches always come after entry mismatches.
					return false
				}

				// these should be sorted by lex order
				return iEntry.checksum < jEntry.checksum
			})
		}

		// clear
		contiguousCount = 1
		hasChecksumMismatch = false
		hasEntryMismatch = false
		if mismatchChecksum.entryMismatch {
			hasEntryMismatch = true
		} else {
			hasChecksumMismatch = true
		}

		nextContiguous = checksum + 1
	}
}

func allMismatchChecksumsToMismatchesByBatch(
	checksums generatedChecksums,
	allMismatchChecksums []mismatchChecksum,
) []mismatchChecksumBatch {
	allMismatchIdx := 0
	var mismatchBatch []mismatchChecksumBatch
	for _, batch := range checksums.blockBatch {
		l := len(batch.Checksums)
		if l == 0 {
			continue
		}

		lastChecksum := batch.Checksums[l-1]
		lastElementMarker := false
		var mismatches []mismatchChecksum
		for _, mismatch := range allMismatchChecksums[allMismatchIdx:] {
			if mismatch.checksum > lastChecksum {
				// mismatch past last checksum in batch; append current batch and
				// start a new one.
				break
			}

			mismatches = append(mismatches, mismatch)
			allMismatchIdx++
			if mismatch.checksum == lastChecksum {
				// mismatch is last checksum in batch; append current batch and
				// start a new one.
				lastElementMarker = true
				break
			}
		}

		if len(mismatches) == 0 {
			continue
		}

		// add a mismatch batch; imporant to note if the last element is a mismatch,
		// since if it is, it should always remain the last element, regardless of
		// if it forms a contiguous group or not.
		mismatchBatch = append(mismatchBatch, mismatchChecksumBatch{
			lastElementMarker: lastElementMarker,
			mismatches:        mismatches,
		})
	}

	// add any remaining mismatch checksums as a separate batch. This is ok
	// since they will all be entry mismatches, so no additional sorting will be
	// performed on this batch.
	if allMismatchIdx < len(allMismatchChecksums) {
		mismatchBatch = append(mismatchBatch, mismatchChecksumBatch{
			lastElementMarker: false,
			mismatches:        allMismatchChecksums[allMismatchIdx:],
		})
	}

	return mismatchBatch
}

type mismatchChecksum struct {
	missingOnBoth bool
	checksum      int64
	entryMismatch bool
}

func buildExpectedMismatchChecksums(
	checksums generatedChecksums,
	takeEntries []bool,
) []mismatchChecksum {
	var allMismatchChecksums []mismatchChecksum
	takeChecksums := checksums.taking
	// Collect only elements that don't match.
	for idx, takeEntry := range takeEntries {
		if takeEntry != takeChecksums[idx] {
			allMismatchChecksums = append(allMismatchChecksums, mismatchChecksum{
				checksum:      int64(idx),
				entryMismatch: takeEntry,
			})
		} else if !takeEntry && !takeChecksums[idx] {
			// Note checksums missing from both sets; this will be necessary when
			// checking for congiuous series in gatherContiguousMismatchValues.
			allMismatchChecksums = append(allMismatchChecksums, mismatchChecksum{
				missingOnBoth: true,
				checksum:      int64(idx),
			})
		}
	}

	var gatheredMismatchChecksums []mismatchChecksum
	// Gather mismatches to match incoming batches.
	mismatchesByBatch := allMismatchChecksumsToMismatchesByBatch(checksums, allMismatchChecksums)
	for _, batchMismatches := range mismatchesByBatch {
		// Sort each batch as will be expected in output.
		batchMismatches.gatherContiguousMismatchValues()

		// Filter out series which do not appear in either checksum source.
		filteredMismatches := batchMismatches.mismatches[:0]
		for _, mismatch := range batchMismatches.mismatches {
			if !mismatch.missingOnBoth {
				filteredMismatches = append(filteredMismatches, mismatch)
			}
		}

		gatheredMismatchChecksums = append(gatheredMismatchChecksums, filteredMismatches...)
	}

	return gatheredMismatchChecksums
}

func TestIndexEntryWideBatchMismatchChecker(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 80, os.Stdout)

		hasher       = xhash.NewParsedIndexHasher(t)
		decodingOpts = msgpack.NewDecodingOptions().SetIndexEntryHasher(hasher)
		opts         = NewOptions().SetDecodingOptions(decodingOpts)

		size     = 100
		numTests = 1000
	)

	parameters.MinSuccessfulTests = numTests
	parameters.Rng.Seed(seed)

	// NB: capture seed to be able to replicate failed runs.
	fmt.Println("Running test with seed", seed)
	props.Property("Checksum mismatcher detects correctly",
		prop.ForAll(
			func(
				genChecksums generatedChecksums,
				genEntries generatedEntries,
			) (bool, error) {
				inputBlockCh := make(chan IndexChecksumBlockBatch)
				inputBlockReader := NewIndexChecksumBlockBatchReader(inputBlockCh)

				go func() {
					for _, bl := range genChecksums.blockBatch {
						inputBlockCh <- bl
					}

					close(inputBlockCh)
				}()

				checker := NewEntryChecksumMismatchChecker(inputBlockReader, opts)
				var readMismatches []ReadMismatch
				for _, entry := range genEntries.entries {
					entryMismatches, err := checker.ComputeMismatchesForEntry(entry)
					if err != nil {
						return false, fmt.Errorf("failed to compute index entry: %v", err)
					}

					readMismatches = append(readMismatches, entryMismatches...)
				}

				readMismatches = append(readMismatches, checker.Drain()...)
				expectedMismatches := buildExpectedMismatchChecksums(
					genChecksums, genEntries.taking)

				if len(expectedMismatches) != len(readMismatches) {
					return false, fmt.Errorf("expected %d expectedMismatches, got %d",
						len(expectedMismatches), len(readMismatches))
				}

				for i, expected := range expectedMismatches {
					actual := readMismatches[i]
					if actual.MetadataChecksum != expected.checksum {
						return false, fmt.Errorf("expected checksum %d, got %d at %d",
							actual.MetadataChecksum, expected.checksum, i)
					}

					if expected.entryMismatch {
						expectedTags := fmt.Sprintf("tags-%03d", actual.MetadataChecksum)
						actual.EncodedTags.IncRef()
						acTags := string(actual.EncodedTags.Bytes())
						actual.EncodedTags.DecRef()
						if acTags != expectedTags {
							return false, fmt.Errorf("expected tags %s, got %s",
								expectedTags, acTags)
						}

						expectedID := fmt.Sprintf("id-%03d", actual.MetadataChecksum)
						if acID := actual.ID.String(); acID != expectedID {
							return false, fmt.Errorf("expected tags %s, got %s",
								expectedID, acID)
						}
					} else {
						if actual.EncodedTags != nil {
							return false, fmt.Errorf("index mismatch should not have tags")
						}
						if actual.ID != nil {
							return false, fmt.Errorf("index mismatch should not have id")
						}
					}
				}

				return true, nil
			}, genChecksumTestInput(size, opts), genEntryTestInput(size, opts)))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}
