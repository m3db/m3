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
	"github.com/m3db/m3/src/x/ident"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

func generateRawEntries(size int) []schema.IndexEntry {
	entries := make([]schema.IndexEntry, size)
	for i := range entries {
		entries[i] = schema.IndexEntry{
			ID:          []byte(fmt.Sprintf("foo-%03d", i)),
			EncodedTags: []byte(fmt.Sprintf("bar-%03d", i)),
		}
	}

	return entries
}

type generatedEntries struct {
	taking  []bool
	entries []schema.IndexEntry
}

// genEntryTestInput creates a list of indexEntries, dropping a certain percentage.
func genEntryTestInput(size int, opts Options) gopter.Gen {
	entries := generateRawEntries(size)

	return gopter.CombineGens(
		// NB: This generator controls if the element should be removed
		gen.SliceOfN(len(entries), gen.IntRange(0, 10)),
	).Map(func(val []interface{}) generatedEntries {
		var (
			dropChances = val[0].([]int)

			taking       []bool
			takenEntries []schema.IndexEntry
		)

		for i, chance := range dropChances {
			shouldKeep := chance <= 9
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
	blockBatch []ident.IndexChecksumBlock
}

// genChecksumTestInput creates index checksum blockBatch of randomized sizes,
// dropping a certain percentage of index checksums.
func genChecksumTestInput(size int, opts Options) gopter.Gen {
	entries := generateRawEntries(size)

	indexHasher := opts.DecodingOptions().IndexEntryHasher()
	return gopter.CombineGens(
		// NB: This generator controls if the element should be removed
		gen.SliceOfN(len(entries), gen.IntRange(0, 10)),
		// NB: This generator controls how large each batch will be
		gen.SliceOfN(len(entries), gen.IntRange(1, len(entries))),
	).Map(func(val []interface{}) generatedChecksums {
		var (
			dropChances = val[0].([]int)
			blockSizes  = val[0].([]int)

			taking         []bool
			takenChecksums []ident.IndexChecksum
			checksumBlocks []ident.IndexChecksumBlock
		)

		for i, chance := range dropChances {
			shouldKeep := chance <= 9
			taking = append(taking, shouldKeep)
			if shouldKeep {
				takenChecksums = append(takenChecksums, ident.IndexChecksum{
					ID:       entries[i].ID,
					Checksum: indexHasher.HashIndexEntry(entries[i]),
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

			block := ident.IndexChecksumBlock{
				Checksums: make([]int64, 0, take),
			}

			for i := 0; i < take; i++ {
				block.Checksums = append(block.Checksums, takenChecksums[i].Checksum)
				block.EndMarker = takenChecksums[i].ID
			}

			takenChecksums = takenChecksums[take:]
			checksumBlocks = append(checksumBlocks, block)
		}

		return generatedChecksums{taking: taking, blockBatch: checksumBlocks}
	})
}

func (c mismatchChecksum) String() string {
	return fmt.Sprintf("{c: %d, e: %v}", c.checksum, c.entryMismatch)
}

type mismatchChecksumBatch struct {
	lastElementMarker bool
	mismatches        []mismatchChecksum
}

func TestGatherContiguousMismatchValues(t *testing.T) {
	b := mismatchChecksumBatch{
		mismatches: []mismatchChecksum{
			{false, 0, true}, {false, 1, false}, {true, 2, false},
			{false, 3, true}, {false, 6, false}, {false, 8, false},
		},
	}

	b.gatherContiguousMismatchValues()
	fmt.Println(b)
}

func (b *mismatchChecksumBatch) gatherContiguousMismatchValues() {
	checksumSet := false
	hasEntryMismatch := false
	hasChecksumMismatch := false
	nextContiguous := int64(0)
	contiguous := 0

	// fmt.Println("mismatch", b)
	pr := func(idx int64, a ...interface{}) {
		// if idx > 90 {
		// 	fmt.Println(a...)
		// }
	}
	for idx, mismatchChecksum := range b.mismatches {
		checksum := mismatchChecksum.checksum
		isLast := idx == len(b.mismatches)-1
		lastIsContiguous := false
		if !checksumSet || checksum == nextContiguous {
			checksumSet = true
			pr(checksum, "checking checksum", checksum, nextContiguous, !isLast, !b.lastElementMarker)

			if mismatchChecksum.entryMismatch {
				hasEntryMismatch = true
			} else {
				hasChecksumMismatch = true
			}

			contiguous++
			if !isLast {
				nextContiguous = checksum + 1
				continue
			} else {
				lastIsContiguous = true
			}
		}

		pr(checksum, "   contiguous", contiguous, "index", idx, hasEntryMismatch, hasChecksumMismatch)
		// sort any contiguous values only if both entry and checksum mismatches
		// present in the contiguous group.
		// fmt.Println("idx-contiguous", idx, contiguous, idx-contiguous)
		if contiguous > 1 && hasEntryMismatch && hasChecksumMismatch {
			firstContiguous := idx - contiguous
			lastContiguous := idx
			if lastIsContiguous {
				firstContiguous++
				if !b.lastElementMarker {
					lastContiguous++
				}
			}
			contiguousSlice := b.mismatches[firstContiguous:lastContiguous]
			pr(checksum, "Slice before", contiguousSlice)
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
			pr(checksum, "Slice after ", contiguousSlice)
		}

		// clear
		contiguous = 1
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
				break
			}

			mismatches = append(mismatches, mismatch)
			allMismatchIdx++
			if mismatch.checksum == lastChecksum {
				lastElementMarker = true
				break
			}
		}

		if len(mismatches) == 0 {
			continue
		}

		mismatchBatch = append(mismatchBatch, mismatchChecksumBatch{
			lastElementMarker: lastElementMarker,
			mismatches:        mismatches,
		})
	}

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
	print bool,
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
			allMismatchChecksums = append(allMismatchChecksums, mismatchChecksum{
				missingOnBoth: true,
				checksum:      int64(idx),
			})
		}
	}

	var gatheredMismatchChecksums []mismatchChecksum
	if print {
		fmt.Println("allMismatchChecksums\n", allMismatchChecksums)
	}
	mismatchesByBatch := allMismatchChecksumsToMismatchesByBatch(checksums, allMismatchChecksums)
	if print {
		fmt.Println("mismatchesByBatch")
		for i, b := range mismatchesByBatch {
			fmt.Println("", i, b)
		}
	}
	for _, batchMismatches := range mismatchesByBatch {
		batchMismatches.gatherContiguousMismatchValues()
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

func TestRawChecksums(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 80, os.Stdout)

		size = 300

		hasher       = xhash.NewParsedIndexHasher(t)
		decodingOpts = msgpack.NewDecodingOptions().SetIndexEntryHasher(hasher)
		opts         = NewOptions().SetDecodingOptions(decodingOpts)
	)

	parameters.MinSuccessfulTests = 2000
	// size, seed = 300,  1602212654922291000
	seed = 1602212654922291000
	// logRun := 0
	parameters.Rng.Seed(seed)
	j := 0
	fmt.Println("Running test with seed", seed)
	props.Property("Checksum mismatcher detects correctly",
		prop.ForAll(
			func(
				genChecksums generatedChecksums,
				genEntries generatedEntries,
			) (bool, error) {
				// fmt.Println("RUN", j)
				j++
				// fmt.Println("taking checksums:")
				// for i, take := range genChecksums.taking {
				// 	if !take {
				// 		fmt.Println(i, take)
				// 	}
				// }

				// fmt.Println("taking entries:")
				// for i, take := range genEntries.taking {
				// 	if !take {
				// 		fmt.Println(i, take)
				// 	}
				// }

				// for _, bl := range genChecksums.blockBatch {
				// 	fmt.Println(string(bl.EndMarker), bl.Checksums)
				// }

				inputBlockCh := make(chan ident.IndexChecksumBlock)
				inputBlockReader := NewIndexChecksumBlockReader(inputBlockCh)

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

					if len(entryMismatches) > 0 {
						// fmt.Println("ENTRY", entry)
						// fmt.Println("MISMATCHES", entryMismatches)
					}
					readMismatches = append(readMismatches, entryMismatches...)
				}

				drained := checker.Drain()
				if len(drained) > 0 {
					// fmt.Println("DRAINING")
					// fmt.Println("DRAINED", drained)
				}
				readMismatches = append(readMismatches, drained...)

				expectedMismatches := buildExpectedMismatchChecksums(
					genChecksums, genEntries.taking, false)

				// if j > 48 {
				// 	fmt.Println()
				// 	fmt.Println(j)
				// 	fmt.Println()
				// 	fmt.Println("expected")
				// 	fmt.Println(expectedMismatches)
				// 	fmt.Println()
				// 	fmt.Println("actual")
				// 	fmt.Println(readMismatches)
				// }

				if len(expectedMismatches) != len(readMismatches) {
					return false, fmt.Errorf("expected %d expectedMismatches, got %d",
						len(expectedMismatches), len(readMismatches))
				}

				for i, expected := range expectedMismatches {
					actual := readMismatches[i]
					if actual.Checksum != expected.checksum {
						return false, fmt.Errorf("expected checksum %d, got %d at %d",
							actual.Checksum, expected.checksum, i)
					}

					// fmt.Println(i, "mismatch", readMismatches[i], "expected", expected)
					// if print {
					// 	fmt.Println("after\n", allMismatchChecksums)
					// }
					if expected.entryMismatch {
						expectedTags := fmt.Sprintf("bar-%03d", actual.Checksum)
						if acTags := string(actual.EncodedTags); acTags != expectedTags {
							return false, fmt.Errorf("expected tags %s, got %s",
								expectedTags, acTags)
						}
					} else {
						if len(actual.EncodedTags) > 0 {
							return false, fmt.Errorf("expected mismatch checksum only mismatch")
						}
					}
				}

				return true, nil
			}, genChecksumTestInput(size, opts), genEntryTestInput(size, opts)))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}
