// Copyright (c) 2019 Uber Technologies, Inc.
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
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof" // pprof: for debug listen server if configured
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pkg/profile"
	"github.com/stretchr/testify/require"
)

// BenchmarkBootstrapIndex allows for testing indexing bootstrap time with the
// FS bootstrapper, this tests the speed and performance of index segment
// building from reading a set of files that sit on disk taken from a real
// DB node.
// To use test data and capture CPU profile run with:
// TEST_TSDB_DIR=/tmp/m3db_data PROFILE_CPU=true go test -v -run none -bench Index
func BenchmarkBootstrapIndex(b *testing.B) {
	dir, err := ioutil.TempDir("", "var_lib_m3db_fake")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	srv := httptest.NewServer(http.DefaultServeMux)
	spew.Printf("test server with pprof: %v\n", srv.URL)

	timesOpts := testTimesOptions{
		numBlocks: 2,
	}
	times := newTestBootstrapIndexTimes(timesOpts)

	testNamespace := testNs1ID
	testNamespaceMetadata := testNsMetadata(b)
	if testDir := os.Getenv("TEST_TSDB_DIR"); testDir != "" {
		spew.Printf("using test dir: %s\n", testDir)

		// Allow for test directory overrides, must name the namespace
		// "test_namespace" in the override directory.
		dir = testDir

		namespaceDataDirPath := fs.NamespaceDataDirPath(dir, testNamespace)
		handle, err := os.Open(namespaceDataDirPath)
		require.NoError(b, err)

		results, err := handle.Readdir(0)
		require.NoError(b, err)

		require.NoError(b, handle.Close())

		var shards []uint32
		for _, result := range results {
			if !result.IsDir() {
				// Looking for shard directories.
				spew.Printf("shard discover: entry not directory, %v\n", result.Name())
				continue
			}

			v, err := strconv.Atoi(result.Name())
			if err != nil {
				// Not a shard directory.
				spew.Printf("shard discover: not number, %v, %v\n", result.Name(), err)
				continue
			}

			shards = append(shards, uint32(v))
		}

		spew.Printf("discovered shards: dir=%v, shards=%v\n",
			namespaceDataDirPath, shards)

		// Clear the shard time ranges and add new ones.
		times.shardTimeRanges = result.NewShardTimeRanges()
		times.start = xtime.UnixNano(math.MaxInt64)
		times.end = xtime.UnixNano(0)
		for _, shard := range shards {
			var (
				min     = xtime.UnixNano(math.MaxInt64)
				max     = xtime.UnixNano(0)
				ranges  = xtime.NewRanges()
				entries = fs.ReadInfoFiles(dir, testNamespace, shard,
					0, msgpack.NewDecodingOptions(), persist.FileSetFlushType)
			)
			for _, entry := range entries {
				if entry.Err != nil {
					require.NoError(b, entry.Err.Error())
				}

				start := xtime.UnixNano(entry.Info.BlockStart)
				if start.Before(min) {
					min = start
				}

				blockSize := time.Duration(entry.Info.BlockSize)
				end := start.Add(blockSize)
				if end.After(max) {
					max = end
				}

				ranges.AddRange(xtime.Range{Start: start, End: end})

				// Override the block size if different.
				namespaceOpts := testNamespaceMetadata.Options()
				retentionOpts := namespaceOpts.RetentionOptions()
				currBlockSize := retentionOpts.BlockSize()
				if blockSize > currBlockSize {
					newRetentionOpts := retentionOpts.
						SetBlockSize(blockSize).
						// 42yrs of retention to make sure blocks are in retention.
						// Why 42? Because it's the answer to life, the universe and everything.
						SetRetentionPeriod(42 * 365 * 24 * time.Hour)
					newIndexOpts := namespaceOpts.IndexOptions().SetBlockSize(blockSize)
					newNamespaceOpts := namespaceOpts.
						SetRetentionOptions(newRetentionOpts).
						SetIndexOptions(newIndexOpts)
					testNamespaceMetadata, err = namespace.NewMetadata(testNamespace, newNamespaceOpts)
					require.NoError(b, err)
				}
			}

			if ranges.IsEmpty() {
				continue // Nothing to bootstrap for shard.
			}

			times.shardTimeRanges.Set(shard, ranges)

			if min.Before(times.start) {
				times.start = min
			}
			if max.After(times.end) {
				times.end = max
			}
		}
	} else {
		writeTSDBGoodTaggedSeriesDataFiles(b, dir, testNamespace, times.start)
	}

	testOpts := newTestOptionsWithPersistManager(b, dir).
		SetResultOptions(testDefaultResultOpts.SetSeriesCachePolicy(series.CacheLRU))

	src, err := newFileSystemSource(testOpts)
	require.NoError(b, err)

	runOpts := testDefaultRunOpts.
		SetPersistConfig(bootstrap.PersistConfig{
			Enabled:     true,
			FileSetType: persist.FileSetFlushType,
		})

	tester := bootstrap.BuildNamespacesTester(b, runOpts,
		times.shardTimeRanges, testNamespaceMetadata)
	defer tester.Finish()

	spew.Printf("running test with times: %v\n", times)

	if strings.ToLower(os.Getenv("PROFILE_CPU")) == "true" {
		p := profile.Start(profile.CPUProfile)
		defer p.Stop()
	}

	b.ResetTimer()
	b.StartTimer()
	tester.TestReadWith(src)
	b.StopTimer()
}
