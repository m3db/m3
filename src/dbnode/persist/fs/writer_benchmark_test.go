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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	xsync "github.com/m3db/m3/src/x/sync"
)

func BenchmarkCreateEmptyFilesets(b *testing.B) {
	type benchEmptyFileset struct {
		parallelism int
		numShards   int
	}

	testCases := []benchEmptyFileset{}
	for i := 2; i <= 4096; i *= 2 {
		testCases = append(testCases, benchEmptyFileset{
			parallelism: i,
			numShards:   1,
		})
		testCases = append(testCases, benchEmptyFileset{
			parallelism: i,
			numShards:   256,
		})
		testCases = append(testCases, benchEmptyFileset{
			parallelism: i,
			numShards:   1024,
		})
	}

	for _, tc := range testCases {
		title := fmt.Sprintf(
			"parallelism: %d, numShards: %d",
			tc.parallelism, tc.numShards)

		b.Run(title, func(b *testing.B) {
			benchmarkCreateEmptyFilesets(b, tc.parallelism, tc.numShards)
		})
	}
}

func benchmarkCreateEmptyFilesets(b *testing.B, parallelism, numShards int) {
	dir, err := ioutil.TempDir("", "testdir")
	if err != nil {
		panic(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	var (
		blockSize = 2 * time.Hour
		start     = time.Now().Truncate(blockSize)
	)

	workerPool, err := xsync.NewPooledWorkerPool(
		parallelism, xsync.NewPooledWorkerPoolOptions())
	if err != nil {
		panic(err)
	}
	workerPool.Init()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)

		workerPool.Go(func() {
			writerOpts := DataWriterOpenOptions{
				Identifier: FileSetFileIdentifier{
					Namespace:   testNs1ID,
					Shard:       uint32(i % numShards),
					BlockStart:  start,
					VolumeIndex: i,
				},
				BlockSize:   testBlockSize,
				FileSetType: persist.FileSetFlushType,
			}

			writer, err := NewWriter(testDefaultOpts.
				SetFilePathPrefix(filePathPrefix).
				SetWriterBufferSize(testWriterBufferSize))
			if err != nil {
				panic(err)
			}

			if err := writer.Open(writerOpts); err != nil {
				panic(err)
			}
			if err := writer.Close(); err != nil {
				panic(err)
			}
			wg.Done()
		})
	}

	wg.Wait()
}
