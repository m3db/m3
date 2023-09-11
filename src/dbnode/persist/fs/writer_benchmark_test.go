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
	xtime "github.com/m3db/m3/src/x/time"
)

// Benchmarks run on a production machine with 32 cores and non server-grade, non NVME SSD drives.
// goos: linux
// goarch: amd64
// pkg: github.com/m3db/m3/src/dbnode/persist/fs
// BenchmarkCreateEmptyFilesets/parallelism:_2,_numShards:_1-32         	   10000	    135045 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_2,_numShards:_256-32       	   10000	    124712 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_2,_numShards:_1024-32      	   10000	    149700 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_4,_numShards:_1-32         	   20000	     86291 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_4,_numShards:_256-32       	   20000	     94382 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_4,_numShards:_1024-32      	   20000	    102477 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_8,_numShards:_1-32         	   20000	     62403 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_8,_numShards:_256-32       	   20000	     68515 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_8,_numShards:_1024-32      	   20000	     72531 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_16,_numShards:_1-32        	   30000	     51230 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_16,_numShards:_256-32      	   50000	     41634 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_16,_numShards:_1024-32     	   30000	     48799 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_32,_numShards:_1-32        	   30000	     46718 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_32,_numShards:_256-32      	   50000	     38207 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_32,_numShards:_1024-32     	   30000	     40722 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_64,_numShards:_1-32        	   30000	     42638 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_64,_numShards:_256-32      	   50000	     34545 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_64,_numShards:_1024-32     	   30000	     37479 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_128,_numShards:_1-32       	   30000	     40628 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_128,_numShards:_256-32     	   50000	     34262 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_128,_numShards:_1024-32    	   30000	     37234 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_256,_numShards:_1-32       	   50000	     39045 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_256,_numShards:_256-32     	   50000	     33717 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_256,_numShards:_1024-32    	   30000	     37385 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_512,_numShards:_1-32       	   50000	     38813 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_512,_numShards:_256-32     	   50000	     33760 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_512,_numShards:_1024-32    	   30000	     36175 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_1024,_numShards:_1-32      	   50000	     46628 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_1024,_numShards:_256-32    	   50000	     33590 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_1024,_numShards:_1024-32   	   30000	     34465 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_2048,_numShards:_1-32      	   50000	     40628 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_2048,_numShards:_256-32    	   50000	     31257 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_2048,_numShards:_1024-32   	   30000	     34975 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_4096,_numShards:_1-32      	   30000	     40306 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_4096,_numShards:_256-32    	   50000	     34649 ns/op
// BenchmarkCreateEmptyFilesets/parallelism:_4096,_numShards:_1024-32   	   30000	     38800 ns/op
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
		start     = xtime.Now().Truncate(blockSize)
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

		workerPool.Go(func() {
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
