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

package process

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type cleanupFn func()

func TestNumFDs(t *testing.T) {
	for i := 0; i <= 8; i++ {
		var numFiles int
		if i == 0 {
			numFiles = 0
		} else {
			numFiles = int(math.Pow(float64(2), float64(i)))
		}

		func() {
			numExpectedFds := numFiles + 5
			cleanupFn := createTempFiles(numFiles)
			defer cleanupFn()

			selfPID := os.Getpid()

			t.Run(fmt.Sprintf("func: %s, numFiles: %d", "NumFDsReference", numFiles), func(t *testing.T) {
				numFDs, err := NumFDsReference(selfPID)
				require.NoError(t, err)
				require.Equal(t, numExpectedFds, numFDs)
			})

			t.Run(fmt.Sprintf("func: %s, numFiles: %d", "NumFDs", numFiles), func(t *testing.T) {
				numFDs, err := NumFDs(selfPID)
				require.NoError(t, err)
				require.Equal(t, numExpectedFds, numFDs)
			})

			t.Run(fmt.Sprintf("func: %s, numFiles: %d", "NumFDsWithDefaultBatchSleep", numFiles), func(t *testing.T) {
				numFDs, err := NumFDsWithDefaultBatchSleep(selfPID)
				require.NoError(t, err)
				require.Equal(t, numExpectedFds, numFDs)
			})
		}()
	}
}

func BenchmarkNumFDs(b *testing.B) {
	var (
		// Low for C.I and local testing, bump this up to a much larger number
		// when performing actual benchmarking.
		numFiles = 16000
		// +5 to account for standard F.Ds that each process gets.
		numExpectedFds = numFiles + 5
	)
	cleanupFn := createTempFiles(numFiles)
	defer cleanupFn()

	selfPID := os.Getpid()
	b.Run("NumFDsReference", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			numFDs, err := NumFDsReference(selfPID)
			if err != nil {
				b.Fatal(err)
			}
			if numFDs != numExpectedFds {
				b.Fatalf("expected %d files but got %d", numExpectedFds, numFDs)
			}
		}
	})

	b.Run("NumFDs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			numFDs, err := NumFDs(selfPID)
			if err != nil {
				b.Fatal(err)
			}
			if numFDs != numExpectedFds {
				b.Fatalf("expected %d files but got %d", numExpectedFds, numFDs)
			}
		}
	})

	b.Run("NumFDsWithDefaultBatchSleep", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			numFDs, err := NumFDsWithDefaultBatchSleep(selfPID)
			if err != nil {
				b.Fatal(err)
			}
			if numFDs != numExpectedFds {
				b.Fatalf("expected %d files but got %d", numExpectedFds, numFDs)
			}
		}
	})
}

func createTempFiles(numFiles int) cleanupFn {
	tempDir, err := ioutil.TempDir("", "test")
	if err != nil {
		panic(err)
	}

	files := make([]*os.File, 0, numFiles)
	for i := 0; i < numFiles; i++ {
		tempFilePath := filepath.Join(tempDir, fmt.Sprintf("%d.txt", i))
		f, err := os.OpenFile(tempFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			panic(err)
		}
		files = append(files, f)
	}

	return func() {
		for _, f := range files {
			f.Close()
		}
		os.RemoveAll(tempDir)
	}

}
