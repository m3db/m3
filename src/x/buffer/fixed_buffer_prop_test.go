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

package buffer

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	xerrors "github.com/m3db/m3/src/x/errors"
)

func printMemUsage() (int, string) {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)

	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("Alloc = %v MiB", bToMb(m.Alloc)))
	builder.WriteString(fmt.Sprintf("\tHeapAlloc = %v MiB", bToMb(m.HeapAlloc)))
	builder.WriteString(fmt.Sprintf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc)))
	builder.WriteString(fmt.Sprintf("\tSys = %v MiB", bToMb(m.Sys)))
	builder.WriteString(fmt.Sprintf("\tNumGC = %v", m.NumGC))
	return int(m.TotalAlloc), builder.String()
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func TestRegexpCompilationProperty(t *testing.T) {
	initMem, initStr := printMemUsage()
	fmt.Println("Initial", initMem, ":", initStr)

	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 1 //00000
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	var (
		bufferCapacity  = 256
		bufferCount     = 32
		numberOfBuffers = bufferCount * 100

		opts = NewOptions().
			SetFixedBufferCapacity(bufferCapacity).
			SetFixedBufferCount(bufferCount).
			SetFixedBufferTimeout(time.Millisecond * 20)
	)

	properties.Property("Does not grow stack", prop.ForAll(
		func(byteSlices [][]byte, takeSlices [][]takeBytes) (bool, error) {
			var (
				wg       sync.WaitGroup
				mu       sync.Mutex
				multiErr xerrors.MultiError

				manager            = newFixedBufferManager(opts)
				startMem, startStr = printMemUsage()
			)

			for i := 0; i < numberOfBuffers; i++ {
				byteSlice := byteSlices[i]
				takeSlice := takeSlices[i]
				wg.Add(1)
				go func() {
					defer wg.Done()

					var leaseWg sync.WaitGroup
					for _, take := range takeSlice {
						takingBytes := byteSlice[0:take.size]
						copied, lease, err := manager.Copy(takingBytes)
						if err != nil {
							mu.Lock()
							multiErr = multiErr.Add(err)
							mu.Unlock()
							return
						}

						workDuration := take.runtime
						leaseWg.Add(1)
						go func() {
							time.Sleep(workDuration)

							if !bytes.Equal(takingBytes, copied) {
								mu.Lock()
								multiErr = multiErr.Add(fmt.Errorf("bytes not equal: "+
									"expected %d, got %d", takingBytes, copied))
								mu.Unlock()
								return
							}

							lease.Finalize()
							leaseWg.Done()
						}()

						byteSlice = byteSlice[:take.size]
					}

					leaseWg.Wait()
				}()
			}

			wg.Wait()

			endMem, endStr := printMemUsage()
			fmt.Println("startMem", startMem, "endMem", endMem, startMem-endMem, "init diff", initMem-endMem)
			fmt.Println("start", startStr)
			fmt.Println("end  ", endStr)
			return true, nil
		},
		genLongBytesChunked(numberOfBuffers, bufferCapacity),
		genTakeByteSlices(numberOfBuffers, bufferCapacity),
	))

	// genParam := parameters.(gen.GenParameters)
	// fmt.Println(genParam.NextUInt8())
	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func genLongBytesChunked(size, count int) gopter.Gen {
	return gen.SliceOfN(size, genLongBytes(count))
}

func genLongBytes(count int) gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOfN(count, gen.UInt8()),
	).Map(func(vals []interface{}) []byte {
		inInts := vals[0].([]uint8)
		out := make([]byte, 0, len(inInts))
		for _, in := range inInts {
			out = append(out, byte(in))
		}

		return out
	})
}

type takeBytes struct {
	size    int
	runtime time.Duration
}

func genTakeByteSlices(size, count int) gopter.Gen {
	return gen.SliceOfN(size, genTakeBytes(count))
}

func genTakeBytes(count int) gopter.Gen {
	return gen.SliceOfN(
		count,
		gopter.CombineGens(
			gen.IntRange(1, count),
			gen.IntRange(int(time.Millisecond), int(time.Millisecond*10)),
		),
	).Map(func(vals [][]interface{}) []takeBytes {
		out := make([]takeBytes, 0, len(vals))
		countSoFar := 0
		for _, in := range vals {
			var (
				size    = in[0].(int)
				runtime = time.Duration(in[1].(int))
			)
			if countSoFar+size > count {
				size = count - countSoFar
				if size > 0 {
					out = append(out, takeBytes{size: size, runtime: runtime})
				}

				return out
			}

			countSoFar = size + countSoFar
			out = append(out, takeBytes{size: size, runtime: runtime})
		}

		return out
	})
}
