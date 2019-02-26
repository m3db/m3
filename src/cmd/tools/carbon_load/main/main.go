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

// carbon_load is a tool for load testing carbon ingestion.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

const (
	metricFmt = "%s %.4f %v\n"
)

func startWorker(
	target string,
	metrics []string,
	targetQPS int,
	closeCh chan struct{},
) (numSuccess, numError int) {
	var (
		rng                  = rand.New(rand.NewSource(time.Now().UnixNano()))
		idealTimeBetweenSend = time.Duration(int(time.Second) / targetQPS)
	)

	for {
		select {
		case <-closeCh:
			return numSuccess, numError
		default:
		}

		// Establish initial connection and reestablish if we get disconnected.
		conn, err := net.Dial("tcp", target)
		if err != nil {
			fmt.Printf("dial error: %v, reconnecting in one second\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for err == nil {
			select {
			case <-closeCh:
				return numSuccess, numError
			default:
			}

			var (
				now     = time.Now()
				randIdx = rng.Int63n(int64(len(metrics)))
				metric  = metrics[randIdx]
			)

			_, err = fmt.Fprintf(conn, metricFmt, metric, rng.Float32(), now.Unix())
			numSuccess++

			timeSinceSend := time.Since(now)
			if timeSinceSend < idealTimeBetweenSend {
				time.Sleep(idealTimeBetweenSend - timeSinceSend)
			}
		}

		numError++
	}
}

func main() {
	var (
		target     = flag.String("target", "0.0.0.0:7204", "Target host port")
		numWorkers = flag.Int("numWorkers", 20, "Number of concurrent connections")
		numMetrics = flag.Int("cardinality", 1000, "Cardinality of metrics")
		metric     = flag.String("name", "local.random", "The metric you send will be [name].[0..1024]")
		targetQPS  = flag.Int("qps", 1000, "Target QPS")
		duration   = flag.Duration("duration", 10*time.Second, "Duration of test")
	)

	flag.Parse()
	if len(*target) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	metrics := make([]string, 0, *numMetrics)
	for i := 0; i < *numMetrics; i++ {
		metrics = append(metrics, fmt.Sprintf("%s.%d", *metric, i))
	}

	var (
		targetQPSPerWorker = *targetQPS / *numWorkers
		wg                 sync.WaitGroup
		lock               sync.Mutex
		numSuccess         int
		numError           int
		closeCh            = make(chan struct{})
	)
	for n := 0; n < *numWorkers; n++ {
		wg.Add(1)
		go func() {
			nSuccess, nError := startWorker(
				*target, metrics, targetQPSPerWorker, closeCh)

			lock.Lock()
			numSuccess += nSuccess
			numError += nError
			lock.Unlock()

			wg.Done()
		}()
	}

	start := time.Now()
	go func() {
		time.Sleep(*duration)

		fmt.Println("beginning shutdown...")
		close(closeCh)
	}()

	wg.Wait()

	durationOfBench := time.Since(start).Seconds()
	fmt.Println("average QPS: ", float64(numSuccess)/durationOfBench)
}
