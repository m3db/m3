// Copyright (c) 2016 Uber Technologies, Inc.
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

package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/server"
	"github.com/m3db/m3aggregator/services/m3aggregator/processor"
	"github.com/m3db/m3aggregator/services/m3aggregator/serve"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
)

const (
	gracefulShutdownTimeout = 10 * time.Second
	processorQueueSize      = 4096
)

var (
	listenAddrArg = flag.String("listenAddr", "0.0.0.0:6000", "server listen address")
)

func main() {
	flag.Parse()

	if *listenAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	aggregatorOpts := aggregator.NewOptions()
	log := aggregatorOpts.InstrumentOptions().Logger()
	metricWithPolicyfn := func(metric aggregated.Metric, policy policy.Policy) error {
		log.WithFields(
			xlog.NewLogField("metric", metric),
			xlog.NewLogField("policy", policy),
		).Info("aggregated metric")
		return nil
	}
	numWorkers := int(math.Max(float64(runtime.NumCPU()/4), 1.0))
	processor := processor.NewAggregatedMetricProcessor(
		processorQueueSize,
		numWorkers,
		metricWithPolicyfn,
		log,
		msgpack.NewAggregatedIteratorOptions(),
	)

	// Creating the aggregator
	aggregatorOpts = aggregatorOpts.SetFlushFn(processor.Add)
	aggregator := aggregator.NewAggregator(aggregatorOpts)

	// Creating the server
	listenAddr := *listenAddrArg
	serverOpts := server.NewOptions()

	// Start listening
	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := serve.Serve(
			listenAddr,
			serverOpts,
			aggregator,
			aggregatorOpts,
			doneCh,
		); err != nil {
			log.Fatalf("could not start serving traffic: %v", err)
		}
		log.Debug("server closed")
		processor.Close()
		log.Debug("processor closed")
		close(closedCh)
	}()

	// Handle interrupts
	log.Warnf("interrupt: %v", interrupt())

	close(doneCh)

	select {
	case <-closedCh:
		log.Info("server closed clean")
	case <-time.After(gracefulShutdownTimeout):
		log.Infof("server closed due to %s timeout", gracefulShutdownTimeout.String())
	}
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
