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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	httpserver "github.com/m3db/m3aggregator/server/http"
	msgpackserver "github.com/m3db/m3aggregator/server/msgpack"
	"github.com/m3db/m3aggregator/services/m3aggregator/processor"
	"github.com/m3db/m3aggregator/services/m3aggregator/serve"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/log"
)

const (
	gracefulShutdownTimeout = 10 * time.Second
)

var (
	msgpackAddrArg = flag.String("msgpackAddr", "0.0.0.0:6000", "msgpack server address")
	httpAddrArg    = flag.String("httpAddr", "0.0.0.0:6001", "http server address")
)

func main() {
	flag.Parse()

	if *msgpackAddrArg == "" || *httpAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Creating the downstream processor
	processorOpts := processor.NewOptions()
	log := processorOpts.InstrumentOptions().Logger()
	metricWithPolicyfn := func(metric aggregated.Metric, policy policy.Policy) error {
		log.WithFields(
			xlog.NewLogField("metric", metric),
			xlog.NewLogField("policy", policy),
		).Info("aggregated metric")
		return nil
	}
	processorOpts = processorOpts.SetMetricWithPolicyFn(metricWithPolicyfn)
	processor := processor.NewAggregatedMetricProcessor(processorOpts)

	// Creating the aggregator
	aggregatorOpts := aggregator.NewOptions().SetFlushFn(processor.Add)
	aggregator := aggregator.NewAggregator(aggregatorOpts)

	// Creating the mgspack server options
	msgpackAddr := *msgpackAddrArg
	msgpackServerOpts := msgpackserver.NewOptions()
	httpAddr := *httpAddrArg
	httpServerOpts := httpserver.NewOptions()

	// Start listening
	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := serve.Serve(
			msgpackAddr,
			msgpackServerOpts,
			httpAddr,
			httpServerOpts,
			aggregator,
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
