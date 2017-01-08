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
	"github.com/m3db/m3aggregator/server"
)

const (
	gracefulShutdownTimeout = 10 * time.Second
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

	// Creating the aggregator
	// TODO(xichen): customize flush flunction
	aggOpts := aggregator.NewOptions()
	aggregator := aggregator.NewAggregator(aggOpts)

	// Creating the server
	listendAddr := *listenAddrArg
	serverOpts := server.NewOptions()
	log := serverOpts.InstrumentOptions().Logger()
	s := server.NewServer(listendAddr, aggregator, serverOpts)

	// Start listening
	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		closer, err := s.ListenAndServe()
		if err != nil {
			log.Fatalf("could not listen on %s: %v", listendAddr, err)
		}
		log.Infof("start listening on %s", listendAddr)

		// Wait for exit signal
		<-doneCh

		log.Debug("closing the server")
		closer.Close()

		log.Debug("server closed")
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
