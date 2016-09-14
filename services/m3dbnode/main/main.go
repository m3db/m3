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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/storage"
)

var (
	idArg                  = flag.String("id", "", "Node host ID")
	httpClusterAddrArg     = flag.String("clusterhttpaddr", "0.0.0.0:9000", "Cluster HTTP server address")
	tchannelClusterAddrArg = flag.String("clustertchanneladdr", "0.0.0.0:9001", "Cluster TChannel server address")
	httpNodeAddrArg        = flag.String("nodehttpaddr", "0.0.0.0:9002", "Node HTTP server address")
	tchannelNodeAddrArg    = flag.String("nodetchanneladdr", "0.0.0.0:9003", "Node TChannel server address")
)

func main() {
	flag.Parse()

	if *httpClusterAddrArg == "" ||
		*tchannelClusterAddrArg == "" ||
		*httpNodeAddrArg == "" ||
		*tchannelNodeAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	id := *idArg
	httpClusterAddr := *httpClusterAddrArg
	tchannelClusterAddr := *tchannelClusterAddrArg
	httpNodeAddr := *httpNodeAddrArg
	tchannelNodeAddr := *tchannelNodeAddrArg

	storageOpts := storage.NewOptions()

	log := storageOpts.InstrumentOptions().Logger()
	shardSet, err := server.DefaultShardSet()
	if err != nil {
		log.Fatalf("could not create sharding scheme: %v", err)
	}

	if id == "" {
		id, err = os.Hostname()
		if err != nil {
			log.Fatalf("could not get hostname: %v", err)
		}
	}

	clientOpts, err := server.DefaultClientOptions(id, tchannelNodeAddr, shardSet)
	if err != nil {
		log.Fatalf("could not create client options: %v", err)
	}

	c, err := client.NewClient(clientOpts)
	if err != nil {
		log.Fatalf("could not create cluster client: %v", err)
	}

	storageOpts = storageOpts.SetRepairOptions(storageOpts.RepairOptions().SetAdminClient(c.(client.AdminClient)))

	namespaces := server.DefaultNamespaces()

	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := server.Serve(
			httpClusterAddr,
			tchannelClusterAddr,
			httpNodeAddr,
			tchannelNodeAddr,
			namespaces,
			c,
			storageOpts,
			doneCh,
		); err != nil {
			log.Fatalf("server fatal error: %v", err)
		}

		// Server is closed
		closedCh <- struct{}{}
	}()

	// Handle interrupt
	log.Infof("interrupt: %v", interrupt())

	// Attempt graceful server close
	doneCh <- struct{}{}
	<-closedCh
	log.Infof("server closed")
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
