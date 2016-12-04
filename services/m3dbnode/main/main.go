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
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/cluster"
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
	fileOpOpts := storageOpts.FileOpOptions().
		SetRetentionOptions(storageOpts.RetentionOptions())
	storageOpts = storageOpts.
		SetFileOpOptions(fileOpOpts)

	log := storageOpts.InstrumentOptions().Logger()
	topoInit, err := server.DefaultTopologyInitializer(id, tchannelNodeAddr)
	if err != nil {
		log.Fatalf("could not create topology initializer: %v", err)
	}

	if id == "" {
		id, err = os.Hostname()
		if err != nil {
			log.Fatalf("could not get hostname: %v", err)
		}
	}

	cli, err := client.NewClient(server.DefaultClientOptions(topoInit))
	if err != nil {
		log.Fatalf("could not create cluster client: %v", err)
	}

	repairOpts := storageOpts.RepairOptions().
		SetAdminClient(cli.(client.AdminClient))
	storageOpts = storageOpts.
		SetRepairOptions(repairOpts)

	namespaces := server.DefaultNamespaces()

	db, err := cluster.NewDatabase(namespaces, id, topoInit, storageOpts)
	if err != nil {
		log.Fatalf("could not create database: %v", err)
	}
	doneCh := make(chan struct{}, 1)
	closedCh := make(chan struct{}, 1)
	go func() {
		if err := server.OpenAndServe(
			httpClusterAddr, tchannelClusterAddr,
			httpNodeAddr, tchannelNodeAddr,
			db, cli, storageOpts, doneCh,
		); err != nil {
			log.Fatalf("server fatal error: %v", err)
		}

		// Server is closed
		closedCh <- struct{}{}
	}()

	// Handle interrupt
	log.Warnf("interrupt: %v", interrupt())

	// Attempt graceful server close
	cleanCloseTimeout := 10 * time.Second

	select {
	case doneCh <- struct{}{}:
	default:
		// Currently bootstrapping, cannot send close clean signal
		closedCh <- struct{}{}
	}

	// Wait then close or hard close
	select {
	case <-closedCh:
		log.Infof("server closed clean")
	case <-time.After(cleanCloseTimeout):
		log.Errorf("server closed due to %s timeout", cleanCloseTimeout.String())
	}
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
