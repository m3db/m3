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

	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/storage"
)

var (
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

	httpClusterAddr := *httpClusterAddrArg
	tchannelClusterAddr := *tchannelClusterAddrArg
	httpNodeAddr := *httpNodeAddrArg
	tchannelNodeAddr := *tchannelNodeAddrArg

	var opts m3db.DatabaseOptions
	opts = storage.NewDatabaseOptions().NewBootstrapFn(func() m3db.Bootstrap {
		return bootstrap.NewNoOpBootstrapProcess(opts)
	})

	log := opts.GetLogger()
	shardingScheme, err := server.DefaultShardingScheme()
	if err != nil {
		log.Fatalf("could not create sharding scheme: %v", err)
	}

	doneCh := make(chan struct{})
	go func() {
		if err := server.Serve(
			httpClusterAddr,
			tchannelClusterAddr,
			httpNodeAddr,
			tchannelNodeAddr,
			shardingScheme,
			opts,
			doneCh,
		); err != nil {
			log.Fatalf("serve error: %v", err)
		}
	}()

	log.Fatalf("interrupt: %v", interrupt())
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
