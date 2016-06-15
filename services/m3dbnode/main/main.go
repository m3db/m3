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

	"github.com/m3db/m3db"
	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/services/m3dbnode/serve/httpjson"
	"github.com/m3db/m3db/services/m3dbnode/serve/tchannelthrift"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"

	"github.com/spaolacci/murmur3"
)

var (
	tchannelAddrArg = flag.String("tchanneladdr", "0.0.0.0:9000", "TChannel server address")
	httpAddrArg     = flag.String("httpaddr", "0.0.0.0:9001", "HTTP server address")
)

func main() {
	flag.Parse()

	if *tchannelAddrArg == "" || *httpAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	tchannelAddr := *tchannelAddrArg
	httpAddr := *httpAddrArg

	var opts memtsdb.DatabaseOptions
	opts = storage.NewDatabaseOptions().NewBootstrapFn(func() memtsdb.Bootstrap {
		return bootstrap.NewNoOpBootstrapProcess(opts)
	})

	log := opts.GetLogger()

	shards := uint32(1024)
	shardingScheme, err := sharding.NewShardScheme(0, shards-1, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % shards
	})
	if err != nil {
		log.Fatalf("could not create sharding scheme: %v", err)
	}

	db := storage.NewDatabase(shardingScheme.All(), opts)
	if err := db.Open(); err != nil {
		log.Fatalf("could not open database: %v", err)
	}
	defer db.Close()

	tchannelthriftClose, err := tchannelthrift.NewServer(db, tchannelAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open tchannelthrift interface: %v", err)
	}
	defer tchannelthriftClose()
	log.Infof("tchannelthrift: listening on %v", tchannelAddr)

	httpjsonClose, err := httpjson.NewServer(db, httpAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open httpjson interface: %v", err)
	}
	defer httpjsonClose()
	log.Infof("httpjson: listening on %v", httpAddr)

	if err := db.Bootstrap(time.Now()); err != nil {
		log.Fatalf("could not bootstrap database: %v", err)
	}
	log.Infof("bootstrapped")

	log.Fatalf("interrupt: %v", interrupt())
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
