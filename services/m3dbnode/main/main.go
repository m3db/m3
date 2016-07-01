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
	"strings"
	"syscall"
	"time"

	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/interfaces/m3db"
	hjcluster "github.com/m3db/m3db/network/server/httpjson/cluster"
	hjnode "github.com/m3db/m3db/network/server/httpjson/node"
	ttcluster "github.com/m3db/m3db/network/server/tchannelthrift/cluster"
	ttnode "github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/topology"

	"github.com/spaolacci/murmur3"
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

	var localNodeAddr string
	if !strings.ContainsRune(tchannelNodeAddr, ':') {
		log.Fatalf("tchannelthrift address does not specify port")
	}
	localNodeAddrComponents := strings.Split(tchannelNodeAddr, ":")
	localNodeAddr = fmt.Sprintf("127.0.0.1:%s", localNodeAddrComponents[len(localNodeAddrComponents)-1])

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

	hostShardSet := topology.NewHostShardSet(topology.NewHost(localNodeAddr), shardingScheme.All())
	topologyOptions := topology.NewStaticTopologyTypeOptions().
		ShardScheme(shardingScheme).
		Replicas(1).
		HostShardSets([]m3db.HostShardSet{hostShardSet})
	clientOptions := client.NewOptions().TopologyType(topology.NewStaticTopologyType(topologyOptions))
	client, err := client.NewClient(clientOptions)
	if err != nil {
		log.Fatalf("could not create client for cluster service: %v", err)
	}

	nativeNodeClose, err := ttnode.NewServer(db, tchannelNodeAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open tchannelthrift interface %s: %v", tchannelNodeAddr, err)
	}
	defer nativeNodeClose()
	log.Infof("node tchannelthrift: listening on %v", tchannelNodeAddr)

	httpjsonNodeClose, err := hjnode.NewServer(db, httpNodeAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open httpjson interface %s: %v", httpNodeAddr, err)
	}
	defer httpjsonNodeClose()
	log.Infof("node httpjson: listening on %v", httpNodeAddr)

	nativeClusterClose, err := ttcluster.NewServer(client, tchannelClusterAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open tchannelthrift interface %s: %v", tchannelClusterAddr, err)
	}
	defer nativeClusterClose()
	log.Infof("cluster tchannelthrift: listening on %v", tchannelClusterAddr)
	httpjsonClusterClose, err := hjcluster.NewServer(client, httpClusterAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open httpjson interface %s: %v", httpClusterAddr, err)
	}
	defer httpjsonClusterClose()
	log.Infof("cluster httpjson: listening on %v", httpClusterAddr)

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
