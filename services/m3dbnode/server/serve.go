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

package server

import (
	"errors"
	"fmt"
	"strings"

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

// DefaultShardingScheme creates a default sharding scheme.
func DefaultShardingScheme() (m3db.ShardScheme, error) {
	shards := uint32(1024)
	return sharding.NewShardScheme(0, shards-1, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % shards
	})
}

// DefaultClientOptions creates a default m3db client options.
func DefaultClientOptions(tchannelNodeAddr string, shardingScheme m3db.ShardScheme) (m3db.ClientOptions, error) {
	var localNodeAddr string
	if !strings.ContainsRune(tchannelNodeAddr, ':') {
		return nil, errors.New("tchannelthrift address does not specify port")
	}
	localNodeAddrComponents := strings.Split(tchannelNodeAddr, ":")
	localNodeAddr = fmt.Sprintf("127.0.0.1:%s", localNodeAddrComponents[len(localNodeAddrComponents)-1])

	hostShardSet := topology.NewHostShardSet(topology.NewHost(localNodeAddr), shardingScheme.All())
	topologyOptions := topology.NewStaticTopologyTypeOptions().
		ShardScheme(shardingScheme).
		Replicas(1).
		HostShardSets([]m3db.HostShardSet{hostShardSet})
	return client.NewOptions().TopologyType(topology.NewStaticTopologyType(topologyOptions)), nil
}

// Serve starts up the tchannel server as well as the http server.
func Serve(
	httpClusterAddr string,
	tchannelClusterAddr string,
	httpNodeAddr string,
	tchannelNodeAddr string,
	clientOpts m3db.ClientOptions,
	dbOpts m3db.DatabaseOptions,
	doneCh chan struct{},
) error {
	log := dbOpts.GetLogger()
	shardingScheme := clientOpts.GetTopologyType().Options().GetShardScheme()
	db := storage.NewDatabase(shardingScheme.All(), dbOpts)
	if err := db.Open(); err != nil {
		return fmt.Errorf("could not open database: %v", err)
	}
	defer db.Close()

	client, err := client.NewClient(clientOpts)
	if err != nil {
		return fmt.Errorf("could not create cluster client: %v", err)
	}

	nativeNodeClose, err := ttnode.NewServer(db, tchannelNodeAddr, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open tchannelthrift interface %s: %v", tchannelNodeAddr, err)
	}
	defer nativeNodeClose()
	log.Infof("node tchannelthrift: listening on %v", tchannelNodeAddr)

	httpjsonNodeClose, err := hjnode.NewServer(db, httpNodeAddr, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open httpjson interface %s: %v", httpNodeAddr, err)
	}
	defer httpjsonNodeClose()
	log.Infof("node httpjson: listening on %v", httpNodeAddr)

	nativeClusterClose, err := ttcluster.NewServer(client, tchannelClusterAddr, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open tchannelthrift interface %s: %v", tchannelClusterAddr, err)
	}
	defer nativeClusterClose()
	log.Infof("cluster tchannelthrift: listening on %v", tchannelClusterAddr)
	httpjsonClusterClose, err := hjcluster.NewServer(client, httpClusterAddr, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open httpjson interface %s: %v", httpClusterAddr, err)
	}
	defer httpjsonClusterClose()
	log.Infof("cluster httpjson: listening on %v", httpClusterAddr)

	if err := db.Bootstrap(); err != nil {
		log.Errorf("bootstrapping database encountered error(s): %v", err)
	}
	log.Debug("bootstrapped")

	<-doneCh
	log.Debug("server exiting")

	return nil
}
