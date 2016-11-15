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
	hjcluster "github.com/m3db/m3db/network/server/httpjson/cluster"
	hjnode "github.com/m3db/m3db/network/server/httpjson/node"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	ttcluster "github.com/m3db/m3db/network/server/tchannelthrift/cluster"
	ttnode "github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/cluster"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
)

const defaultNamespaceName = "metrics"

// DefaultShardSet creates a default shard set
func DefaultShardSet() (sharding.ShardSet, error) {
	shardsLen := uint32(1024)
	var shards []uint32
	for i := uint32(0); i < shardsLen; i++ {
		shards = append(shards, i)
	}

	return sharding.NewShardSet(shards, sharding.DefaultHashGen(1024))
}

// DefaultTopologyInitializer creates a default topology initializer
func DefaultTopologyInitializer(
	hostID string,
	tchannelNodeAddr string,
) (topology.Initializer, error) {
	shardSet, err := DefaultShardSet()
	if err != nil {
		return nil, err
	}

	return DefaultTopologyInitializerForShardSet(hostID, tchannelNodeAddr, shardSet)
}

// DefaultTopologyInitializerForShardSet creates a default topology initializer for a shard set
func DefaultTopologyInitializerForShardSet(
	hostID string,
	tchannelNodeAddr string,
	shardSet sharding.ShardSet,
) (topology.Initializer, error) {
	var localNodeAddr string
	if !strings.ContainsRune(tchannelNodeAddr, ':') {
		return nil, errors.New("tchannelthrift address does not specify port")
	}
	localNodeAddrComponents := strings.Split(tchannelNodeAddr, ":")
	localNodeAddr = fmt.Sprintf("127.0.0.1:%s", localNodeAddrComponents[len(localNodeAddrComponents)-1])

	hostShardSet := topology.NewHostShardSet(topology.NewHost(hostID, localNodeAddr), shardSet)
	staticOptions := topology.NewStaticOptions().
		SetShardSet(shardSet).
		SetReplicas(1).
		SetHostShardSets([]topology.HostShardSet{hostShardSet})

	return topology.NewStaticInitializer(staticOptions), nil
}

// DefaultNamespaces creates a list of default namespaces
func DefaultNamespaces() []namespace.Metadata {
	opts := namespace.NewOptions()
	return []namespace.Metadata{
		namespace.NewMetadata(ts.StringID(defaultNamespaceName), opts),
	}
}

// DefaultClientOptions creates a default m3db client options
func DefaultClientOptions(initializer topology.Initializer) client.Options {
	return client.NewOptions().SetTopologyInitializer(initializer)
}

// Serve starts up the tchannel server as well as the http server
func Serve(
	httpClusterAddr string,
	tchannelClusterAddr string,
	httpNodeAddr string,
	tchannelNodeAddr string,
	namespaces []namespace.Metadata,
	hostID string,
	topoInit topology.Initializer,
	client client.Client,
	opts storage.Options,
	doneCh chan struct{},
) error {
	log := opts.InstrumentOptions().Logger()
	db, err := cluster.NewDatabase(namespaces, hostID, topoInit, opts)
	if err != nil {
		return err
	}
	if err := db.Open(); err != nil {
		return fmt.Errorf("could not open database: %v", err)
	}

	contextPool := opts.ContextPool()
	ttopts := tchannelthrift.NewOptions()
	nativeNodeClose, err := ttnode.NewServer(db, tchannelNodeAddr, contextPool, nil, ttopts).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open tchannelthrift interface %s: %v", tchannelNodeAddr, err)
	}
	defer nativeNodeClose()
	log.Infof("node tchannelthrift: listening on %v", tchannelNodeAddr)

	httpjsonNodeClose, err := hjnode.NewServer(db, httpNodeAddr, contextPool, nil, ttopts).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open httpjson interface %s: %v", httpNodeAddr, err)
	}
	defer httpjsonNodeClose()
	log.Infof("node httpjson: listening on %v", httpNodeAddr)

	nativeClusterClose, err := ttcluster.NewServer(client, tchannelClusterAddr, contextPool, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open tchannelthrift interface %s: %v", tchannelClusterAddr, err)
	}
	defer nativeClusterClose()
	log.Infof("cluster tchannelthrift: listening on %v", tchannelClusterAddr)

	httpjsonClusterClose, err := hjcluster.NewServer(client, httpClusterAddr, contextPool, nil).ListenAndServe()
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
	log.Debug("server closing")

	return db.Close()
}
