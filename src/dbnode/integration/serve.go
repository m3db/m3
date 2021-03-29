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

package integration

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	hjcluster "github.com/m3db/m3/src/dbnode/network/server/httpjson/cluster"
	hjnode "github.com/m3db/m3/src/dbnode/network/server/httpjson/node"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	ttcluster "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/cluster"
	ttnode "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node"
	"github.com/m3db/m3/src/dbnode/server"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/topology"
	xretry "github.com/m3db/m3/src/x/retry"

	"go.uber.org/zap"
)

// newTestShardSet creates a default shard set
func newTestShardSet(numShards int) (sharding.ShardSet, error) {
	var ids []uint32
	for i := uint32(0); i < uint32(numShards); i++ {
		ids = append(ids, i)
	}

	shards := sharding.NewShards(ids, shard.Available)
	return sharding.NewShardSet(shards, sharding.DefaultHashFn(numShards))
}

// newTopologyInitializerForShardSet creates a default topology initializer for a shard set
func newTopologyInitializerForShardSet(
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

// defaultClientOptions creates a default m3db client options
func defaultClientOptions(initializer topology.Initializer) client.Options {
	return client.NewOptions().
		SetTopologyInitializer(initializer).
		// Default to zero retries to prevent tests from taking too long in situations where
		// errors are expected.
		SetWriteRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(0))).
		SetFetchRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(0)))
}

// openAndServe opens the database, starts up the RPC servers and bootstraps
// the database for serving traffic
func openAndServe(
	httpClusterAddr string,
	tchannelClusterAddr string,
	httpNodeAddr string,
	tchannelNodeAddr string,
	httpDebugAddr string,
	db storage.Database,
	client client.Client,
	opts storage.Options,
	serverStorageOpts server.StorageOptions,
	doneCh <-chan struct{},
) error {
	logger := opts.InstrumentOptions().Logger()
	if err := db.Open(); err != nil {
		return fmt.Errorf("could not open database: %v", err)
	}

	contextPool := opts.ContextPool()
	ttopts := tchannelthrift.NewOptions()
	service := ttnode.NewService(db, ttopts)
	nodeOpts := ttnode.NewOptions(nil)
	if fn := serverStorageOpts.TChanChannelFn; fn != nil {
		nodeOpts = nodeOpts.SetTChanChannelFn(fn)
	}
	if fn := serverStorageOpts.TChanNodeServerFn; fn != nil {
		nodeOpts = nodeOpts.SetTChanNodeServerFn(fn)
	}
	nativeNodeClose, err := ttnode.NewServer(service, tchannelNodeAddr, contextPool, nodeOpts).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open tchannelthrift interface %s: %v", tchannelNodeAddr, err)
	}
	defer nativeNodeClose()
	logger.Info("node tchannelthrift: listening", zap.String("address", tchannelNodeAddr))

	httpjsonNodeClose, err := hjnode.NewServer(service, httpNodeAddr, contextPool, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open httpjson interface %s: %v", httpNodeAddr, err)
	}
	defer httpjsonNodeClose()
	logger.Info("node httpjson: listening", zap.String("address", httpNodeAddr))

	nativeClusterClose, err := ttcluster.NewServer(client, tchannelClusterAddr, contextPool, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open tchannelthrift interface %s: %v", tchannelClusterAddr, err)
	}
	defer nativeClusterClose()
	logger.Info("cluster tchannelthrift: listening", zap.String("address", tchannelClusterAddr))

	httpjsonClusterClose, err := hjcluster.NewServer(client, httpClusterAddr, contextPool, nil).ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not open httpjson interface %s: %v", httpClusterAddr, err)
	}
	defer httpjsonClusterClose()
	logger.Info("cluster httpjson: listening", zap.String("address", httpClusterAddr))

	if httpDebugAddr != "" {
		go func() {
			if err := http.ListenAndServe(httpDebugAddr, nil); err != nil {
				logger.Warn("debug server could not listen", zap.String("address", httpDebugAddr), zap.Error(err))
			}
		}()
	}

	if err := db.Bootstrap(); err != nil {
		return fmt.Errorf("bootstrapping database encountered error: %v", err)
	}
	logger.Debug("bootstrapped")

	<-doneCh
	logger.Debug("server closing")

	return db.Terminate()
}
