// Copyright (c) 2018 Uber Technologies, Inc.
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

package dtests

import (
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/cmd/tools/dtest/harness"
	xclock "github.com/m3db/m3/src/x/clock"

	"github.com/spf13/cobra"
)

var (
	addUpNodeRemoveTestCmd = &cobra.Command{
		Use:   "add_up_node_remove",
		Short: "Run a dtest where a node that is UP, is added to the cluster. Node is removed as it begins bootstrapping.",
		Long: `
		Perform the following operations on the provided set of nodes:
		(1) Create a new cluster placement using all but one of the provided nodes.
		(2) Seed the nodes used in (1), with initial data on their respective file-systems.
		(3) Start the nodes from (1), and wait until they are bootstrapped.
		(4) Start the one unused node's process.
		(5) Add the node from (4) to the cluster placement.
		(6) Wait until any shard on the node is marked as available.
		(7) Remove the node from the cluster placement.
`,
		Example: `./dtest add_up_node_remove --m3db-build path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
		Run:     addUpNodeRemoveDTest,
	}
)

func addUpNodeRemoveDTest(cmd *cobra.Command, args []string) {
	if err := globalArgs.Validate(); err != nil {
		printUsage(cmd)
		return
	}

	rawLogger := newLogger(cmd)
	defer rawLogger.Sync()
	logger := rawLogger.Sugar()

	dt := harness.New(globalArgs, rawLogger)
	defer dt.Close()

	nodes := dt.Nodes()
	numNodes := len(nodes) - 1 // leaving one spare
	testCluster := dt.Cluster()

	logger.Infof("setting up cluster")
	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	logger.Infof("seeding nodes with initial data")
	panicIfErr(dt.Seed(setupNodes), "unable to seed nodes")
	logger.Infof("seeded nodes")

	logger.Infof("starting cluster")
	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(setupNodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")

	// get a spare, ensure it's up and add to the cluster
	logger.Infof("adding spare to the cluster")
	spares := testCluster.SpareNodes()
	panicIf(len(spares) < 1, "no spares to add to the cluster")
	spare := spares[0]

	// start node
	logger.Infof("starting new node: %v", spare.ID())
	panicIfErr(spare.Start(), "unable to start node")
	logger.Infof("started node")

	// add to placement
	logger.Infof("adding node")
	panicIfErr(testCluster.AddSpecifiedNode(spare), "unable to add node")
	logger.Infof("added node")

	// NB(prateek): ideally we'd like to wait until the node begins bootstrapping, but we don't
	// have a way to capture that node status. The rpc endpoint in m3dbnode only captures bootstrap
	// status at the database level, and m3kv only captures state once a shard is marked as bootstrapped.
	// So here we wait until any shard is marked as bootstrapped before continuing.

	// wait until any shard is bootstrapped (i.e. marked available on new node)
	logger.Infof("waiting till any shards are bootstrapped on new node")
	timeout := dt.BootstrapTimeout()
	anyBootstrapped := xclock.WaitUntil(func() bool { return dt.AnyInstanceShardHasState(spare.ID(), shard.Available) }, timeout)
	panicIf(!anyBootstrapped, "all shards not available")

	// remove the node once it has a shard available
	logger.Infof("node has at least 1 shard available. removing node")
	panicIfErr(testCluster.RemoveNode(spare), "unable to remove node")
	logger.Infof("removed node")
}
