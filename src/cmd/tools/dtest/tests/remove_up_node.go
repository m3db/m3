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
	"github.com/m3db/m3/src/cmd/tools/dtest/harness"

	"github.com/spf13/cobra"
)

var removeUpNodeTestCmd = &cobra.Command{
	Use:   "remove_up_node",
	Short: "Run a dtest where a node that is UP, is removed from the cluster. Node is left UP.",
	Long: `
	Perform the following operations on the provided set of nodes:
	(1) Create a new cluster placement using all the provided nodes.
	(2) Seed the nodes used in (1), with initial data on their respective file-systems.
	(3) Start the nodes from (1), and wait until they are bootstrapped.
	(4) Remove any one node from the cluster placement.
	(5) Wait until all the shards in the placement are marked as available.
`,
	Example: `./dtest remove_up_node --m3db-build path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
	Run:     removeUpNodeDTest,
}

func removeUpNodeDTest(cmd *cobra.Command, args []string) {
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
	numNodes := len(nodes)
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

	// remove first node from the cluster
	removeNode := setupNodes[0]
	panicIfErr(testCluster.RemoveNode(removeNode), "unable to remove node")
	logger.Infof("removed node: %v", removeNode.String())

	// wait until all shards are marked available again
	logger.Infof("waiting till all shards are available")
	panicIfErr(dt.WaitUntilAllShardsAvailable(), "all shards not available")
	logger.Infof("all shards available!")
}
