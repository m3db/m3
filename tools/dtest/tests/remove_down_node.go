package dtests

import (
	"github.com/spf13/cobra"

	"github.com/m3db/m3db/tools/dtest/harness"
)

var removeDownNodeTestCmd = &cobra.Command{
	Use:   "remove_down_node",
	Short: "Run a dtest where a node that is down, is removed from the cluster. Node is left down.",
	Long:  "",
	Example: `
TODO(prateek): write up`,
	Run: removeDownNodeDTest,
}

func removeDownNodeDTest(cmd *cobra.Command, args []string) {
	if err := globalArgs.Validate(); err != nil {
		printUsage(cmd)
		return
	}

	logger := newLogger(cmd)
	dt := harness.New(globalArgs, logger)
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
	panicIfErr(dt.WaitUntilAllBootstrapped(nodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")

	// stop first node in the cluster
	removeNode := setupNodes[0]
	logger.Infof("bringing node down: %v", removeNode.String())
	panicIfErr(removeNode.Stop(), "unable to stop node")
	logger.Infof("node is now down")

	// remove from cluster
	logger.Infof("removing node")
	panicIfErr(testCluster.RemoveNode(removeNode), "unable to remove node")
	logger.Infof("removed node")

	// wait until all shards are marked available again
	logger.Infof("waiting till all shards are available")
	panicIfErr(dt.WaitUntilAllShardsAvailable(), "all shards not available")
	logger.Infof("all shards available!")
}
