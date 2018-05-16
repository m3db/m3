package dtests

import (
	"github.com/m3db/m3db/src/dbnode/tools/dtest/harness"

	"github.com/spf13/cobra"
)

var (
	replaceDownNodeTestCmd = &cobra.Command{
		Use:   "replace_down_node",
		Short: "Run a dtest where a node that is DOWN, is replaced from the cluster. Node is left DOWN.",
		Long: `
		Perform the following operations on the provided set of nodes:
		(1) Create a new cluster placement using all but one of the provided nodes.
		(2) Seed the nodes used in (1), with initial data on their respective file-systems.
		(3) Start the nodes from (1), and wait until they are bootstrapped.
		(4) Stop the process on any one node in the cluster.
		(5) The node from (4) is replaced with the unused node, in the cluster placement.
		(6) Wait until all the shards in the cluster placement are marked as available.
`,
		Example: `./dtest replace_down_node --m3db-build path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
		Run:     replaceDownNodeDTest,
	}
)

func replaceDownNodeDTest(cmd *cobra.Command, args []string) {
	if err := globalArgs.Validate(); err != nil {
		printUsage(cmd)
		return
	}

	logger := newLogger(cmd)
	dt := harness.New(globalArgs, logger)
	defer dt.Close()

	nodes := dt.Nodes()
	numNodes := len(nodes) - 1 // leaving spare to replace with
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

	// bring down and replace first node from the cluster
	replaceNode := setupNodes[0]
	logger.Infof("bringing node down: %v", replaceNode.String())
	panicIfErr(replaceNode.Stop(), "unable to bring node down")

	logger.Infof("replacing node: %v", replaceNode.String())
	newNodes, err := testCluster.ReplaceNode(replaceNode)
	panicIfErr(err, "unable to replace node")
	logger.Infof("replaced node: %s", replaceNode.ID())

	// start added nodes
	logger.Infof("starting replacement nodes: %+v", newNodes)
	for _, n := range newNodes {
		panicIfErr(n.Start(), "unable to start node")
	}

	// wait until all shards are marked available again
	logger.Infof("waiting till all shards are available")
	panicIfErr(dt.WaitUntilAllShardsAvailable(), "all shards not available")
	logger.Infof("all shards available!")
}
