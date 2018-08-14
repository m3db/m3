package dtests

import (
	"github.com/m3db/m3/src/cmd/tools/dtest/harness"

	"github.com/spf13/cobra"
)

var (
	replaceUpNodeTestCmd = &cobra.Command{
		Use:   "replace_up_node",
		Short: "Run a dtest where a node that is UP, is replaced from the cluster. Node is left UP.",
		Long: `
		Perform the following operations on the provided set of nodes:
		(1) Create a new cluster placement using all but one of the provided nodes.
		(2) Seed the nodes used in (1), with initial data on their respective file-systems.
		(3) Start the nodes from (1), and wait until they are bootstrapped.
		(4) One node in the cluster is replaced with the unused node, in the cluster placement.
		(5) The joining node's process is started.
		(6) Wait until all shards in the cluster placement are available.
`,
		Example: `./dtest replace_up_node --m3db-build  path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
		Run:     replaceUpNodeDTest,
	}
)

func replaceUpNodeDTest(cmd *cobra.Command, args []string) {
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

	// replace first node from the cluster
	logger.Infof("replacing node")
	replaceNode := setupNodes[0]
	newNodes, err := testCluster.ReplaceNode(replaceNode)
	panicIfErr(err, "unable to replace node")
	logger.Infof("replaced node: %s", replaceNode.ID())

	// start added nodes
	for _, n := range newNodes {
		panicIfErr(n.Start(), "unable to start node")
	}

	// wait until all shards are marked available again
	logger.Infof("waiting till all shards are available")
	panicIfErr(dt.WaitUntilAllShardsAvailable(), "all shards not available")
	logger.Infof("all shards available!")
}
