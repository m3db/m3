package dtests

import (
	"github.com/m3db/m3db/src/cmd/tools/dtest/harness"
	"github.com/m3db/m3em/node"

	"github.com/spf13/cobra"
)

var (
	addDownNodeAndBringUpTestCmd = &cobra.Command{
		Use:   "add_down_node_bring_up",
		Short: "Run a dtest where a node that is DOWN, is added to the cluster. Node is then brought up.",
		Long: `
		Perform the following operations on the provided set of nodes:
		(1) Create a new cluster placement using all but one of the provided nodes.
		(2) Seed the nodes used in (1), with initial data on their respective file-systems.
		(3) Start the the nodes from (1), and wait until they are bootstrapped.
		(4) Add the one unused node to the cluster placement, and start it's process.
		(5) Wait until all the shards in the cluster placement are marked as available.
`,
		Example: `./dtest add_down_node_bring_up --m3db-build path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
		Run:     addDownNodeAndBringUpDTest,
	}
)

func addDownNodeAndBringUpDTest(cmd *cobra.Command, args []string) {
	if err := globalArgs.Validate(); err != nil {
		printUsage(cmd)
		return
	}
	logger := newLogger(cmd)
	dt := harness.New(globalArgs, logger)
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

	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(setupNodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")

	// get a spare, ensure it's down and add to the cluster
	logger.Infof("adding spare to the cluster")
	spares := testCluster.SpareNodes()
	panicIf(len(spares) < 1, "no spares to add to the cluster")
	spare := spares[0]
	panicIf(spare.Status() != node.StatusSetup, "expected node to be setup")
	panicIfErr(testCluster.AddSpecifiedNode(spare), "unable to add node")
	logger.Infof("added node: %v", spare.String())

	// start added nodes
	logger.Infof("starting added node")
	panicIfErr(spare.Start(), "unable to start node")
	logger.Infof("started added node")

	// wait until all shards are marked available again
	logger.Infof("waiting till all shards are available")
	panicIfErr(dt.WaitUntilAllShardsAvailable(), "all shards not available")
	logger.Infof("all shards available!")
}
