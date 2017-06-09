package dtests

import (
	"github.com/spf13/cobra"

	"github.com/m3db/m3db/tools/dtest/harness"
)

var seededBootstrapTestCmd = &cobra.Command{
	Use:   "seeded_bootstrap",
	Short: "Run a dtest where all the provided nodes are seeded with data, and bootstrapped",
	Long:  "",
	Example: `
TODO(prateek): write up`,
	Run: seededBootstrapDTest,
}

func seededBootstrapDTest(cmd *cobra.Command, args []string) {
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

	logger.Infof("setting up cluster with %d nodes", numNodes)
	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster")

	logger.Infof("seeding nodes with initial data")
	panicIfErr(dt.Seed(setupNodes), "unable to seed nodes")
	logger.Infof("seeded nodes")

	logger.Infof("starting cluster")
	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster")

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(nodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")
}
