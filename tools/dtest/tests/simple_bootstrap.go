package dtests

import (
	"github.com/spf13/cobra"

	"github.com/m3db/m3db/tools/dtest/harness"
)

var simpleBootstrapTestCmd = &cobra.Command{
	Use:   "simple",
	Short: "Run a dtest where all the provided nodes are configured & bootstrapped",
	Long:  "",
	Example: `
TODO(prateek): write up`,
	Run: simpleBootstrapDTest,
}

func simpleBootstrapDTest(cmd *cobra.Command, args []string) {
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

	_, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(nodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")
}
