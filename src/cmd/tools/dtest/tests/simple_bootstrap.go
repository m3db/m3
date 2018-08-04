package dtests

import (
	"github.com/m3db/m3/src/cmd/tools/dtest/harness"

	"github.com/spf13/cobra"
)

var simpleBootstrapTestCmd = &cobra.Command{
	Use:   "simple",
	Short: "Run a dtest where all the provided nodes are configured & bootstrapped",
	Long: `
	Perform the following operations on the provided set of nodes:
	(1) Create a new cluster placement using all of the provided nodes.
	(2) The nodes from (1) are started, and wait until they are bootstrapped.
`,
	Example: `./dtest simple --m3db-build  path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
	Run:     simpleBootstrapDTest,
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

	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(setupNodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")
}
