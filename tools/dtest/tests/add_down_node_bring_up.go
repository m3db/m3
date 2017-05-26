package dtests

import (
	"fmt"
	"os"

	"github.com/m3db/m3db/tools/dtest/harness"
	"github.com/m3db/m3db/tools/dtest/util"
	"github.com/m3db/m3db/x/m3em/convert"
	m3dbnode "github.com/m3db/m3db/x/m3em/node"

	"github.com/m3db/m3em/node"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/spf13/cobra"
)

var (
	addDownNodeAndBringUpTestCmd = &cobra.Command{
		Use:   "add_down_node_bring_up",
		Short: "Run a dtest where a node that is DOWN, is added to the cluster. Node is then brought up.",
		Long:  "",
		Example: `
TODO(prateek): write up`,
		Run: addDownNodeAndBringUpDTest,
	}
)

func addDownNodeAndBringUpDTest(cmd *cobra.Command, args []string) {
	if err := globalCLIOpts.Validate(); err != nil {
		printUsage(cmd)
		return
	}
	logger := xlog.NewLogger(os.Stdout)
	logger.Infof("============== %v  ==============", cmd.Name())

	dt := harness.New(globalCLIOpts, logger)
	dt.SetClusterOptions(dt.ClusterOptions().
		SetNodeListener(util.NewPanicListener()))
	defer dt.Close()

	nodes := dt.Nodes()
	numNodes := len(nodes) - 1 // leaving one spare
	testCluster := dt.Cluster()

	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	m3dbnodes, err := convert.AsM3DBNodes(setupNodes)
	panicIfErr(err, "unable to cast to m3dbnodes")

	logger.Infof("waiting until all instances are bootstrapped")
	watcher := util.NewM3DBNodesWatcher(m3dbnodes, logger, defaultBootstrapStatusReportingInterval)
	allBootstrapped := watcher.WaitUntilAll(m3dbnode.Node.Bootstrapped, dt.BootstrapTimeout())
	panicIf(!allBootstrapped, fmt.Sprintf("unable to bootstrap all nodes, err = %v", watcher.PendingAsError()))
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
	allAvailable := xclock.WaitUntil(dt.AllShardsAvailable, dt.BootstrapTimeout())
	panicIf(!allAvailable, "all shards not available")
	logger.Infof("all shards available!")
}
