package dtests

import (
	"fmt"
	"os"

	"github.com/m3db/m3db/tools/dtest/harness"
	"github.com/m3db/m3db/tools/dtest/util"
	"github.com/m3db/m3db/x/m3em/convert"
	m3dbnode "github.com/m3db/m3db/x/m3em/node"

	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/spf13/cobra"
)

var (
	replaceDownNodeTestCmd = &cobra.Command{
		Use:   "replace_down_node",
		Short: "Run a dtest where a node that is DOWN, is replaced from the cluster. Node is left DOWN.",
		Long:  "",
		Example: `
TODO(prateek): write up`,
		Run: replaceDownNodeDTest,
	}
)

func replaceDownNodeDTest(cmd *cobra.Command, args []string) {
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
	numNodes := len(nodes) - 1 // leaving spare to replace with
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
	allAvailable := xclock.WaitUntil(dt.AllShardsAvailable, dt.BootstrapTimeout())
	panicIf(!allAvailable, "all shards not available")
	logger.Infof("all shards available!")
}
