package dtests

import (
	"fmt"
	"os"

	"github.com/m3db/m3db/tools/dtest/harness"
	"github.com/m3db/m3db/tools/dtest/util"

	m3dbnode "github.com/m3db/m3em/node/m3db"
	m3dbutil "github.com/m3db/m3em/node/m3db/util"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/spf13/cobra"
)

var removeUpNodeTestCmd = &cobra.Command{
	Use:   "remove_up_node",
	Short: "Run a dtest where a node that is UP, is removed from the cluster. Node is left UP.",
	Long:  "",
	Example: `
TODO(prateek): write up`,
	Run: removeUpNodeDTest,
}

func removeUpNodeDTest(cmd *cobra.Command, args []string) {
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
	numNodes := len(nodes)
	testCluster := dt.Cluster()

	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	watcher := m3dbutil.NewM3DBNodesWatcher(nodes)
	if ok := watcher.WaitUntilAll(m3dbnode.Node.Bootstrapped, dt.BootstrapTimeout()); !ok {
		panic(fmt.Errorf("unable to bootstrap all nodes, err = %v", watcher.PendingAsError()))
	}
	logger.Infof("all nodes bootstrapped successfully!")

	// remove first node from the cluster
	removeNode := setupNodes[0]
	panicIfErr(testCluster.RemoveNode(removeNode), "unable to remove node")
	logger.Infof("removed node: %v", removeNode.String())

	// wait until all shards are marked available again
	logger.Infof("waiting till all shards are available")
	if allAvailable := xclock.WaitUntil(dt.AllShardsAvailable, dt.BootstrapTimeout()); !allAvailable {
		panic("all shards not available")
	}
	logger.Infof("all shards available!")
}
