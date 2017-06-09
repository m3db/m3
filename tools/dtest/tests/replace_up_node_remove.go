package dtests

import (
	"github.com/spf13/cobra"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/tools/dtest/harness"
	"github.com/m3db/m3db/x/m3em/convert"
	xclock "github.com/m3db/m3x/clock"
)

var (
	replaceUpNodeRemoveTestCmd = &cobra.Command{
		Use:   "replace_up_node_remove",
		Short: "Run a dtest where a node that is UP is replaced from the cluster. The replacing Node is removed as it begins bootstrapping.",
		Long:  "",
		Example: `
TODO(prateek): write up`,
		Run: replaceUpNodeRemoveDTest,
	}
)

func replaceUpNodeRemoveDTest(cmd *cobra.Command, args []string) {
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

	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	m3dbnodes, err := convert.AsM3DBNodes(setupNodes)
	panicIfErr(err, "unable to cast to m3dbnodes")

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(m3dbnodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")

	// pick first node from cluster
	nodeToReplace := m3dbnodes[0]
	logger.Infof("replacing node: %v", nodeToReplace.ID())
	replacementNodes, err := testCluster.ReplaceNode(nodeToReplace)
	panicIfErr(err, "unable to replace node")
	panicIf(len(replacementNodes) < 1, "no replacement nodes returned")
	logger.Infof("replaced node with: %+v", replacementNodes)

	logger.Infof("starting replacement nodes")
	// starting replacement nodes
	for _, n := range replacementNodes {
		panicIfErr(n.Start(), "unable to start node")
	}
	logger.Infof("started replacement nodes")

	// NB(prateek): ideally we'd like to wait until the node begins bootstrapping, but we don't
	// have a way to capture that node status. The rpc endpoint in m3dbnode only captures bootstrap
	// status at the database level, and m3kv only captures state once a shard is marked as bootstrapped.
	// So here we wait until any shard is marked as bootstrapped before continuing.

	// wait until any shard is bootstrapped (i.e. marked available on new node)
	replacementNode := replacementNodes[0]
	logger.Infof("waiting till any shards are bootstrapped on node: %v", replacementNode.ID())
	timeout := dt.BootstrapTimeout() / 10
	anyBootstrapped := xclock.WaitUntil(func() bool { return dt.AnyInstanceShardHasState(replacementNode.ID(), shard.Available) }, timeout)
	panicIf(!anyBootstrapped, "all shards not available")

	// remove the node once it has a shard available
	logger.Infof("node has at least 1 shard available. removing replacement nodes")
	for _, n := range replacementNodes {
		panicIfErr(testCluster.RemoveNode(n), "unable to remove node")
	}
	logger.Infof("removed replacement nodes")
}
