package dtests

import (
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/tools/dtest/harness"
	"github.com/m3db/m3db/tools/dtest/util"
	"github.com/m3db/m3db/tools/dtest/util/bootstrap"
	"github.com/m3db/m3db/ts"
	m3emnode "github.com/m3db/m3db/x/m3em/node"
	"github.com/m3db/m3em/node"
	"github.com/m3db/m3x/instrument"
)

var seededBootstrapTestCmd = &cobra.Command{
	Use:   "seeded_bootstrap",
	Short: "Run a dtest where all the provided nodes are seeded with data, and bootstrapped",
	Long:  "",
	Example: `
TODO(prateek): write up`,
	Run: seededBootstrapDTest,
}

const agentM3DBDataDir = "m3db-data/data"

func seededBootstrapDTest(cmd *cobra.Command, args []string) {
	if err := globalArgs.Validate(); err != nil {
		printUsage(cmd)
		return
	}

	var (
		logger       = newLogger(cmd)
		iopts        = instrument.NewOptions().SetLogger(logger)
		generateOpts = generate.NewOptions().
				SetRetentionPeriod(24 * time.Hour).
				SetBlockSize(2 * time.Hour)

		bootstrapDataOpts = bootstrap.NewOptions().
					SetInstrumentOptions(iopts).
					SetGenerateOptions(generateOpts)

		outputNamespace = ts.StringID("metrics")
		shardNum        = uint32(777)
		generator       = bootstrap.NewGenerator(bootstrapDataOpts)
	)
	logger.Infof("generating data to bootstrap with")
	err := generator.Generate(outputNamespace, shardNum)
	if err != nil {
		logger.Fatalf("unable to generate data: %v", err)
	}
	logger.Infof("generated data")
	// TODO(prateek): set filepathprefix; cleanup locally generated data

	generatedDataPath := path.Join(generateOpts.FilePathPrefix(), "data")
	fakeShardDir := newShardDir(generatedDataPath, outputNamespace, shardNum)
	localFiles, err := ioutil.ReadDir(fakeShardDir)
	panicIfErr(err, "unable to list local shard directory")

	dt := harness.New(globalArgs, logger)
	co := dt.ClusterOptions()
	defer dt.Close()

	nodes := dt.Nodes()
	numNodes := len(nodes)
	testCluster := dt.Cluster()

	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	// transfer the generated data to the remote hosts
	var (
		placement   = testCluster.Placement()
		concurrency = co.NodeConcurrency()
		timeout     = co.NodeOperationTimeout()
	)
	transferDataExecutor := node.NewConcurrentExecutor(setupNodes, concurrency, timeout, func(n node.ServiceNode) error {
		for _, file := range localFiles {
			base := path.Base(file.Name())
			srcPath := path.Join(fakeShardDir, base)
			paths := generatePaths(placement, n, outputNamespace, base)
			logger.Debugf("transferring %s to host %s, at paths: %v", base, n.ID(), paths)
			if err := n.TransferLocalFile(srcPath, paths, true); err != nil {
				return err
			}
		}
		return nil
	})
	panicIfErr(transferDataExecutor.Run(), "unable to transfer generated data")

	// startup hosts once the transfer is done
	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	watcher := util.NewNodesWatcher(nodes, logger, defaultBootstrapStatusReportingInterval)
	allBootstrapped := watcher.WaitUntilAll(m3emnode.Node.Bootstrapped, dt.BootstrapTimeout())
	panicIf(!allBootstrapped, fmt.Sprintf("unable to bootstrap all nodes, err = %v", watcher.PendingAsError()))
	logger.Infof("all nodes bootstrapped successfully!")
}

func newShardDir(prefix string, ns ts.ID, shard uint32) string {
	return path.Join(prefix, ns.String(), strconv.FormatUint(uint64(shard), 10))
}

func generatePaths(placement services.ServicePlacement, n node.ServiceNode, ns ts.ID, file string) []string {
	paths := []string{}
	pi, ok := placement.Instance(n.ID())
	if !ok {
		return paths
	}
	shards := pi.Shards().AllIDs()
	for _, s := range shards {
		paths = append(paths, path.Join(newShardDir(agentM3DBDataDir, ns, s), file))
	}
	return paths
}
