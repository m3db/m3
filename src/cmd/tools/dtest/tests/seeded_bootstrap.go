// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package dtests

import (
	"github.com/m3db/m3/src/cmd/tools/dtest/harness"

	"github.com/spf13/cobra"
)

var seededBootstrapTestCmd = &cobra.Command{
	Use:   "seeded_bootstrap",
	Short: "Run a dtest where all the provided nodes are seeded with data, and bootstrapped",
	Long: `
	Perform the following operations on the provided set of nodes:
	(1) Create a new cluster placement using all of the provided nodes.
	(2) Seed the nodes used in (1), with initial data on their respective file-systems.
	(3) Start the nodes from (1), and wait until they are bootstrapped.
`,
	Example: `./dtest seeded_bootstrap --m3db-build path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
	Run:     seededBootstrapDTest,
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
	panicIfErr(dt.WaitUntilAllBootstrapped(setupNodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")
}
