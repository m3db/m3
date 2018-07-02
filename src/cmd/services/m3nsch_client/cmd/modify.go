// Copyright (c) 2017 Uber Technologies, Inc.
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

package cmd

import (
	"log"

	"github.com/m3db/m3db/src/m3nsch/coordinator"
	"github.com/m3db/m3x/instrument"

	"github.com/spf13/cobra"
)

var (
	modifyWorkload cliWorkload

	modifyCmd = &cobra.Command{
		Use:   "modify",
		Short: "modify the workload used in load generation",
		Long:  "Modify changes the worload used during load generation on each agent process",
		Run:   modifyExec,
		Example: `# Modify agents with explicit workload:
./m3nsch_client --endpoints "<agent1_host:agent1_port>,...,<agentN_host>:<agentN_port>" modify \
	--metric-prefix m3nsch_metric_prefix \
	--namespace metrics                  \
	--cardinality 1000000                \
	--ingress-qps 200000                 \`,
	}
)

func init() {
	flags := modifyCmd.Flags()
	registerWorkloadFlags(flags, &modifyWorkload)
}

func modifyExec(cmd *cobra.Command, _ []string) {
	if !gFlags.isValid() {
		log.Fatalf("Invalid flags\n%s", cmd.UsageString())
	}
	if err := modifyWorkload.validate(); err != nil {
		log.Fatalf("Invalid flags: %v\n%s", err, cmd.UsageString())
	}

	var (
		workload   = modifyWorkload.toM3nschWorkload()
		iopts      = instrument.NewOptions()
		logger     = iopts.Logger()
		mOpts      = coordinator.NewOptions(iopts)
		coord, err = coordinator.New(mOpts, gFlags.endpoints)
	)

	if err != nil {
		logger.Fatalf("unable to create coord: %v", err)
	}
	defer coord.Teardown()

	err = coord.SetWorkload(workload)
	if err != nil {
		logger.Fatalf("unable to modify workload: %v", err)
	}

	logger.Infof("workload modified successfully!")
}
