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

	"github.com/m3db/m3/src/m3nsch/coordinator"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	statusCmd = &cobra.Command{
		Use:   "status",
		Short: "Retrieves the status of agent processes",
		Long:  "Status retrieves a status from each of the described agent processes",
		Run:   statusExec,
		Example: `# Get status from the various agents:
./m3nsch_client -e "<agent1_host:agent1_port>,...,<agentN_host>:<agentN_port>" status`,
	}
)

func statusExec(_ *cobra.Command, _ []string) {
	if !gFlags.isValid() {
		log.Fatalf("unable to execute, invalid flags\n%s", M3nschCmd.UsageString())
	}

	var (
		iopts      = instrument.NewOptions()
		logger     = iopts.Logger()
		mOpts      = coordinator.NewOptions(iopts)
		coord, err = coordinator.New(mOpts, gFlags.endpoints)
	)

	if err != nil {
		logger.Fatal("unable to create coordinator", zap.Error(err))
	}
	defer coord.Teardown()

	statusMap, err := coord.Status()
	if err != nil {
		logger.Fatal("unable to retrieve status", zap.Error(err))
	}

	for endpoint, status := range statusMap {
		token := status.Token
		if token == "" {
			token = "<undefined>"
		}
		logger.Sugar().Infof("[%v] MaxQPS: %d, Status: %v, Token: %v, Workload: %+v",
			endpoint, status.MaxQPS, status.Status, token, status.Workload)
	}
}
