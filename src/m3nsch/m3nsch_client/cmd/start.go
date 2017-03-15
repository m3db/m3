package cmd

import (
	"log"

	"github.com/m3db/m3nsch/coordinator"

	"github.com/m3db/m3x/instrument"
	"github.com/spf13/cobra"
)

var (
	startCmd = &cobra.Command{
		Use:   "start",
		Short: "start the load generation",
		Long:  "Start kicks off the load generation on each agent process",
		Run:   startExec,
		Example: `# Start the load generation process on various agents:
./m3nsch_client -e "<agent1_host:agent1_port>,...,<agentN_host>:<agentN_port>" start`,
	}
)

func startExec(_ *cobra.Command, _ []string) {
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
		logger.Fatalf("unable to create coordinator: %v", err)
	}
	defer coord.Teardown()

	err = coord.Start()
	if err != nil {
		logger.Fatalf("unable to start workload: %v", err)
	}

	logger.Infof("workload started!")
}
