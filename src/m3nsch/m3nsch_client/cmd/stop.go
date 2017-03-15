package cmd

import (
	"log"

	"github.com/m3db/m3nsch/coordinator"

	"github.com/m3db/m3x/instrument"
	"github.com/spf13/cobra"
)

var (
	stopCmd = &cobra.Command{
		Use:   "stop",
		Short: "stop the load generation",
		Long:  "Stop stops the load generation on each agent process",
		Run:   stopExec,
		Example: `# Stop the load generation process on various agents:
./m3nsch_client -e "<agent1_host:agent1_port>,...,<agentN_host>:<agentN_port>" stop`,
	}
)

func stopExec(_ *cobra.Command, _ []string) {
	if !gFlags.isValid() {
		log.Fatalf("unable to execute, invalid flags\n%s", M3nschCmd.UsageString())
	}

	var (
		iopts            = instrument.NewOptions()
		logger           = iopts.Logger()
		mOpts            = coordinator.NewOptions(iopts)
		coordinator, err = coordinator.New(mOpts, gFlags.endpoints)
	)

	if err != nil {
		logger.Fatalf("unable to create coordinator: %v", err)
	}
	defer coordinator.Teardown()

	err = coordinator.Stop()
	if err != nil {
		logger.Fatalf("unable to stop workload: %v", err)
	}

	logger.Infof("workload stopped!")
}
