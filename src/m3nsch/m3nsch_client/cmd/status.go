package cmd

import (
	"log"

	"github.com/m3db/m3nsch/coordinator"

	"github.com/m3db/m3x/instrument"
	"github.com/spf13/cobra"
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
		logger.Fatalf("unable to create coordinator: %v", err)
	}
	defer coord.Teardown()

	statusMap, err := coord.Status()
	if err != nil {
		logger.Fatalf("unable to retrieve status: %v", err)
	}

	for endpoint, status := range statusMap {
		token := status.Token
		if token == "" {
			token = "<undefined>"
		}
		logger.Infof("[%v] MaxQPS: %d, Status: %v, Token: %v, Workload: %+v",
			endpoint, status.MaxQPS, status.Status, token, status.Workload)
	}
}
