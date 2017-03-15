package cmd

import (
	"log"

	"github.com/m3db/m3nsch/coordinator"

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
