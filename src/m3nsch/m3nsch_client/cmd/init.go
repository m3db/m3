package cmd

import (
	"fmt"
	"log"

	"github.com/m3db/m3nsch/coordinator"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/spf13/cobra"
)

var (
	// local flags
	localInitFlags initFlags

	// InitCmd represents the base command when called without any subcommands
	initCmd *cobra.Command

	defaultEnv  = "test"
	defaultZone = "sjc1"
)

func init() {
	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize agent processes",
		Long:  "Initialize agent processes with any resources required to execute workload",
		Run:   initExec,
		Example: `# Initialize agents with default workload:
./m3nsch_client -e "<agent1_host:agent1_port>,...,<agentN_host>:<agentN_port>" init -t any_string_breadcrumb

# Initialize agents with explicit workload:
./m3nsch_client --endpoints "<agent1_host:agent1_port>,...,<agentN_host>:<agentN_port>" init \
	--token any_string_breadcrumb        \
	--target-zone sjc1                   \
	--target-env prod                    \
	--metric-prefix m3nsch_metric_prefix \
	--namespace testmetrics              \
	--cardinality 1000000                \
	--ingress-qps 200000                 \`,
	}

	flags := initCmd.Flags()
	flags.StringVarP(&localInitFlags.token, "token", "t", "",
		`[required] unique identifier required for all subsequent interactions on this workload`)
	flags.BoolVarP(&localInitFlags.force, "force", "f", false,
		`force initialization, stop any running workload`)
	flags.StringVarP(&localInitFlags.targetZone, "target-zone", "z", defaultZone,
		`target zone for load test`)
	flags.StringVarP(&localInitFlags.targetEnv, "target-env", "v", defaultEnv,
		`target env for load test`)
	registerWorkloadFlags(flags, &localInitFlags.workload)
}

type initFlags struct {
	token      string
	force      bool
	workload   cliWorkload
	targetZone string
	targetEnv  string
}

func (f initFlags) validate() error {
	var multiErr xerrors.MultiError
	if f.token == "" {
		multiErr = multiErr.Add(fmt.Errorf("token is not set"))
	}
	if f.targetEnv == "" {
		multiErr = multiErr.Add(fmt.Errorf("target-env is not set"))
	}
	if f.targetZone == "" {
		multiErr = multiErr.Add(fmt.Errorf("target-zone is not set"))
	}
	if err := f.workload.validate(); err != nil {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}

func initExec(cmd *cobra.Command, _ []string) {
	if !gFlags.isValid() {
		log.Fatalf("Invalid flags: %v", M3nschCmd.UsageString())
	}
	if err := localInitFlags.validate(); err != nil {
		log.Fatalf("Invalid flags: %v\n%s", err, cmd.UsageString())
	}

	var (
		workload   = localInitFlags.workload.toM3nschWorkload()
		iopts      = instrument.NewOptions()
		logger     = iopts.Logger()
		mOpts      = coordinator.NewOptions(iopts)
		coord, err = coordinator.New(mOpts, gFlags.endpoints)
	)

	if err != nil {
		logger.Fatalf("unable to create coord: %v", err)
	}
	defer coord.Teardown()

	err = coord.Init(localInitFlags.token, workload, localInitFlags.force,
		localInitFlags.targetZone, localInitFlags.targetEnv)

	if err != nil {
		logger.Fatalf("unable to initialize: %v", err)
	}
}
