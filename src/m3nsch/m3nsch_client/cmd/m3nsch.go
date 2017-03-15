package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	// globalFlags
	gFlags globalFlags

	// M3nschCmd represents the base command when called without any subcommands
	M3nschCmd = &cobra.Command{
		Use:   "m3nsch_client",
		Short: "CLI interface to m3nsch - M3DB load generation",
	}
)

type globalFlags struct {
	endpoints []string
}

func (f globalFlags) isValid() bool {
	return len(f.endpoints) != 0
}

func init() {
	flags := M3nschCmd.PersistentFlags()
	flags.StringSliceVarP(&gFlags.endpoints, "endpoints", "e", []string{},
		`host:port for each of the agent process endpoints`)

	M3nschCmd.AddCommand(
		statusCmd,
		initCmd,
		startCmd,
		stopCmd,
		modifyCmd,
	)
}

// Run executes the m3nsch command.
func Run() {
	if err := M3nschCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
