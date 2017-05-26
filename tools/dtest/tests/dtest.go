package dtests

import (
	"fmt"
	"os"
	"time"

	"github.com/m3db/m3db/tools/dtest/config"

	"github.com/spf13/cobra"
)

const defaultBootstrapStatusReportingInterval = time.Minute

var (
	// DTestCmd represents the base command when called without any subcommands
	DTestCmd = &cobra.Command{
		Use:   "dtest",
		Short: "Command line tool to execute m3db dtests",
	}

	globalCLIOpts = &config.CLIOpts{}
)

// Run executes the m3admin command.
func Run() {
	if err := DTestCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	DTestCmd.AddCommand(
		simpleBootstrapTestCmd,
		removeUpNodeTestCmd,
		replaceUpNodeTestCmd,
		replaceDownNodeTestCmd,
		addDownNodeAndBringUpTestCmd,
		removeDownNodeTestCmd,
		addUpNodeRemoveTestCmd,
		replaceUpNodeRemoveTestCmd,
	)

	globalCLIOpts.RegisterFlags(DTestCmd)
}

func printUsage(cmd *cobra.Command) {
	if err := cmd.Usage(); err != nil {
		panic(err)
	}
}

func panicIf(cond bool, msg string) {
	if cond {
		panic(msg)
	}
}

func panicIfErr(err error, msg string) {
	if err == nil {
		return
	}
	errStr := err.Error()
	panic(fmt.Errorf("%s: %s", msg, errStr))
}
