package dtests

import (
	"fmt"
	"os"

	"github.com/m3db/m3db/src/cmd/tools/dtest/config"
	xlog "github.com/m3db/m3x/log"

	"github.com/spf13/cobra"
)

var (
	// DTestCmd represents the base command when called without any subcommands
	DTestCmd = &cobra.Command{
		Use:   "dtest",
		Short: "Command line tool to execute m3db dtests",
	}

	globalArgs = &config.Args{}
)

// Run executes the dtest command.
func Run() {
	if err := DTestCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	DTestCmd.AddCommand(
		seededBootstrapTestCmd,
		simpleBootstrapTestCmd,
		removeUpNodeTestCmd,
		replaceUpNodeTestCmd,
		replaceDownNodeTestCmd,
		addDownNodeAndBringUpTestCmd,
		removeDownNodeTestCmd,
		addUpNodeRemoveTestCmd,
		replaceUpNodeRemoveTestCmd,
		replaceUpNodeRemoveUnseededTestCmd,
	)

	globalArgs.RegisterFlags(DTestCmd)
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

func newLogger(cmd *cobra.Command) xlog.Logger {
	logger := xlog.NewLogger(os.Stdout)
	logger.Infof("============== %v ==============", cmd.Name())
	desc := cmd.Long
	if desc == "" {
		desc = cmd.Short
	}
	logger.Infof("Test description: %v", desc)
	logger.Infof("============================")
	return logger
}
