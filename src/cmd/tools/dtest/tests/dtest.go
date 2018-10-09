// Copyright (c) 2018 Uber Technologies, Inc.
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

package dtests

import (
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/dtest/config"
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
