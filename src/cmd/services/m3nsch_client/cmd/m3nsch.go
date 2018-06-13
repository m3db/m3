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
