// Copyright (c) 2020 Uber Technologies, Inc.
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

package main

import (
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/apply"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/placements"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	DefaultEndpoint = "http://localhost:7201"
)

var (
	endPoint string
	yamlPath string
	showAll  bool
	nodeName string
)

func main() {

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	rootCmd := &cobra.Command{
		Use: "cobra",
	}

	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Get specified resources from the remote",
	}

	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete specified resources from the remote",
	}

	applyCmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply various yamls to remote endpoint",
		Long: `This will take specific yamls and send them over to the remote
endpoint.  See the yaml/examples directory for examples.  Operations such as 
database creation, database init, adding a node, and replacing a node, are supported.
`,
		Run: func(cmd *cobra.Command, args []string) {

			fileArg := cmd.LocalFlags().Lookup("file").Value.String()
			logger.Debug("running command", zap.String("name", cmd.Name()), zap.String("args", fileArg))

			if len(fileArg) == 0 {
				logger.Error("specify a path to a yaml file.\n")
				cmd.Usage()
				os.Exit(1)
			}

			if err := apply.DoApply(endPoint, yamlPath, logger); err != nil {
				logger.Error("apply failed", zap.Error(err))
				os.Exit(1)
			}

		},
	}

	getNamespaceCmd := &cobra.Command{
		Use:     "namespace []",
		Short:   "Get the namespaces from the remote endpoint",
		Aliases: []string{"ns"},
		Run: func(cmd *cobra.Command, args []string) {

			logger.Debug("running command", zap.String("command", cmd.Name()))

			if err := namespaces.DoGet(endPoint, showAll, logger); err != nil {
				logger.Error("get namespace failed", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	getPlacementCmd := &cobra.Command{
		Use:     "placement",
		Short:   "Get the placement from the remote endpoint",
		Aliases: []string{"pl"},
		Run: func(cmd *cobra.Command, args []string) {

			logger.Debug("running command", zap.String("command", cmd.Name()))

			if err := placements.DoGet(endPoint, logger); err != nil {
				logger.Error("get placement failed", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	deletePlacementCmd := &cobra.Command{
		Use:     "placement",
		Short:   "Delete the placement from the remote endpoint",
		Aliases: []string{"pl"},
		Run: func(cmd *cobra.Command, args []string) {

			logger.Debug("running command", zap.String("command", cmd.Name()))

			if err := placements.DoDelete(endPoint, nodeName, showAll, logger); err != nil {
				logger.Error("delete placement failed", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	deleteNamespaceCmd := &cobra.Command{
		Use:     "namespace",
		Short:   "Delete the namespace from the remote endpoint",
		Aliases: []string{"ns"},
		Run: func(cmd *cobra.Command, args []string) {

			logger.Debug("running command", zap.String("command", cmd.Name()))

			if err := namespaces.DoDelete(endPoint, nodeName, logger); err != nil {
				logger.Error("delete namespace failed", zap.Error(err))
				os.Exit(1)
			}
		},
	}

	rootCmd.AddCommand(getCmd, applyCmd, deleteCmd)
	getCmd.AddCommand(getNamespaceCmd)
	getCmd.AddCommand(getPlacementCmd)
	deleteCmd.AddCommand(deletePlacementCmd)
	deleteCmd.AddCommand(deleteNamespaceCmd)

	rootCmd.PersistentFlags().StringVar(&endPoint, "endpoint", DefaultEndpoint, "m3db service endpoint")
	applyCmd.Flags().StringVarP(&yamlPath, "file", "f", "", "times to echo the input")
	getNamespaceCmd.Flags().BoolVarP(&showAll, "showAll", "a", false, "times to echo the input")
	deletePlacementCmd.Flags().BoolVarP(&showAll, "deleteAll", "a", false, "delete the entire placement")
	deleteCmd.PersistentFlags().StringVarP(&nodeName, "nodeName", "n", "", "which node to delete")

	rootCmd.Execute()

}
