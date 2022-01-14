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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/apply"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/placements"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/topics"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultEndpoint = "http://localhost:7201"
)

// Defaults are (so output is easily consumable by JSON tools like "jq").
// - Error log level so usually not printing anything unless error encountered
// so the output can be completely JSON.
// - Do not print log stack traces so errors aren't overwhelming output.
var defaultLoggerOptions = loggerOptions{
	level:            zapcore.ErrorLevel,
	enableStacktrace: false,
}

type loggerOptions struct {
	level            zapcore.Level
	enableStacktrace bool
}

func mustNewLogger(opts loggerOptions) *zap.Logger {
	loggerCfg := zap.NewDevelopmentConfig()
	loggerCfg.Level = zap.NewAtomicLevelAt(opts.level)
	loggerCfg.DisableStacktrace = !opts.enableStacktrace
	logger, err := loggerCfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}

	return logger
}

func main() {
	var (
		debug     bool
		endPoint  string
		headers   = make(map[string]string)
		yamlPath  string
		showAll   bool
		deleteAll bool
		nodeName  string
	)

	logger := mustNewLogger(defaultLoggerOptions)
	defer func() {
		logger.Sync()
		fmt.Printf("\n") // End line since most commands finish without an endpoint.
	}()

	rootCmd := &cobra.Command{
		Use: "m3ctl",
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
endpoint.  See the yaml/examples directory for examples. Operations such as
database creation, database init, adding a node, and replacing a node, are supported.
`,
		Run: func(cmd *cobra.Command, args []string) {
			fileArg := cmd.LocalFlags().Lookup("file").Value.String()
			logger.Debug("running command", zap.String("name", cmd.Name()), zap.String("args", fileArg))

			if len(fileArg) == 0 {
				logger.Fatal("need to specify a path to YAML file")
			}

			resp, err := apply.DoApply(endPoint, headers, yamlPath, logger)
			if err != nil {
				logger.Fatal("apply failed", zap.Error(err))
			}

			os.Stdout.Write(resp)
		},
	}

	getNamespaceCmd := &cobra.Command{
		Use:     "namespace []",
		Short:   "Get the namespaces from the remote endpoint",
		Aliases: []string{"ns"},
		Run: func(cmd *cobra.Command, args []string) {
			logger.Debug("running command", zap.String("command", cmd.Name()))

			resp, err := namespaces.DoGet(endPoint, headers, logger)
			if err != nil {
				logger.Fatal("get namespace failed", zap.Error(err))
			}

			if !showAll {
				var registry admin.NamespaceGetResponse
				unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
				reader := bytes.NewReader(resp)
				if err := unmarshaller.Unmarshal(reader, &registry); err != nil {
					logger.Fatal("could not unmarshal response", zap.Error(err))
				}
				var namespaces []string
				for k := range registry.Registry.Namespaces {
					namespaces = append(namespaces, k)
				}
				// Keep output consistent and output JSON.
				if err := json.NewEncoder(os.Stdout).Encode(namespaces); err != nil {
					logger.Fatal("could not encode output", zap.Error(err))
				}
				return
			}

			os.Stdout.Write(resp)
		},
	}

	getPlacementCmd := &cobra.Command{
		Use:       "placement <m3db/m3coordinator/m3aggregator>",
		Short:     "Get service placement from the remote endpoint",
		Args:      cobra.ExactValidArgs(1),
		ValidArgs: []string{"m3db", "m3coordinator", "m3aggregator"},
		Aliases:   []string{"pl"},
		Run: func(cmd *cobra.Command, args []string) {
			logger.Debug("running command", zap.String("command", cmd.Name()))

			resp, err := placements.DoGet(endPoint, args[0], headers, logger)
			if err != nil {
				logger.Fatal("get placement failed", zap.Error(err))
			}

			os.Stdout.Write(resp)
		},
	}

	deletePlacementCmd := &cobra.Command{
		Use:       "placement <m3db/m3coordinator/m3aggregator>",
		Short:     "Delete service placement from the remote endpoint",
		Args:      cobra.ExactValidArgs(1),
		ValidArgs: []string{"m3db", "m3coordinator", "m3aggregator"},
		Aliases:   []string{"pl"},
		Run: func(cmd *cobra.Command, args []string) {
			logger.Debug("running command", zap.String("command", cmd.Name()))

			resp, err := placements.DoDelete(endPoint, args[0], headers, nodeName, deleteAll, logger)
			if err != nil {
				logger.Fatal("delete placement failed", zap.Error(err))
			}

			os.Stdout.Write(resp)
		},
	}

	deleteNamespaceCmd := &cobra.Command{
		Use:     "namespace",
		Short:   "Delete the namespace from the remote endpoint",
		Aliases: []string{"ns"},
		Run: func(cmd *cobra.Command, args []string) {
			logger.Debug("running command", zap.String("command", cmd.Name()))

			resp, err := namespaces.DoDelete(endPoint, headers, nodeName, logger)
			if err != nil {
				logger.Fatal("delete namespace failed", zap.Error(err))
			}

			os.Stdout.Write(resp)
		},
	}

	getTopicCmd := &cobra.Command{
		Use:     "topic",
		Short:   "Get topic from the remote endpoint",
		Aliases: []string{"t"},
		Run: func(cmd *cobra.Command, args []string) {
			logger.Debug("running command", zap.String("command", cmd.Name()))

			resp, err := topics.DoGet(endPoint, headers, logger)
			if err != nil {
				logger.Fatal("get topic failed", zap.Error(err))
			}

			os.Stdout.Write(resp) //nolint:errcheck
		},
	}

	deleteTopicCmd := &cobra.Command{
		Use:     "topic",
		Short:   "Delete topic from the remote endpoint",
		Aliases: []string{"t"},
		Run: func(cmd *cobra.Command, args []string) {
			logger.Debug("running command", zap.String("command", cmd.Name()))

			resp, err := topics.DoDelete(endPoint, headers, logger)
			if err != nil {
				logger.Fatal("delete topic failed", zap.Error(err))
			}

			os.Stdout.Write(resp) //nolint:errcheck
		},
	}

	rootCmd.AddCommand(getCmd, applyCmd, deleteCmd)
	getCmd.AddCommand(getNamespaceCmd)
	getCmd.AddCommand(getPlacementCmd)
	getCmd.AddCommand(getTopicCmd)
	deleteCmd.AddCommand(deletePlacementCmd)
	deleteCmd.AddCommand(deleteNamespaceCmd)
	deleteCmd.AddCommand(deleteTopicCmd)

	var headersSlice []string
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "debug log output level (cannot use JSON output)")
	rootCmd.PersistentFlags().StringVar(&endPoint, "endpoint", defaultEndpoint, "m3coordinator endpoint URL")
	rootCmd.PersistentFlags().StringSliceVarP(&headersSlice, "header", "H", []string{}, "headers to append to requests")
	applyCmd.Flags().StringVarP(&yamlPath, "file", "f", "", "times to echo the input")
	getNamespaceCmd.Flags().BoolVarP(&showAll, "show-all", "a", false, "times to echo the input")
	deletePlacementCmd.Flags().BoolVarP(&deleteAll, "delete-all", "a", false, "delete the entire placement")
	deleteCmd.PersistentFlags().StringVarP(&nodeName, "name", "n", "", "which namespace or node to delete")

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Override logger if debug flag set.
		if debug {
			logger = mustNewLogger(loggerOptions{
				level:            zapcore.DebugLevel,
				enableStacktrace: true,
			})
		}

		// Parse headers slice.
		for _, h := range headersSlice {
			parts := strings.Split(h, ":")
			if len(parts) != 2 {
				return fmt.Errorf(
					"header must be of format 'name: value': actual='%s'", h)
			}

			name, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			headers[name] = value
		}

		return nil
	}

	rootCmd.Execute()
}
