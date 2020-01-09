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

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/database"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/placements"
	"github.com/m3db/m3/src/x/config/configflag"
	"go.uber.org/zap"
)

const (
	defaultEndpoint = "http://localhost:7201"
)

func main() {

	// top-level option
	endPoint := flag.String("endpoint", defaultEndpoint, "The url for target m3db backend.")

	// the database-related subcommand
	createDatabaseYAML := configflag.FlagStringSlice{}
	databaseFlags := database.SetupDatabaseFlags(&createDatabaseYAML)

	// the namespace-relateed subcommand
	namespaceArgs := namespaces.NamespaceArgs{}
	namespaceFlags := namespaces.SetupNamespaceFlags(&namespaceArgs)

	// the placement-related subcommand
	placementArgs := placements.PlacementArgs{}
	placementFlags := placements.SetupPlacementFlags(&placementArgs)

	flag.Parse()

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %+v", err)
		os.Exit(1)
	}
	log := rawLogger.Sugar()

	switch flag.Arg(0) {
	case databaseFlags.Name():
		if err := databaseFlags.Parse(flag.Args()[1:]); err != nil {
			databaseFlags.Usage()
			os.Exit(1)
		}
		if databaseFlags.NFlag() == 0 {
			databaseFlags.Usage()
			os.Exit(1)
		}
		databaseFlags.Visit(func(f *flag.Flag) {
			vals := f.Value.(*configflag.FlagStringSlice)
			for _, val := range vals.Value {
				if len(val) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					databaseFlags.Usage()
					os.Exit(1)
				}
			}
		})
		database.Command(createDatabaseYAML.Value[len(createDatabaseYAML.Value)-1], *endPoint, log)
	case namespaceFlags.Name():
		if err := namespaceFlags.Parse(flag.Args()[1:]); err != nil {
			namespaceFlags.Usage()
			os.Exit(1)
		}
		if namespaceFlags.NFlag() > 1 {
			fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
			namespaceFlags.Usage()
			os.Exit(1)
		}
		namespaceFlags.Visit(func(f *flag.Flag) {
			vals := f.Value.(*configflag.FlagStringSlice)
			for _, val := range vals.Value {
				if len(val) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					namespaceFlags.Usage()
					os.Exit(1)
				}
			}
		})
		namespaces.Command(&namespaceArgs, *endPoint, log)
	case placementFlags.Name():
		if err := placementFlags.Parse(flag.Args()[1:]); err != nil {
			placementFlags.Usage()
			os.Exit(1)
		}
		if placementFlags.NFlag() > 1 {
			fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
			placementFlags.Usage()
			os.Exit(1)
		}
		placementFlags.Visit(func(f *flag.Flag) {
			switch f.Name {
			case placements.DeleteNodeName:
				if len(f.Value.String()) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					placementFlags.Usage()
					os.Exit(1)
				}
			case placements.InitName, placements.NewNodeName, placements.ReplaceNodeName:
				vals := f.Value.(*configflag.FlagStringSlice)
				for _, val := range vals.Value {
					if len(val) == 0 {
						fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
						placementFlags.Usage()
						os.Exit(1)
					}
				}
			default:
				return
			}
		})
		placements.Command(&placementArgs, *endPoint, log)
	default:
		flag.Usage()
		os.Exit(1)
	}
}
