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
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/database"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
	"github.com/m3db/m3/src/x/config/configflag"
	"go.uber.org/zap"
	"os"
)

const (
	defaultEndpoint = "http://localhost:7201"
)

func main() {

	// top-level option
	endPoint := flag.String("endpoint", defaultEndpoint, "The url for target m3db backend.")

	// the database-related subcommand
	createDatabaseYAML := configflag.FlagStringSlice{}
	databaseFlagSets := database.SetupFlags(&createDatabaseYAML)

	// the namespace-related subcommand
	namespaceArgs := namespaces.NamespaceArgs{}
	namespaceFlagSets := namespaces.SetupFlags(&namespaceArgs)

	// the placement-related subcommand
	placementArgs := placements.PlacementArgs{}
	placementFlagSets := placements.SetupFlags(&placementArgs)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
Usage of %s:

	Specify one of the following subcommands, which are shorthand for database, placement, and namespace:

	%s
	%s
	%s

Each subcommand has its own built-in help provided via "-h".

`, os.Args[0], databaseFlagSets.Database.Name(),
			placementFlagSets.Placement.Name(),
			namespaceFlagSets.Namespace.Name())

		flag.PrintDefaults()
	}

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
	case databaseFlagSets.Database.Name():
		if err := databaseFlagSets.Database.Parse(flag.Args()[1:]); err != nil {
			databaseFlagSets.Database.Usage()
			os.Exit(1)
		}
		if databaseFlagSets.Database.NArg() == 0 {
			databaseFlagSets.Database.Usage()
			os.Exit(1)
		}
		switch flag.Arg(1) {
		case databaseFlagSets.Create.Name():
			if err := databaseFlagSets.Create.Parse(flag.Args()[2:]); err != nil {
				databaseFlagSets.Create.Usage()
				os.Exit(1)
			}
			if databaseFlagSets.Create.NFlag() == 0 {
				databaseFlagSets.Create.Usage()
				os.Exit(1)
			}
			databaseFlagSets.Create.Visit(func(f *flag.Flag) {
				vals := f.Value.(*configflag.FlagStringSlice)
				for _, val := range vals.Value {
					if len(val) == 0 {
						fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
						databaseFlagSets.Create.Usage()
						os.Exit(1)
					}
				}
			})
			// the below createDatabaseYAML.Value has at least one by this time per the arg parser
			database.Create(createDatabaseYAML.Value[len(createDatabaseYAML.Value)-1], *endPoint, log)
		default:
			databaseFlagSets.Database.Usage()
			os.Exit(1)
		}
	case namespaceFlagSets.Namespace.Name():
		if err := namespaceFlagSets.Namespace.Parse(flag.Args()[1:]); err != nil {
			namespaceFlagSets.Namespace.Usage()
			os.Exit(1)
		}
		// maybe do the default action which is listing the names
		if namespaceFlagSets.Namespace.NArg() == 0 {
			namespaces.Show(&namespaceArgs, *endPoint, log)
			os.Exit(0)
		}
		switch flag.Arg(1) {
		case namespaceFlagSets.Delete.Name():
			if err := namespaceFlagSets.Delete.Parse(flag.Args()[2:]); err != nil {
				namespaceFlagSets.Delete.Usage()
				os.Exit(1)
			}
			if namespaceFlagSets.Delete.NFlag() == 0 {
				namespaceFlagSets.Delete.Usage()
				os.Exit(1)
			}
			namespaceFlagSets.Delete.Visit(func(f *flag.Flag) {
				if len(f.Value.String()) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					namespaceFlagSets.Delete.Usage()
					os.Exit(1)
				}
			})
			namespaces.Delete(&namespaceArgs, *endPoint, log)
		default:
			namespaceFlagSets.Namespace.Usage()
			os.Exit(1)
		}
	case placementFlagSets.Placement.Name():
		if err := placementFlagSets.Placement.Parse(flag.Args()[1:]); err != nil {
			placementFlagSets.Placement.Usage()
			os.Exit(1)
		}
		if placementFlagSets.Placement.NArg() == 0 {
			placements.Get(&placementArgs, *endPoint, log)
			os.Exit(0)
		}
		switch flag.Arg(1) {
		case placementFlagSets.Add.Name():
			if err := placementFlagSets.Add.Parse(flag.Args()[2:]); err != nil {
				placementFlagSets.Add.Usage()
				os.Exit(1)
			}
			if placementFlagSets.Add.NFlag() == 0 {
				placementFlagSets.Add.Usage()
				os.Exit(1)
			}
			placementFlagSets.Add.Visit(func(f *flag.Flag) {
				vals := f.Value.(*configflag.FlagStringSlice)
				for _, val := range vals.Value {
					if len(val) == 0 {
						fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
						placementFlagSets.Add.Usage()
						os.Exit(1)
					}
				}
			})
			placements.Add(&placementArgs, *endPoint, log)
		case placementFlagSets.Delete.Name():
			if err := placementFlagSets.Delete.Parse(flag.Args()[2:]); err != nil {
				placementFlagSets.Delete.Usage()
				os.Exit(1)
			}
			if placementFlagSets.Delete.NFlag() == 0 {
				placementFlagSets.Delete.Usage()
				os.Exit(1)
			}
			placementFlagSets.Delete.Visit(func(f *flag.Flag) {
				if len(f.Value.String()) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					placementFlagSets.Delete.Usage()
					os.Exit(1)
				}
			})
			placements.Delete(&placementArgs, *endPoint, log)
		case placementFlagSets.Init.Name():
			if err := placementFlagSets.Init.Parse(flag.Args()[2:]); err != nil {
				placementFlagSets.Init.Usage()
				os.Exit(1)
			}
			if placementFlagSets.Init.NFlag() == 0 {
				placementFlagSets.Init.Usage()
				os.Exit(1)
			}
			placementFlagSets.Init.Visit(func(f *flag.Flag) {
				vals := f.Value.(*configflag.FlagStringSlice)
				for _, val := range vals.Value {
					if len(val) == 0 {
						fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
						placementFlagSets.Init.Usage()
						os.Exit(1)
					}
				}
			})
			placements.Init(&placementArgs, *endPoint, log)
		case placementFlagSets.Replace.Name():
			if err := placementFlagSets.Replace.Parse(flag.Args()[2:]); err != nil {
				placementFlagSets.Replace.Usage()
				os.Exit(1)
			}
			if placementFlagSets.Replace.NFlag() == 0 {
				placementFlagSets.Replace.Usage()
				os.Exit(1)
			}
			placementFlagSets.Replace.Visit(func(f *flag.Flag) {
				vals := f.Value.(*configflag.FlagStringSlice)
				for _, val := range vals.Value {
					if len(val) == 0 {
						fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
						placementFlagSets.Replace.Usage()
						os.Exit(1)
					}
				}
			})
			placements.Replace(&placementArgs, *endPoint, log)
		default:
			placements.Get(&placementArgs, *endPoint, log)
		}
	default:
		flag.Usage()
		os.Exit(1)
	}
}
