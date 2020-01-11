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
		database.ParseAndDo(createDatabaseYAML.Value[len(createDatabaseYAML.Value)-1], &databaseFlagSets, *endPoint, log)
	case namespaceFlagSets.Namespace.Name():
		namespaces.ParseAndDo(&namespaceArgs, &namespaceFlagSets, *endPoint, log)
	case placementFlagSets.Placement.Name():
		placements.ParseAndDo(&placementArgs, &placementFlagSets, *endPoint, log)
	default:
		flag.Usage()
		os.Exit(1)
	}
}
