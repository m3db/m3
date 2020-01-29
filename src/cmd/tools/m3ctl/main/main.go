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

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/database"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
	"github.com/m3db/m3/src/x/config/configflag"
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
	//placementArgs := placements.PlacementArgs{}
	//placementFlagSets := placements.SetupFlags(&placementArgs)
	placementFlagSets := placements.SetupFlags()
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
Usage of %s:

	Specify one of the following subcommands, which are shorthand for database, placement, and namespace:

	%s
	%s
	%s

Each subcommand has its own built-in help provided via "-h".

`, os.Args[0], databaseFlagSets.Database.Name(),
			//placementFlagSets.Placement.Name(),
			placementFlagSets.Placement.Name(),
			namespaceFlagSets.Namespace.Name())

		flag.PrintDefaults()
	}

	flag.Parse()

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	switch flag.Arg(0) {
	case databaseFlagSets.Database.Name():
		database.ParseAndDo(&createDatabaseYAML, &databaseFlagSets, *endPoint)
	case namespaceFlagSets.Namespace.Name():
		namespaces.ParseAndDo(&namespaceArgs, &namespaceFlagSets, *endPoint)
	case placementFlagSets.Placement.Name():
		//placementFlagSets.ParseAndDo(flag.Args(), &placementArgs, *endPoint)
		placementFlagSets.ParseAndDo(flag.Args(), *endPoint)
	default:
		flag.Usage()
		os.Exit(1)
	}
}
