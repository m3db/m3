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
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/flagvar"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/database"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/placements"
	"go.uber.org/zap"
)

const (
	defaultEndpoint = "http://localhost:7201"
)

func main() {

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	endPoint := flag.String("endpoint", defaultEndpoint, "The url for target m3db backend.")

	createDatabaseYaml := flagvar.File{}
	databaseCmdFlags := database.SetupDatabaseFlags(&createDatabaseYaml)

	namespaceArgs := namespaces.NamespaceArgs{}
	namespaceCmdFlags := namespaces.SetupNamespaceFlags(&namespaceArgs)

	placementArgs := placements.PlacementArgs{}
	placementCmdFlags := placements.SetupPlacementFlags(&placementArgs)

	flag.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %+v", err)
		os.Exit(1)
	}
	log := rawLogger.Sugar()

	switch flag.Arg(0) {
	case databaseCmdFlags.Name():

		if err := databaseCmdFlags.Parse(flag.Args()[1:]); err != nil {
			databaseCmdFlags.Usage()
			os.Exit(1)
		}

		if databaseCmdFlags.NFlag() > 1 {
			fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
			databaseCmdFlags.Usage()
			os.Exit(1)
		}

		if len(createDatabaseYaml.Value) < 1 {
			databaseCmdFlags.Usage()
			os.Exit(1)
		}

		database.Cmd(createDatabaseYaml.Value, *endPoint, log)

	case namespaceCmdFlags.Name():

		if err := namespaceCmdFlags.Parse(flag.Args()[1:]); err != nil {
			namespaceCmdFlags.Usage()
			os.Exit(1)
		}

		if namespaceCmdFlags.NFlag() > 1 {
			fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
			namespaceCmdFlags.Usage()
			os.Exit(1)
		}

		namespaces.Cmd(&namespaceArgs, *endPoint, log)

	case placementCmdFlags.Name():

		if err := placementCmdFlags.Parse(flag.Args()[1:]); err != nil {
			placementCmdFlags.Usage()
			os.Exit(1)
		}

		if placementCmdFlags.NFlag() > 1 {
			fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
			placementCmdFlags.Usage()
			os.Exit(1)
		}

		placements.Cmd(&placementArgs, *endPoint, log)

	default:
		flag.Usage()
		os.Exit(1)

	}

}
