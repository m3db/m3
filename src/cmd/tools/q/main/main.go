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
	"strings"

	"github.com/m3db/m3/src/cmd/tools/q/main/database"
	"github.com/m3db/m3/src/cmd/tools/q/main/namespaces"
	"github.com/m3db/m3/src/cmd/tools/q/main/placements"
	//"github.com/m3db/m3/src/dbnode/persist/fs"
	//"github.com/m3db/m3/src/x/ident"

	//"github.com/pborman/getopt"
	"go.uber.org/zap"
)

func main() {

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
Usage of %s:

	Specify one of the following subcommands:

	%s


`, os.Args[0], strings.Join([]string{
			database.CmdFlags.Name(),
			placements.CmdFlags.Name(),
			namespaces.CmdFlags.Name(),
		}, ", "))

		flag.PrintDefaults()
	}

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}

	flag.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %+v", err)
		os.Exit(1)
	}
	log := rawLogger.Sugar()

	switch flag.Arg(0) {
	case database.CmdFlags.Name():

		database.Cmd(log)

	case namespaces.CmdFlags.Name():

		namespaces.Cmd(log)

	case placements.CmdFlags.Name():

		placements.Cmd(log)

	default:
		flag.Usage()
		os.Exit(1)

	}

}
