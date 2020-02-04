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
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/apply"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/delete"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/get"
	"os"
)

const (
	defaultEndpoint = "http://localhost:7201"
)

func main() {

	// top-level option
	endPoint := flag.String("endpoint", defaultEndpoint, "The url for target m3db backend.")

	// flagsets for next level down
	getFlagSets := get.InitializeFlags()
	deleteFlagSets := delete.InitializeFlags()
	applyFlagSets := apply.InitializeFlags()
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
Usage of %s:

	Specify one of the following subcommands, which are shorthand for database, placement, and namespace:

	%s
	%s
	%s

Each subcommand has its own built-in help provided via "-h".

`, os.Args[0], applyFlagSets.Flags.Name(),
			getFlagSets.Get.Name(),
			deleteFlagSets.Flags.Name())

		flag.PrintDefaults()
	}
	// parse this level
	flag.Parse()
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	// dispatch to the next level
	switch flag.Arg(0) {
	case getFlagSets.Get.Name():
		getFlagSets.GlobalOpts.Endpoint = *endPoint
		if err := getFlagSets.PopParseDispatch(flag.Args()); err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(1)
		}
	case deleteFlagSets.Flags.Name():
		deleteFlagSets.GlobalOpts.Endpoint = *endPoint
		if err := deleteFlagSets.PopParseDispatch(flag.Args()); err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(1)
		}
	case applyFlagSets.Flags.Name():
		applyFlagSets.GlobalOpts.Endpoint = *endPoint
		if err := applyFlagSets.PopParseDispatch(flag.Args()); err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(1)
		}
	default:
		flag.Usage()
		os.Exit(1)
	}
}
