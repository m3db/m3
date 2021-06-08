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
	"log"
	"os"

	"github.com/m3db/m3/src/dbnode/persist/fs/clone"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

var (
	optSrcPathPrefix  = flag.String("src-path-prefix", "/var/lib/m3db", "Source Path prefix")
	optSrcNamespace   = flag.String("src-namespace", "metrics", "Source Namespace")
	optSrcShard       = flag.Uint("src-shard", 0, "Source Shard ID")
	optSrcBlockstart  = flag.Int64("src-block-start", 0, "Source Block Start Time [in nsec]")
	optDestPathPrefix = flag.String("dest-path-prefix", "/tmp/m3db-copy", "Destination Path prefix")
	optDestNamespace  = flag.String("dest-namespace", "metrics", "Destination Namespace")
	optDestShard      = flag.Uint("dest-shard", 0, "Destination Shard ID")
	optDestBlockstart = flag.Int64("dest-block-start", 0, "Destination Block Start Time [in nsec]")
	optDestBlockSize  = flag.Duration("dest-block-size", 0, "Destination Block Size")
)

func main() {
	flag.Parse()
	if *optSrcPathPrefix == "" ||
		*optDestPathPrefix == "" ||
		*optSrcNamespace == "" ||
		*optDestNamespace == "" ||
		*optSrcBlockstart <= 0 ||
		*optDestBlockstart <= 0 {
		flag.Usage()
		os.Exit(1)
	}

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	logger := rawLogger.Sugar()

	src := clone.FileSetID{
		PathPrefix: *optSrcPathPrefix,
		Namespace:  *optSrcNamespace,
		Shard:      uint32(*optSrcShard),
		Blockstart: xtime.UnixNano(*optSrcBlockstart),
	}
	dest := clone.FileSetID{
		PathPrefix: *optDestPathPrefix,
		Namespace:  *optDestNamespace,
		Shard:      uint32(*optDestShard),
		Blockstart: xtime.UnixNano(*optDestBlockstart),
	}

	logger.Infof("source: %+v", src)
	logger.Infof("destination: %+v", dest)

	opts := clone.NewOptions()
	cloner := clone.New(opts)
	if err := cloner.Clone(src, dest, *optDestBlockSize); err != nil {
		logger.Fatalf("unable to clone: %v", err)
	}

	logger.Infof("successfully cloned data")
}
