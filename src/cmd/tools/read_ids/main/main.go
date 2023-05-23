// Copyright (c) 2016 Uber Technologies, Inc.
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
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	nchannel "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3/src/x/ident"
	xretry "github.com/m3db/m3/src/x/retry"

	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
)

func main() {
	var (
		tchannelNodeAddrArg = flag.String("nodetchanneladdr", "127.0.0.1:9003", "Node TChannel server address")
		namespaceArg        = flag.String("namespace", "default", "Namespace to read from")
		shardsArg           = flag.String("shards", "0", "Shards to pull IDs from, comma separated")
		pageLimitArg        = flag.Int64("pagelimit", 4096, "Page limit to pull for a single request")
	)
	flag.Parse()

	if *tchannelNodeAddrArg == "" ||
		*namespaceArg == "" ||
		*shardsArg == "" ||
		*pageLimitArg < 0 {
		flag.Usage()
		os.Exit(1)
	}

	tchannelNodeAddr := *tchannelNodeAddrArg
	namespace := *namespaceArg
	shards := []uint32{}
	for _, str := range strings.Split(*shardsArg, ",") {
		value, err := strconv.Atoi(str)
		if err != nil {
			log.Fatalf("could not parse shard '%s': %v", str, err)
		}
		if value < 0 {
			log.Fatalf("could not parse shard '%s': not uint", str)
		}
		shards = append(shards, uint32(value))
	}
	pageLimit := *pageLimitArg

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	log := rawLogger.Sugar()

	channel, err := tchannel.NewChannel("Client", nil)
	if err != nil {
		log.Fatalf("could not create new tchannel channel: %v", err)
	}
	endpoint := &thrift.ClientOptions{HostPort: tchannelNodeAddr}
	thriftClient := thrift.NewClient(channel, nchannel.ChannelName, endpoint)
	client := rpc.NewTChanNodeClient(thriftClient)

	writer := os.Stdout

	for _, shard := range shards {
		log.Infof("reading ids for shard %d", shard)
		var (
			total     int
			pageToken []byte
			retrier   = xretry.NewRetrier(xretry.NewOptions().
				SetBackoffFactor(2).
				SetMaxRetries(3).
				SetInitialBackoff(time.Second).
				SetJitter(true))
			optionIncludeSizes     = true
			optionIncludeChecksums = true
			moreResults            = true
		)
		// Declare before loop to avoid redeclaring each iteration
		attemptFn := func() error {
			tctx, _ := thrift.NewContext(60 * time.Second)
			req := rpc.NewFetchBlocksMetadataRawV2Request()
			req.NameSpace = ident.StringID(namespace).Bytes()
			req.Shard = int32(shard)
			req.RangeStart = 0
			req.RangeEnd = time.Now().Add(365 * 24 * time.Hour).UnixNano()
			req.Limit = pageLimit
			req.PageToken = pageToken
			req.IncludeSizes = &optionIncludeSizes
			req.IncludeChecksums = &optionIncludeChecksums

			result, err := client.FetchBlocksMetadataRawV2(tctx, req)
			if err != nil {
				return err
			}

			if result.NextPageToken != nil {
				// Create space on the heap for the page token and take it's
				// address to avoid having to keep the entire result around just
				// for the page token
				pageToken = append([]byte(nil), result.NextPageToken...)
			} else {
				// No further results
				moreResults = false
			}

			endLine := []byte("\n")
			for _, elem := range result.Elements {
				writer.Write(elem.ID)
				writer.Write(endLine)
				total++
			}

			return nil
		}
		for moreResults {
			if err := retrier.Attempt(attemptFn); err != nil {
				log.Fatalf("could not stream metadata: %v", err)
			}
		}
		log.Infof("read %d ids for shard %d", total, shard)
	}
}
