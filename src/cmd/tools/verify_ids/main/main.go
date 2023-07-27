// Copyright (c) 2023 Uber Technologies, Inc.
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

// Package main is the entry point for verify_ids command-line tool.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/exp/slices"
)

const (
	maxIDs  = 1000
	epsilon = 0.001
)

type queryRange struct {
	start time.Time
	end   time.Time
}

func main() {
	var (
		tchannelNodeAddrsArg = flag.String("nodes", "127.0.0.1:9000", "Node TChannel server addresses, comma separated")
		namespaceArg         = flag.String("namespace", "default", "Namespace to read from")
		shardsArg            = flag.String("shards", "0", "Shards to pull IDs from, comma separated")
		rangeStartArg        = flag.String("start", "", "Query time range start")
		rangeEndArg          = flag.String("end", "", "Query time range end")
		limitArg             = flag.Int64("num", 8, "Number of IDs to fetch per shard")
		fromStdInArg         = flag.Bool("stdin", false, "Read IDs from stdin instead")
		dumpRawArg           = flag.Bool("raw", false, "Dump data as json to stdout only")
	)
	flag.Parse()

	if *tchannelNodeAddrsArg == "" ||
		*namespaceArg == "" ||
		*shardsArg == "" ||
		*limitArg < 0 {
		flag.Usage()
		os.Exit(1)
	}

	if *limitArg > maxIDs {
		log.Fatalf("requested number of IDs is too high")
	}

	var (
		rangeStart time.Time
		rangeEnd   time.Time
	)

	if *rangeStartArg == "" {
		rangeStart = time.Now().Add(-30 * time.Minute).Truncate(time.Minute)
	} else if err := rangeEnd.UnmarshalText([]byte(*rangeStartArg)); err != nil {
		log.Fatalf("failed to parse start time: %v", *rangeStartArg)
	}

	if *rangeEndArg == "" {
		rangeEnd = time.Now().Add(-1 * time.Minute).Truncate(time.Minute)
	} else if err := rangeEnd.UnmarshalText([]byte(*rangeEndArg)); err != nil {
		log.Fatalf("failed to parse end time: %v", *rangeEndArg)
	}

	dumpRaw := *dumpRawArg
	readInputFromStdin := *fromStdInArg
	namespace := []byte(*namespaceArg)

	var shards []uint32
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
	nodeAddrs := strings.Split(*tchannelNodeAddrsArg, ",")
	resultLimit := int(*limitArg)

	nodes := make([]dbnode, 0, len(nodeAddrs))
	for _, v := range nodeAddrs {
		nodes = append(nodes, dbnode{
			client:    getClient(v),
			namespace: namespace,
			addr:      v,
		})
	}

	qr := queryRange{start: rangeStart, end: rangeEnd}

	var ids [][]byte

	if !readInputFromStdin {
		for i := range shards {
			res, err := nodes[0].getIDs(shards[i], qr, resultLimit)
			if err != nil {
				log.Fatalf("failed to get IDs from %q for shard %v: %v", nodes[0].addr, shards[i], err)
			}
			ids = append(ids, res...)
		}
		log.Printf("read %d ids from %q", len(ids), nodes[0].addr)
	} else {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			if b := scanner.Bytes(); len(b) > 0 {
				ids = append(ids, append([]byte(nil), b...))
			}
		}
		if len(ids) > maxIDs {
			log.Fatalf("got more than max of %v ids", maxIDs)
		}
	}

	resultsByIDByNode := map[string]map[string][]ts.Datapoint{}

	for _, n := range nodes {
		res, err := n.query(ids, qr)
		if err != nil {
			log.Fatalf("failed to query node %q: %v", n.addr, err)
		}
		dps, err := fetchTaggedResultsToDatapoints(res)
		if err != nil {
			log.Fatalf("could not convert results: %v", err)
		}

		for id, result := range dps {
			if _, ok := resultsByIDByNode[id]; !ok {
				resultsByIDByNode[id] = map[string][]ts.Datapoint{}
			}
			resultsByIDByNode[id][n.addr] = result
		}
	}

	if dumpRaw {
		if err := dumpJSON(resultsByIDByNode); err != nil {
			log.Fatal(err)
		}
		return
	}
	printComparision(nodeAddrs, resultsByIDByNode)
}

func dumpJSON(results map[string]map[string][]ts.Datapoint) error {
	b, err := json.Marshal(results)
	if err != nil {
		return err
	}
	fmt.Printf("%s", b)
	return nil
}

//nolint:errcheck
func printComparision(nodes []string, results map[string]map[string][]ts.Datapoint) {
	bufferedStdout := bufio.NewWriter(os.Stdout)
	defer bufferedStdout.Flush()

	for id, res := range results {
		var (
			timestamps []int64
			datapoints = make([][]float64, len(nodes))
		)

		// first collect all seen timestamps from nodes,
		// then sort and deduplicate.
		for _, node := range nodes {
			r := res[node]
			for _, dp := range r {
				timestamps = append(timestamps, int64(dp.TimestampNanos))
			}
		}
		slices.Sort(timestamps)
		timestamps = slices.Compact(timestamps)

		for i, node := range nodes {
			datapoints[i] = make([]float64, len(timestamps))
			src, dst := 0, 0
			r := res[node]
			// there should be at least 1 datapoint for each timestamp from any of the nodes.
			for ; dst < len(timestamps); dst++ {
				datapoints[i][dst] = math.NaN()
				for src < len(r) && src <= dst && int64(r[src].TimestampNanos) != timestamps[dst] {
					src++
				}
				if src < len(r) && int64(r[src].TimestampNanos) == timestamps[dst] {
					datapoints[i][dst] = r[src].Value
				}
			}
		}

		fmt.Fprintf(bufferedStdout, "===\nID: %v\n", id)
		w := tabwriter.NewWriter(bufferedStdout, 0, 0, 0, ' ', tabwriter.Debug|tabwriter.AlignRight)
		w.Write([]byte("Timestamp"))
		w.Write([]byte{'\t'})
		for _, node := range nodes {
			w.Write([]byte(node))
			w.Write([]byte{'\t'})
		}
		w.Write([]byte{'\n'})

		var mismatches []mismatch
		for i := range timestamps {
			fmt.Fprint(w, time.Unix(0, timestamps[i]).Format(time.StampMilli))
			w.Write([]byte{'\t'})
			for j := range nodes {
				ne := false
				refVal := datapoints[0][i]
				curVal := datapoints[j][i]

				if math.IsNaN(refVal) && !math.IsNaN(curVal) ||
					!math.IsNaN(refVal) && math.IsNaN(curVal) {
					ne = true
				} else if math.Abs(curVal-refVal) > epsilon {
					ne = true
				}

				if j > 0 && ne {
					mismatches = append(mismatches, mismatch{
						node:      nodes[j],
						timestamp: timestamps[i],
					})
					fmt.Fprint(w, " (!) ")
				}
				fmt.Fprintf(w, "%f", datapoints[j][i])
				fmt.Fprint(w, "\t")
			}
			fmt.Fprint(w, "\n")
		}
		w.Flush()

		if len(mismatches) > 0 {
			fmt.Fprintf(bufferedStdout, "Mismatches (timestamp, node) for ID: %v\n", id)
			for _, m := range mismatches {
				fmt.Fprintf(bufferedStdout,
					"%v %v\n",
					time.Unix(0, m.timestamp).Format(time.StampMilli),
					m.node)
			}
		}
	}
}

type dbnode struct {
	addr      string
	namespace []byte
	client    rpc.TChanNode
}

func (d *dbnode) getIDs(shard uint32, r queryRange, num int) ([][]byte, error) {
	var (
		results   [][]byte
		pageToken []byte
	)

	getIDsFn := func() error {
		tctx, cancel := thrift.NewContext(60 * time.Second)
		defer cancel()

		req := rpc.NewFetchBlocksMetadataRawV2Request()
		req.NameSpace = d.namespace
		req.Shard = int32(shard)
		if !r.start.IsZero() {
			req.RangeStart = r.start.UnixNano()
		}
		if !r.end.IsZero() {
			req.RangeEnd = r.end.UnixNano()
		}
		req.Limit = int64(num)
		req.PageToken = pageToken

		result, err := d.client.FetchBlocksMetadataRawV2(tctx, req)
		if err != nil {
			return err
		}

		for _, elem := range result.Elements {
			results = append(results, elem.ID)
			if len(results) >= num {
				return io.EOF
			}
		}

		if result.NextPageToken == nil {
			return io.EOF
		}

		pageToken = append([]byte(nil), result.NextPageToken...)
		return nil
	}

	var err error
	for err == nil {
		if err = getIDsFn(); err != nil {
			break
		}
	}

	if err == io.EOF {
		return results, nil
	}
	return results, err
}

func (d *dbnode) query(ids [][]byte, r queryRange) (*rpc.FetchTaggedResult_, error) {
	req := rpc.NewFetchTaggedRequest()
	req.NameSpace = d.namespace
	req.FetchData = true

	if !r.start.IsZero() {
		req.RangeStart = r.start.UnixNano()
	}
	if !r.end.IsZero() {
		req.RangeEnd = r.end.UnixNano()
	}

	var (
		termQueries []idx.Query
		err         error
	)
	for _, id := range ids {
		termQueries = append(termQueries, idx.NewTermQuery(doc.IDReservedFieldName, id))
	}

	req.Query, err = idx.Marshal(idx.NewDisjunctionQuery(termQueries...))
	if err != nil {
		return nil, err
	}

	tctx, cancel := thrift.NewContext(15 * time.Second)
	defer cancel()

	return d.client.FetchTagged(tctx, req)
}

func getClient(nodeAddr string) rpc.TChanNode {
	ch, err := tchannel.NewChannel("Client", nil)
	if err != nil {
		panic(fmt.Sprintf("could not create new tchannel channel for %q: %v", nodeAddr, err))
	}
	endpoint := &thrift.ClientOptions{HostPort: nodeAddr}
	thriftClient := thrift.NewClient(ch, channel.ChannelName, endpoint)
	return rpc.NewTChanNodeClient(thriftClient)
}

func fetchTaggedResultsToDatapoints(result *rpc.FetchTaggedResult_) (map[string][]ts.Datapoint, error) {
	res := map[string][]ts.Datapoint{}
	encodingOpts := encoding.NewOptions()

	for _, elem := range result.Elements {
		var dps []ts.Datapoint
		iter := client.NewReaderSliceOfSlicesIterator(elem.Segments, nil)
		multiReader := encoding.NewMultiReaderIterator(m3tsz.DefaultReaderIteratorAllocFn(encodingOpts), nil)
		multiReader.ResetSliceOfSlices(iter, nil)

		for multiReader.Next() {
			dp, _, _ := multiReader.Current()
			dps = append(dps, dp)
		}

		if err := multiReader.Err(); err != nil {
			return nil, err
		}
		res[string(elem.ID)] = dps
	}

	return res, nil
}

type mismatch struct {
	node      string
	timestamp int64
}
