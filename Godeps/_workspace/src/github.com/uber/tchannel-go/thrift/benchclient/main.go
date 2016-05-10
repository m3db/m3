// Copyright (c) 2015 Uber Technologies, Inc.

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

// benchclient is used to make requests to a specific server.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/uber/tchannel-go/testutils"
	"github.com/uber/tchannel-go/thrift"
	gen "github.com/uber/tchannel-go/thrift/gen-go/test"
)

var (
	requestSize = flag.Int("requestSize", 10000, "The number of bytes of each request")
	serviceName = flag.String("serviceName", "bench-server", "The benchmark server's service name")
	timeout     = flag.Duration("timeout", time.Second, "Timeout for each request")
)

func main() {
	flag.Parse()

	ch, err := testutils.NewClientChannel(nil)
	if err != nil {
		log.Fatalf("Failed to create client channel: %v", err)
	}

	for _, host := range flag.Args() {
		ch.Peers().Add(host)
	}
	thriftClient := thrift.NewClient(ch, *serviceName, nil)
	client := gen.NewTChanSecondServiceClient(thriftClient)

	fmt.Println("bench-client started")

	rdr := bufio.NewReader(os.Stdin)
	for {
		line, err := rdr.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatalf("stdin read failed: %v", err)
		}

		line = strings.TrimSuffix(line, "\n")
		switch line {
		case "call":
			makeCall(client)
		case "quit":
			return
		default:
			log.Fatalf("unrecognized command: %v", line)
		}
	}
}

var arg string

func makeArg() string {
	if len(arg) > 0 {
		return arg
	}

	bs := []byte{}
	for i := 0; i < *requestSize; i++ {
		bs = append(bs, byte(i%26+'A'))
	}
	arg = string(bs)
	return arg
}

func makeCall(client gen.TChanSecondService) {
	ctx, cancel := thrift.NewContext(*timeout)
	defer cancel()

	arg := makeArg()
	started := time.Now()
	res, err := client.Echo(ctx, arg)
	if err != nil {
		fmt.Println("failed:", err)
		return
	}
	if res != arg {
		log.Fatalf("Echo gave different string!")
	}
	duration := time.Since(started)
	fmt.Println(duration)
}
