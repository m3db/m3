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
	"fmt"
	_ "net/http/pprof" // pprof: for debug listen server if configured
	"os"

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/src/cmd/services/m3dbnode/config"
	dbserver "github.com/m3db/m3db/src/cmd/services/m3dbnode/server"
	coordinatorserver "github.com/m3db/m3db/src/coordinator/services/m3coordinator/server"
	"github.com/m3db/m3db/src/dbnode/client"
	xconfig "github.com/m3db/m3x/config"
)

var (
	configFile = flag.String("f", "", "configuration file")
)

func main() {
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, *configFile, xconfig.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "unable to load config from %s: %v\n", *configFile, err)
		os.Exit(1)
	}

	var (
		dbClientCh        chan client.Client
		clusterClientCh   chan clusterclient.Client
		coordinatorDoneCh chan struct{}
	)

	if cfg.DB != nil {
		dbClientCh = make(chan client.Client, 1)
		clusterClientCh = make(chan clusterclient.Client, 1)
	}

	if cfg.Coordinator != nil {
		coordinatorDoneCh = make(chan struct{}, 1)
		go func() {
			coordinatorserver.Run(coordinatorserver.RunOptions{
				Config:        *cfg.Coordinator,
				DBClient:      dbClientCh,
				ClusterClient: clusterClientCh,
			})
			coordinatorDoneCh <- struct{}{}
		}()
	}

	if cfg.DB != nil {
		dbserver.Run(dbserver.RunOptions{
			Config:          *cfg.DB,
			ClientCh:        dbClientCh,
			ClusterClientCh: clusterClientCh,
		})
	} else if cfg.Coordinator != nil {
		<-coordinatorDoneCh
	}
}
