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
	_ "net/http/pprof" // pprof: for debug listen server if configured

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/client"
	dbserver "github.com/m3db/m3/src/dbnode/server"
	coordinatorserver "github.com/m3db/m3/src/query/server"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/configflag"
	xos "github.com/m3db/m3/src/x/os"
)

func main() {
	var cfgOpts configflag.Options
	cfgOpts.Register()

	flag.Parse()

	var cfg config.Configuration
	if err := cfgOpts.MainLoad(&cfg, xconfig.Options{}); err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("error validating config: %v", err)
	}

	var (
		numComponents     int
		dbClientCh        chan client.Client
		clusterClientCh   chan clusterclient.Client
		coordinatorDoneCh chan struct{}
	)
	if cfg.DB != nil {
		numComponents++
	}
	if cfg.Coordinator != nil {
		numComponents++
	}

	interruptCh := xos.NewInterruptChannel(numComponents)
	if cfg.DB != nil {
		dbClientCh = make(chan client.Client, 1)
		clusterClientCh = make(chan clusterclient.Client, 1)
	}

	if cfg.Coordinator != nil {
		coordinatorDoneCh = make(chan struct{}, 1)
		go func() {
			coordinatorserver.Run(coordinatorserver.RunOptions{
				Config:        *cfg.Coordinator,
				DBConfig:      cfg.DB,
				DBClient:      dbClientCh,
				ClusterClient: clusterClientCh,
				InterruptCh:   interruptCh,
			})
			coordinatorDoneCh <- struct{}{}
		}()
	}

	if cfg.DB != nil {
		dbserver.Run(dbserver.RunOptions{
			Config:          *cfg.DB,
			ClientCh:        dbClientCh,
			ClusterClientCh: clusterClientCh,
			InterruptCh:     interruptCh,
		})
	} else if cfg.Coordinator != nil {
		<-coordinatorDoneCh
	}
}
