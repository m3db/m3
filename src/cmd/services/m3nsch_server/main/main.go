// Copyright (c) 2017 Uber Technologies, Inc.
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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/m3db/m3db/src/cmd/services/m3nsch_server/config"
	"github.com/m3db/m3db/src/cmd/services/m3nsch_server/services"
	"github.com/m3db/m3db/src/cmd/services/m3nsch_server/tcp"
	"github.com/m3db/m3db/src/m3nsch"
	"github.com/m3db/m3db/src/m3nsch/agent"
	"github.com/m3db/m3db/src/m3nsch/datums"
	"github.com/m3db/m3db/src/m3nsch/rpc"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"

	"github.com/pborman/getopt"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
)

func main() {
	var (
		configFile = getopt.StringLong("config-file", 'f', "", "Configuration file")
	)
	getopt.Parse()
	if len(*configFile) == 0 {
		getopt.Usage()
		return
	}

	logger := xlog.NewLogger(os.Stdout)
	conf, err := config.New(*configFile)
	if err != nil {
		logger.Fatalf("unable to read configuration file: %v", err.Error())
	}

	maxProcs := int(float64(runtime.NumCPU()) * conf.Server.CPUFactor)
	logger.Infof("setting maxProcs = %d", maxProcs)
	runtime.GOMAXPROCS(maxProcs)

	StartPProfServer(conf.Server.DebugAddress, logger)

	scope, closer, err := conf.Metrics.NewRootScope()
	if err != nil {
		logger.Fatalf("could not connect to metrics: %v", err)
	}
	defer closer.Close()

	listener, err := tcp.NewTCPListener(conf.Server.ListenAddress, 3*time.Minute)
	if err != nil {
		logger.Fatalf("could not create TCP Listener: %v", err)
	}

	iopts := instrument.
		NewOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(conf.Metrics.SamplingRate)
	datumRegistry := datums.NewDefaultRegistry(conf.M3nsch.NumPointsPerDatum)
	agentOpts := agent.NewOptions(iopts).
		SetConcurrency(conf.M3nsch.Concurrency) // TODO: also need to SetNewSessionFn()
	agent := agent.New(datumRegistry, agentOpts)
	ServeGRPCService(listener, agent, scope, logger)
}

// StartPProfServer starts a pprof server at specified address, or crashes
// the program on failure.
func StartPProfServer(debugAddress string, logger xlog.Logger) {
	go func() {
		if err := http.ListenAndServe(debugAddress, nil); err != nil {
			logger.Fatalf("unable to serve debug server: %v", err)
		}
	}()
	logger.Infof("serving pprof endpoints at: %v", debugAddress)
}

// ServeGRPCService serves m3nsch_server GRPC service at the specified address.
func ServeGRPCService(
	listener net.Listener,
	agent m3nsch.Agent,
	scope tally.Scope,
	logger xlog.Logger,
) {
	server := grpc.NewServer(grpc.MaxConcurrentStreams(16384))
	service, err := services.NewGRPCService(agent, scope, logger)
	if err != nil {
		logger.Fatalf("could not create grpc service: %v", err)
	}
	rpc.RegisterMenschServer(server, service)
	logger.Infof("serving m3nsch endpoints at %v", listener.Addr().String())
	err = server.Serve(listener)
	if err != nil {
		logger.Fatalf("could not serve: %v", err)
	}
}
