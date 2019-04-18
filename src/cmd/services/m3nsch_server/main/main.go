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
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3nsch_server/config"
	"github.com/m3db/m3/src/cmd/services/m3nsch_server/services"
	"github.com/m3db/m3/src/cmd/services/m3nsch_server/tcp"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/m3nsch"
	"github.com/m3db/m3/src/m3nsch/agent"
	"github.com/m3db/m3/src/m3nsch/datums"
	proto "github.com/m3db/m3/src/m3nsch/generated/proto/m3nsch"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
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

	rawLogger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("unable to create logger: %v", err)
	}

	logger := rawLogger.Sugar()
	conf, err := config.New(*configFile)
	if err != nil {
		logger.Fatalf("unable to read configuration file: %v", err.Error())
	}

	maxProcs := int(float64(runtime.NumCPU()) * conf.Server.CPUFactor)
	logger.Infof("setting maxProcs = %d", maxProcs)
	runtime.GOMAXPROCS(maxProcs)

	StartPProfServer(conf.Server.DebugAddress, rawLogger)

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
		SetLogger(rawLogger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(conf.Metrics.SamplingRate)
	datumRegistry := datums.NewDefaultRegistry(conf.M3nsch.NumPointsPerDatum)
	agentOpts := agent.NewOptions(iopts).
		SetConcurrency(conf.M3nsch.Concurrency).
		SetNewSessionFn(newSessionFn(conf, iopts))
	agent := agent.New(datumRegistry, agentOpts)
	ServeGRPCService(listener, agent, scope, rawLogger)
}

func newSessionFn(conf config.Configuration, iopts instrument.Options) m3nsch.NewSessionFn {
	return m3nsch.NewSessionFn(func(zone, env string) (client.Session, error) {
		svc := conf.DBClient.EnvironmentConfig.Service
		svc.Env = env
		svc.Zone = zone
		cl, err := conf.DBClient.NewClient(client.ConfigurationParameters{
			InstrumentOptions: iopts,
		})
		if err != nil {
			return nil, err
		}
		return cl.NewSession()
	})
}

// StartPProfServer starts a pprof server at specified address, or crashes
// the program on failure.
func StartPProfServer(debugAddress string, logger *zap.Logger) {
	go func() {
		if err := http.ListenAndServe(debugAddress, nil); err != nil {
			logger.Fatal("unable to serve debug server", zap.Error(err))
		}
	}()
	logger.Info("serving pprof endpoints", zap.String("address", debugAddress))
}

// ServeGRPCService serves m3nsch_server GRPC service at the specified address.
func ServeGRPCService(
	listener net.Listener,
	agent m3nsch.Agent,
	scope tally.Scope,
	logger *zap.Logger,
) {
	server := grpc.NewServer(grpc.MaxConcurrentStreams(16384))
	service, err := services.NewGRPCService(agent, scope, logger)
	if err != nil {
		logger.Fatal("could not create grpc service", zap.Error(err))
	}
	proto.RegisterMenschServer(server, service)
	logger.Info("serving m3nsch endpoints", zap.Stringer("address", listener.Addr()))
	err = server.Serve(listener)
	if err != nil {
		logger.Fatal("could not serve", zap.Error(err))
	}
}
