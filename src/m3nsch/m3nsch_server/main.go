package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/m3db/m3nsch"
	"github.com/m3db/m3nsch/agent"
	"github.com/m3db/m3nsch/datums"
	"github.com/m3db/m3nsch/m3nsch_server/config"
	"github.com/m3db/m3nsch/m3nsch_server/services"
	"github.com/m3db/m3nsch/m3nsch_server/tcp"
	"github.com/m3db/m3nsch/rpc"

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

	reporter, err := conf.Metrics.M3.NewReporter()
	if err != nil {
		logger.Fatalf("could not connect to metrics: %v", err)
	}
	scope, _ := tally.NewCachedRootScope(conf.Metrics.Prefix, nil, reporter, time.Second, tally.DefaultSeparator)

	listener, err := tcp.NewTCPListener(conf.Server.ListenAddress, 3*time.Minute)
	if err != nil {
		logger.Fatalf("could not create TCP Listener: %v", err)
	}

	iopts := instrument.
		NewOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(conf.Metrics.SampleRate)
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
	rpc.RegisterMenschServer(server, service)
	logger.Infof("serving m3nsch endpoints at %v", listener.Addr().String())
	err = server.Serve(listener)
	if err != nil {
		logger.Fatalf("could not serve: %v", err)
	}
}
