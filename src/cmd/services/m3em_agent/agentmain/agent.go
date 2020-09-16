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

package agentmain

import (
	"log"
	"net/http"
	_ "net/http/pprof" // pprof import
	oexec "os/exec"
	"strings"
	"time"

	m3emconfig "github.com/m3db/m3/src/cmd/services/m3em_agent/config"
	"github.com/m3db/m3/src/m3em/agent"
	"github.com/m3db/m3/src/m3em/generated/proto/m3em"
	"github.com/m3db/m3/src/m3em/os/exec"
	xgrpc "github.com/m3db/m3/src/m3em/x/grpc"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xtcp "github.com/m3db/m3/src/x/tcp"

	"github.com/pborman/getopt"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

// Run runs a m3em_agent process
func Run() {
	var (
		configFile = getopt.StringLong("config-file", 'f', "", "Configuration file")
	)
	getopt.Parse()
	if len(*configFile) == 0 {
		getopt.Usage()
		return
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("unable to create logger: %v", err)
	}

	var conf m3emconfig.Configuration
	err = xconfig.LoadFile(&conf, *configFile, xconfig.Options{})
	if err != nil {
		logger.Fatal("unable to read configuration file", zap.Error(err))
	}

	xconfig.WarnOnDeprecation(conf, logger)

	// pprof server
	go func() {
		if err := http.ListenAndServe(conf.Server.DebugAddress, nil); err != nil {
			logger.Fatal("unable to serve debug server", zap.Error(err))
		}
	}()
	logger.Info("serving pprof endpoints", zap.String("address", conf.Server.DebugAddress))

	reporter, err := conf.Metrics.M3.NewReporter()
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Error(err))
	}
	scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
		Prefix:         conf.Metrics.Prefix,
		CachedReporter: reporter,
	}, time.Second)
	defer scopeCloser.Close()

	listener, err := xtcp.NewTCPListener(conf.Server.ListenAddress, 3*time.Minute)
	if err != nil {
		logger.Fatal("could not create TCP Listener", zap.Error(err))
	}

	iopts := instrument.NewOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetTimerOptions(instrument.TimerOptions{StandardSampleRate: conf.Metrics.SampleRate})

	agentOpts := agent.NewOptions(iopts).
		SetWorkingDirectory(conf.Agent.WorkingDir).
		SetInitHostResourcesFn(hostFnMaker("startup", conf.Agent.StartupCmds, logger)).
		SetReleaseHostResourcesFn(hostFnMaker("release", conf.Agent.ReleaseCmds, logger)).
		SetExecGenFn(execGenFn).
		SetEnvMap(envMap(logger, conf.Agent.TestEnvVars))

	agentService, err := agent.New(agentOpts)
	if err != nil {
		logger.Fatal("unable to create agentService", zap.Error(err))
	}

	var serverCreds credentials.TransportCredentials
	if tls := conf.Server.TLS; tls != nil {
		logger.Info("using provided TLS config", zap.Any("config", tls))
		serverCreds, err = tls.Credentials()
		if err != nil {
			logger.Fatal("unable to create transport credentials", zap.Error(err))
		}
	}
	server := xgrpc.NewServer(serverCreds)
	m3em.RegisterOperatorServer(server, agentService)
	logger.Info("serving agent endpoints", zap.Stringer("address", listener.Addr()))
	if err := server.Serve(listener); err != nil {
		logger.Fatal("could not serve", zap.Error(err))
	}
}

// HACK(prateek): YAML un-marshalling returns lower-case keys for everything,
// setting to upper-case explicitly here.
func envMap(logger *zap.Logger, envMap map[string]string) exec.EnvMap {
	logger.Warn("transforming keys set in YAML for testEnvVars to UPPER_CASE")
	if envMap == nil {
		return nil
	}
	newMap := make(map[string]string, len(envMap))
	for key, value := range envMap {
		newMap[strings.ToUpper(key)] = value
	}
	return exec.EnvMap(newMap)
}

func hostFnMaker(mode string, cmds []m3emconfig.ExecCommand, logger *zap.Logger) agent.HostResourcesFn {
	return func() error {
		if len(cmds) == 0 {
			logger.Info("no commands specified, skipping.", zap.String("mode", mode))
			return nil
		}
		for _, cmd := range cmds {
			osCmd := oexec.Command(cmd.Path, cmd.Args...)
			logger.Info("attempting to execute", zap.String("mode", mode), zap.Any("command", osCmd))
			output, err := osCmd.CombinedOutput()
			if err != nil {
				logger.Error("unable to execute cmd", zap.Error(err))
				return err
			}
			logger.Info("successfully ran cmd", zap.String("output", string(output)))
		}
		return nil
	}
}

func execGenFn(binary string, config string) (string, []string) {
	return binary, []string{"-f", config}
}
