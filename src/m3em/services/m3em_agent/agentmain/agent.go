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
	// pprof import
	_ "net/http/pprof"

	"net/http"
	"os"
	oexec "os/exec"
	"strings"
	"time"

	"github.com/m3db/m3em/agent"
	"github.com/m3db/m3em/generated/proto/m3em"
	"github.com/m3db/m3em/os/exec"
	m3emconfig "github.com/m3db/m3em/services/m3em_agent/config"
	xgrpc "github.com/m3db/m3em/x/grpc"

	"github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	xtcp "github.com/m3db/m3x/tcp"
	"github.com/pborman/getopt"
	"github.com/uber-go/tally"
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

	logger := xlog.NewLogger(os.Stdout)

	var conf m3emconfig.Configuration
	err := xconfig.LoadFile(&conf, *configFile)
	if err != nil {
		logger.Fatalf("unable to read configuration file: %v", err.Error())
	}

	// pprof server
	go func() {
		if err := http.ListenAndServe(conf.Server.DebugAddress, nil); err != nil {
			logger.Fatalf("unable to serve debug server: %v", err)
		}
	}()
	logger.Infof("serving pprof endpoints at: %v", conf.Server.DebugAddress)

	reporter, err := conf.Metrics.M3.NewReporter()
	if err != nil {
		logger.Fatalf("could not connect to metrics: %v", err)
	}
	scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
		Prefix:         conf.Metrics.Prefix,
		CachedReporter: reporter,
	}, time.Second)
	defer scopeCloser.Close()

	listener, err := xtcp.NewTCPListener(conf.Server.ListenAddress, 3*time.Minute)
	if err != nil {
		logger.Fatalf("could not create TCP Listener: %v", err)
	}

	iopts := instrument.NewOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(conf.Metrics.SampleRate)

	agentOpts := agent.NewOptions(iopts).
		SetWorkingDirectory(conf.Agent.WorkingDir).
		SetInitHostResourcesFn(hostFnMaker("startup", conf.Agent.StartupCmds, logger)).
		SetReleaseHostResourcesFn(hostFnMaker("release", conf.Agent.ReleaseCmds, logger)).
		SetExecGenFn(execGenFn).
		SetEnvMap(envMap(logger, conf.Agent.TestEnvVars))

	agentService, err := agent.New(agentOpts)
	if err != nil {
		logger.Fatalf("unable to create agentService: %v", err)
	}

	var serverCreds credentials.TransportCredentials
	if tls := conf.Server.TLS; tls != nil {
		logger.Infof("using provided TLS config: %+v", tls)
		serverCreds, err = tls.Credentials()
		if err != nil {
			logger.Fatalf("unable to create transport credentials: %v", err)
		}
	}
	server := xgrpc.NewServer(serverCreds)
	m3em.RegisterOperatorServer(server, agentService)
	logger.Infof("serving agent endpoints at %v", listener.Addr().String())
	if err := server.Serve(listener); err != nil {
		logger.Fatalf("could not serve: %v", err)
	}
}

// HACK(prateek): YAML un-marshalling returns lower-case keys for everything,
// setting to upper-case explicitly here.
func envMap(logger xlog.Logger, envMap map[string]string) exec.EnvMap {
	logger.Warnf("Transformings keys set in YAML for testEnvVars to UPPER_CASE")
	if envMap == nil {
		return nil
	}
	newMap := make(map[string]string, len(envMap))
	for key, value := range envMap {
		newMap[strings.ToUpper(key)] = value
	}
	return exec.EnvMap(newMap)
}

func hostFnMaker(mode string, cmds []m3emconfig.ExecCommand, logger xlog.Logger) agent.HostResourcesFn {
	return func() error {
		if len(cmds) == 0 {
			logger.Infof("no %s commands specified, skipping.", mode)
			return nil
		}
		for _, cmd := range cmds {
			osCmd := oexec.Command(cmd.Path, cmd.Args...)
			logger.Infof("attempting to execute %s cmd: %+v", mode, osCmd)
			output, err := osCmd.CombinedOutput()
			if err != nil {
				logger.Errorf("unable to execute cmd, err: %v", err)
				return err
			}
			logger.Infof("successfully ran cmd, output: [%v]", string(output))
		}
		return nil
	}
}

func execGenFn(binary string, config string) (string, []string) {
	return binary, []string{"-f", config}
}
