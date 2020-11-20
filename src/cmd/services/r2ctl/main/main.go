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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/m3db/m3/src/cmd/services/r2ctl/config"
	"github.com/m3db/m3/src/ctl/auth"
	"github.com/m3db/m3/src/ctl/server/http"
	"github.com/m3db/m3/src/ctl/service/health"
	"github.com/m3db/m3/src/ctl/service/r2"
	"github.com/m3db/m3/src/x/clock"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/configflag"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	portEnvVar              = "R2CTL_PORT"
	r2apiPrefix             = "/r2/v1/"
	gracefulShutdownTimeout = 15 * time.Second
)

func main() {
	configOpts := configflag.Options{
		ConfigFiles: configflag.FlagStringSlice{Value: []string{"m3ctl.yml"}},
	}

	configOpts.Register()

	flag.Parse()

	var cfg config.Configuration
	if err := configOpts.MainLoad(&cfg, xconfig.Options{}); err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	rawLogger, err := cfg.Logging.BuildLogger()
	if err != nil {
		log.Fatalf("error creating logger: %v", err)
	}
	defer rawLogger.Sync()

	xconfig.WarnOnDeprecation(cfg, rawLogger)

	logger := rawLogger.Sugar()
	envPort := os.Getenv(portEnvVar)
	if envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			logger.Infof("using env supplied port var: %s=%d", portEnvVar, p)
			cfg.HTTP.Port = p
		} else {
			logger.Fatalf("%s (%s) is not a valid port number", envPort, portEnvVar)
		}
	}

	if cfg.HTTP.Port == 0 {
		logger.Fatalf("no valid port configured. Can't start.")
	}

	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatalf("error creating metrics root scope: %v", err)
	}
	defer closer.Close()

	instrumentOpts := instrument.NewOptions().
		SetLogger(rawLogger).
		SetMetricsScope(scope).
		SetTimerOptions(instrument.TimerOptions{StandardSampleRate: cfg.Metrics.SampleRate()}).
		SetReportInterval(cfg.Metrics.ReportInterval())

	// Create R2 store.
	storeScope := scope.SubScope("r2-store")
	store, err := cfg.Store.NewR2Store(instrumentOpts.SetMetricsScope(storeScope))
	if err != nil {
		logger.Fatalf("error initializing backing store: %v", err)
	}

	// Create R2 service.
	authService := auth.NewNoopAuth()
	if cfg.Auth != nil {
		authService = cfg.Auth.NewSimpleAuth()
	}
	r2ServiceScope := scope.Tagged(map[string]string{
		"service-name": "r2",
	})
	r2ServiceInstrumentOpts := instrumentOpts.SetMetricsScope(r2ServiceScope)
	r2Service := r2.NewService(
		r2apiPrefix,
		authService,
		store,
		r2ServiceInstrumentOpts,
		clock.NewOptions(),
	)

	// Create health service.
	healthServiceScope := scope.Tagged(map[string]string{
		"service-name": "health",
	})
	healthServiceInstrumentOpts := instrumentOpts.SetMetricsScope(healthServiceScope)
	healthService := health.NewService(healthServiceInstrumentOpts)

	// Create HTTP server.
	listenAddr := fmt.Sprintf("%s:%d", cfg.HTTP.Host, cfg.HTTP.Port)
	httpServerScope := scope.Tagged(map[string]string{
		"server-type": "http",
	})
	httpServerInstrumentOpts := instrumentOpts.SetMetricsScope(httpServerScope)
	httpServerOpts := cfg.HTTP.NewServerOptions(httpServerInstrumentOpts)
	server, err := http.NewServer(listenAddr, httpServerOpts, r2Service, healthService)
	if err != nil {
		logger.Fatalf("could not create new server: %v", err)
	}

	logger.Infof("starting http server on: %s", listenAddr)
	if err := server.ListenAndServe(); err != nil {
		logger.Fatalf("could not start serving traffic: %v", err)
	}

	// Handle interrupts.
	logger.Warnf("interrupt: %v", interrupt())

	doneCh := make(chan struct{})
	go func() {
		server.Close()
		logger.Infof("http server closed")
		doneCh <- struct{}{}
	}()

	select {
	case <-doneCh:
		logger.Infof("clean shutdown")
	case <-time.After(gracefulShutdownTimeout):
		logger.Warnf("forced shutdown due to timeout after waiting for %v", gracefulShutdownTimeout)
	}
}

func interrupt() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
