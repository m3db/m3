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
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/m3db/m3ctl/auth"
	"github.com/m3db/m3ctl/server/http"
	"github.com/m3db/m3ctl/service/health"
	"github.com/m3db/m3ctl/service/r2"
	"github.com/m3db/m3ctl/services/r2ctl/config"
	"github.com/m3db/m3x/clock"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
)

const (
	portEnvVar              = "R2CTL_PORT"
	r2apiPrefix             = "/r2/v1/"
	gracefulShutdownTimeout = 15 * time.Second
)

func main() {
	configFile := flag.String("f", "config/base.yaml", "configuration file")
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, *configFile); err != nil {
		fmt.Printf("error loading config file: %v\n", err)
		os.Exit(1)
	}

	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Printf("error creating logger: %v\n", err)
		os.Exit(1)
	}

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
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(cfg.Metrics.SampleRate()).
		SetReportInterval(cfg.Metrics.ReportInterval())

	listenAddr := fmt.Sprintf("%s:%d", cfg.HTTP.Host, cfg.HTTP.Port)
	serverOpts := cfg.HTTP.NewServerOptions(instrumentOpts)

	store, err := cfg.Store.NewR2Store(instrumentOpts)
	if err != nil {
		logger.Fatalf("error initializing backing store: %v", err)
	}

	authService := auth.NewNoopAuth()
	if cfg.Auth != nil {
		authService = cfg.Auth.NewSimpleAuth()
	}

	r2Service := r2.NewService(
		r2apiPrefix,
		authService,
		store,
		instrumentOpts,
		clock.NewOptions(),
	)
	healthService := health.NewService(instrumentOpts)
	server := http.NewServer(listenAddr, serverOpts, r2Service, healthService)

	logger.Infof("starting HTTP server on: %s", listenAddr)
	if err := server.ListenAndServe(); err != nil {
		logger.Fatalf("could not start serving traffic: %v", err)
	}

	// Handle interrupts.
	logger.Warnf("interrupt: %v", interrupt())

	doneCh := make(chan struct{})
	go func() {
		server.Close()
		logger.Infof("HTTP server closed")
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
