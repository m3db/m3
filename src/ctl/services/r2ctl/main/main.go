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

	"github.com/m3db/m3ctl/auth"
	"github.com/m3db/m3ctl/health"
	"github.com/m3db/m3ctl/r2"
	"github.com/m3db/m3ctl/services/r2ctl/config"
	"github.com/m3db/m3ctl/services/r2ctl/server"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
)

const (
	portEnvVar  = "R2CTL_PORT"
	r2apiPrefix = "/r2/v1/"
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
			logger.Infof("Using env supplied port var: %s=%d", portEnvVar, p)
			cfg.HTTP.Port = p
		} else {
			logger.Fatalf("%s (%s) is not a valid port number", envPort, portEnvVar)
		}
	}

	if cfg.HTTP.Port == 0 {
		logger.Fatalf("No valid port configured. Can't start.")
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
	serverOpts := cfg.HTTP.NewHTTPServerOptions(instrumentOpts)

	store, err := cfg.Store.NewR2Store(instrumentOpts)
	if err != nil {
		logger.Fatalf("error initializing backing store: %v", err)
	}

	authService := auth.NewNoopAuth()
	if cfg.Auth != nil {
		authService = cfg.Auth.NewSimpleAuth()
	}

	r2ApiServer := server.NewServer(
		listenAddr,
		serverOpts,
		r2.NewService(instrumentOpts, r2apiPrefix, authService, store),
		health.NewService(instrumentOpts),
	)

	doneCh := make(chan struct{})
	go func() {
		go func() {
			logger.Infof("Starting http server on: %s", listenAddr)
			if err = r2ApiServer.ListenAndServe(); err != nil {
				logger.Fatalf("could not start serving traffic: %v", err)
			}
		}()
		<-doneCh
		logger.Debug("server stopped")
	}()

	// Handle interrupts.
	logger.Warnf("interrupt: %v", interrupt())

	close(doneCh)
}

func interrupt() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
