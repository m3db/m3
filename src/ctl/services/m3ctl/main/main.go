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
	"syscall"
	"time"

	"github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3ctl/server"
	"github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
)

type serverConfig struct {
	// ListenAddress is the HTTP server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// ReadTimeout is the HTTP server read timeout.
	ReadTimeout time.Duration `yaml:"readTimeout"`

	// WriteTimeout HTTP server write timeout.
	WriteTimeout time.Duration `yaml:"writeTimeout"`
}

// NewHTTPServerOptions create a new set of http server options.
func (c *serverConfig) NewHTTPServerOptions(
	instrumentOpts instrument.Options,
) server.Options {
	opts := server.NewOptions().SetInstrumentOptions(instrumentOpts)
	if c.ReadTimeout != 0 {
		opts = opts.SetReadTimeout(c.ReadTimeout)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}

	return opts
}

type kvClientConfiguration struct {
	Etcd *etcd.Configuration `yaml:"etcd"`
}

type configuration struct {
	// KVStore Config
	KVClient kvClientConfiguration `yaml:"kvClient" validate:"nonzero"`

	// Logging configuration.
	Logging xlog.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// HTTP server configuration.
	HTTP serverConfig `yaml:"http"`
}

func main() {
	configFile := flag.String("f", "", "configuration file")
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg configuration
	if err := xconfig.LoadFile(&cfg, *configFile); err != nil {
		fmt.Printf("error loading config file: %v\n", err)
		os.Exit(1)
	}

	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Printf("error creating logger: %v\n", err)
		os.Exit(1)
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

	listenAddr := cfg.HTTP.ListenAddress
	serverOpts := cfg.HTTP.NewHTTPServerOptions(instrumentOpts)

	doneCh := make(chan struct{})

	s, err := server.NewServer(listenAddr, serverOpts)
	if err != nil {
		logger.Fatalf("could not create m3ctl server: %v", err)
	}

	go func() {
		go func() {
			logger.Infof("Starting http server on: %s", listenAddr)
			if err = s.ListenAndServe(); err != nil {
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
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
