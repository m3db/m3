// Copyright (c) 2018 Uber Technologies, Inc.
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
	"time"

	"github.com/m3db/m3/src/m3nsch"
	"github.com/m3db/m3/src/m3nsch/coordinator"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/pborman/getopt"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	validator "gopkg.in/validator.v2"
)

type m3nschConfig struct {
	Endpoints  []string       `yaml:"endpoints" validate:"min=1"`
	Workload   workloadConfig `yaml:"workload"`
	TargetZone string         `yaml:"targetZone" validate:"nonzero"`
	TargetEnv  string         `yaml:"targetZone" validate:"nonzero"`
}

type workloadConfig struct {
	TimeOffsetMins   int    `yaml:"timeOffset"`
	Namespace        string `yaml:"namespace" validate:"nonzero"`
	MetricNamePrefix string `yaml:"metricPrefix" validate:"nonzero"`
	Cardinality      int    `yaml:"cardinality" validate:"min=100"`
	IngressQPS       int    `yaml:"ingressQPS" validate:"min=10"`
}

func (wc workloadConfig) toM3nschType() m3nsch.Workload {
	return m3nsch.Workload{
		BaseTime:     time.Now().Add(time.Duration(wc.TimeOffsetMins) * time.Minute),
		Namespace:    wc.Namespace,
		MetricPrefix: wc.MetricNamePrefix,
		Cardinality:  wc.Cardinality,
		IngressQPS:   wc.IngressQPS,
	}
}

func main() {
	var (
		configFile = getopt.StringLong("config", 'c', "", "Configuration file")
		token      = getopt.StringLong("token", 't', "", "Identifier Token")
		duration   = getopt.DurationLong("duration", 'd', time.Duration(0), "Workload Duration")
		force      = getopt.BoolLong("force", 'f', "false", "Force")
	)
	getopt.Parse()
	if len(*configFile) == 0 || len(*token) == 0 || duration.Seconds() == 0 {
		getopt.Usage()
		return
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to read configuration file: %v", err)
	}

	var (
		iopts = instrument.NewOptions().SetLogger(logger)
		opts  = coordinator.NewOptions(iopts)
	)
	conf, err := readConfiguration(*configFile)
	if err != nil {
		logger.Fatal("unable to read configuration file", zap.Error(err))
	}

	coord, err := coordinator.New(opts, conf.Endpoints)
	if err != nil {
		logger.Fatal("unable to create coordinator", zap.Error(err))
	}

	workload := conf.Workload.toM3nschType()
	err = coord.Init(*token, workload, *force, conf.TargetZone, conf.TargetEnv)
	if err != nil {
		logger.Fatal("unable to init coordinator", zap.Error(err))
	}
	defer coord.Teardown()

	if err := coord.Start(); err != nil {
		logger.Fatal("unable to start coordinator", zap.Error(err))
	}
	defer coord.Stop()

	stopTime := time.Now().Add(*duration)
	for time.Now().Before(stopTime) {
		logger.Sugar().Infof("status [ time = %v ]", time.Now().String())
		statusMap, err := coord.Status()
		if err != nil {
			logger.Sugar().Fatalf("unable to retrieve status: %v", err)
		}
		logStatus(logger, statusMap)
		time.Sleep(time.Second * 5)
	}
}

func logStatus(logger *zap.Logger, statusMap map[string]m3nsch.AgentStatus) {
	for endpoint, status := range statusMap {
		token := status.Token
		if token == "" {
			token = "<undefined>"
		}
		logger.Sugar().Infof("[%v] MaxQPS: %d, Status: %v, Token: %v, Workload: %+v",
			endpoint, status.MaxQPS, status.Status, token, status.Workload)
	}
}

func readConfiguration(filename string) (m3nschConfig, error) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(filename)

	var conf m3nschConfig
	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}

	if err := viper.Unmarshal(&conf); err != nil {
		return conf, err
	}

	if err := validator.Validate(conf); err != nil {
		return conf, err
	}

	return conf, nil
}
