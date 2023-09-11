// Copyright (c) 2020 Uber Technologies, Inc.
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

package server

import (
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	xnet "github.com/m3db/m3/src/x/net"
	"github.com/m3db/m3/src/x/panicmon"

	"go.uber.org/zap"
)

const (
	multiProcessInstanceEnvVar = "MULTIPROCESS_INSTANCE"
	multiProcessParentInstance = "0"
	multiProcessMetricTagID    = "multiprocess_id"
	goMaxProcsEnvVar           = "GOMAXPROCS"
)

type multiProcessResult struct {
	isParentCleanExit bool

	cfg          config.Configuration
	logger       *zap.Logger
	listenerOpts xnet.ListenerOptions
	commonLabels map[string]string
}

func multiProcessProcessID() string {
	return os.Getenv(multiProcessInstanceEnvVar)
}

func multiProcessRun(
	cfg config.Configuration,
	logger *zap.Logger,
	listenerOpts xnet.ListenerOptions,
) (multiProcessResult, error) {
	multiProcessInstance := multiProcessProcessID()
	if multiProcessInstance != "" {
		// Otherwise is already a sub-process, make sure listener options
		// will reuse ports so multiple processes can listen on the same
		// listen port.
		listenerOpts = xnet.NewListenerOptions(xnet.ListenerReusePort(true))

		// Configure instrumentation to be correctly partitioned.
		logger = logger.With(zap.String("processID", multiProcessInstance))

		instance, err := strconv.Atoi(multiProcessInstance)
		if err != nil {
			return multiProcessResult{},
				fmt.Errorf("multi-process process ID is non-integer: %v", err)
		}

		metrics := cfg.MetricsOrDefault()
		// Listen on a different Prometheus metrics handler listen port.
		if metrics.PrometheusReporter != nil && metrics.PrometheusReporter.ListenAddress != "" {
			// Simply increment the listen address port by instance number
			host, port, err := net.SplitHostPort(metrics.PrometheusReporter.ListenAddress)
			if err != nil {
				return multiProcessResult{},
					fmt.Errorf("could not split host:port for metrics reporter: %v", err)
			}

			portValue, err := strconv.Atoi(port)
			if err != nil {
				return multiProcessResult{},
					fmt.Errorf("prometheus metric reporter port is non-integer: %v", err)
			}
			if portValue > 0 {
				// Increment port value by process ID if valid port.
				address := net.JoinHostPort(host, strconv.Itoa(portValue+instance-1))
				metrics.PrometheusReporter.ListenAddress = address
				logger.Info("multi-process prometheus metrics reporter listen address configured",
					zap.String("address", address))
			}
		}
		return multiProcessResult{
			cfg:          cfg,
			logger:       logger,
			listenerOpts: listenerOpts,
			// Ensure multi-process process ID is set on all metrics.
			commonLabels: map[string]string{multiProcessMetricTagID: multiProcessInstance},
		}, nil
	}

	logger = logger.With(zap.String("processID", multiProcessParentInstance))

	perCPU := defaultPerCPUMultiProcess
	if v := cfg.MultiProcess.PerCPU; v > 0 {
		// Allow config to override per CPU factor for determining count.
		perCPU = v
	}

	count := int(math.Max(1, float64(runtime.NumCPU())*perCPU))
	if v := cfg.MultiProcess.Count; v > 0 {
		// Allow config to override per CPU auto derived count.
		count = v
	}

	logger.Info("starting multi-process subprocesses",
		zap.Int("count", count))
	var (
		wg       sync.WaitGroup
		statuses = make([]panicmon.StatusCode, count)
	)
	for i := 0; i < count; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			newEnv := []string{
				fmt.Sprintf("%s=%d", multiProcessInstanceEnvVar, i+1),
			}

			// Set GOMAXPROCS correctly if configured.
			if v := cfg.MultiProcess.GoMaxProcs; v > 0 {
				newEnv = append(newEnv,
					fmt.Sprintf("%s=%d", goMaxProcsEnvVar, v))
			}

			newEnv = append(newEnv, os.Environ()...)

			exec := panicmon.NewExecutor(panicmon.ExecutorOptions{
				Env: newEnv,
			})
			status, err := exec.Run(os.Args)
			if err != nil {
				logger.Error("process failed", zap.Error(err))
			}

			statuses[i] = status
		}()
	}

	wg.Wait()

	exitNotOk := 0
	for _, v := range statuses {
		if v != 0 {
			exitNotOk++
		}
	}

	if exitNotOk > 0 {
		return multiProcessResult{},
			fmt.Errorf("child exit codes not ok: %v", statuses)
	}

	return multiProcessResult{
		isParentCleanExit: true,
	}, nil
}
