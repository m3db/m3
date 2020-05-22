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
	"github.com/m3db/m3/src/x/instrument"
	xnet "github.com/m3db/m3/src/x/net"
	"github.com/m3db/m3/src/x/panicmon"

	"go.uber.org/zap"
)

type multiProcessResult struct {
	isParent       bool
	childExitOK    bool
	childExitCodes []panicmon.StatusCode

	cfg          config.Configuration
	logger       *zap.Logger
	listenerOpts xnet.ListenerOptions
}

func runMultiProcess(
	cfg config.Configuration,
	logger *zap.Logger,
	listenerOpts xnet.ListenerOptions,
) (multiProcessResult, error) {
	r := multiProcessResult{
		cfg:          cfg,
		logger:       logger,
		listenerOpts: listenerOpts,
	}

	multiProcessInstance := os.Getenv(multiProcessInstanceEnvVar)
	if multiProcessInstance != "" {
		// Otherwise is already a sub-process, make sure listener options
		// will reuse ports so multiple processes can listen on the same
		// listen port.
		r.listenerOpts = xnet.NewListenerOptions(xnet.ListenerReusePort(true))

		// Configure instrumentation to be correctly partitioned.
		r.logger = r.logger.With(zap.String("processID", multiProcessInstance))

		instance, err := strconv.Atoi(multiProcessInstance)
		if err != nil {
			r.logger.Fatal("multi-process process ID is non-integer", zap.Error(err))
		}

		// Set the root scope multi-process process ID.
		if r.cfg.Metrics.RootScope == nil {
			r.cfg.Metrics.RootScope = &instrument.ScopeConfiguration{}
		}
		if r.cfg.Metrics.RootScope.CommonTags == nil {
			r.cfg.Metrics.RootScope.CommonTags = make(map[string]string)
		}
		r.cfg.Metrics.RootScope.CommonTags[multiProcessMetricTagID] = multiProcessInstance

		// Listen on a different Prometheus metrics handler listen port.
		if r.cfg.Metrics.PrometheusReporter != nil && r.cfg.Metrics.PrometheusReporter.ListenAddress != "" {
			// Simply increment the listen address port by instance number.
			host, port, err := net.SplitHostPort(cfg.Metrics.PrometheusReporter.ListenAddress)
			if err != nil {
				return r, fmt.Errorf("could not split host:port for metrics reporter: %v", err)
			}

			portValue, err := strconv.Atoi(port)
			if err != nil {
				return r, fmt.Errorf("prometheus metric reporter port is non-integer: %v", err)
			}
			if portValue > 0 {
				// Increment port value by process ID if valid port.
				address := net.JoinHostPort(host, strconv.Itoa(portValue+instance-1))
				cfg.Metrics.PrometheusReporter.ListenAddress = address
				r.logger.Info("multi-process prometheus metrics reporter listen address configured",
					zap.String("address", address))
			}
		}
	}

	r.logger = r.logger.With(zap.String("processID", multiProcessParentInstance))

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

	r.logger.Info("starting multi-process subprocesses",
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
				r.logger.Error("process failed", zap.Error(err))
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

	r.isParent = true
	r.childExitOK = exitNotOk == 0
	r.childExitCodes = statuses

	if exitNotOk > 0 {
		return r, fmt.Errorf("child exit codes not ok: %v", statuses)
	}
	return r, nil
}
