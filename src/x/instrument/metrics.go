// Copyright (c) 2019 Uber Technologies, Inc.
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

package instrument

import (
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
)

const (
	// wait defines the time to wait between emitting the value of the Gauge
	// again.
	wait = 10 * time.Second
)

// BootstrapGaugeEmitter is used to emit a Gauge informing about the order of
// the bootstrappers.
type BootstrapGaugeEmitter struct {
	sync.Mutex
	running bool
	doneCh  chan bool
	scope   tally.Scope
	gauge   tally.Gauge
}

// NewBootstrapGaugeEmitter returns a BootstrapGaugeEmitter with Gauge that is
// created from the passed scope and tagged according to the passed
// bootstrappers. The value of the Gauge will be set to 1 on initialization.
func NewBootstrapGaugeEmitter(scope tally.Scope, bs []string) *BootstrapGaugeEmitter {
	return &BootstrapGaugeEmitter{
		running: false,
		doneCh:  make(chan bool, 1),
		scope:   scope,
		gauge:   newGauge(scope, bs),
	}
}

func newGauge(scope tally.Scope, bs []string) tally.Gauge {
	tags := make(map[string]string, len(bs))
	for i, v := range bs {
		tags[fmt.Sprintf("bootstrapper%d", i)] = v
	}
	g := scope.Tagged(tags).Gauge("bootstrapper_bootstrappers")
	g.Update(1)

	return g
}

// Start starts a goroutine that continuously emits the value of the Gauge.
func (bge *BootstrapGaugeEmitter) Start() error {
	bge.Lock()
	defer bge.Unlock()

	if bge.running {
		return fmt.Errorf("already running")
	}
	bge.running = true
	go func() {
		for {
			select {
			case <-bge.doneCh:
				return
			default:
				bge.Lock()
				bge.gauge.Update(1)
				bge.Unlock()
				time.Sleep(wait)
			}
		}
	}()
	return nil
}

// UpdateBootstrappers updates the Gauge with new tags according to passed
// bootstrappers while setting the old Gauge to 0 first. This will emit
// new metric with the same metric name but different tags.
func (bge *BootstrapGaugeEmitter) UpdateBootstrappers(bs []string) error {
	bge.Lock()
	defer bge.Unlock()

	if !bge.running {
		return fmt.Errorf("not started yet")
	}

	bge.gauge.Update(0)
	bge.gauge = newGauge(bge.scope, bs)

	return nil
}

// Close stops emitting the Gauge.
func (bge *BootstrapGaugeEmitter) Close() error {
	bge.Lock()
	defer bge.Unlock()

	if !bge.running {
		return fmt.Errorf("not started yet")
	}

	bge.running = false
	close(bge.doneCh)

	return nil
}
