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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
)

const (
	// StringListEmitterWaitInterval defines the time to wait between emitting the value of the Gauge
	// again.
	stringListEmitterWaitInterval = 10 * time.Second
)

var (
	errStringListEmitterAlreadyRunning = errors.New("can't start bootstrapGaugeEmmitter: already running")
	errStringListEmitterNotStarted     = errors.New("bootrapGaugeEmitter is not running")
)

// StringListEmitter emits a gauge where its tags indicate the order of a
// list of strings.
type StringListEmitter struct {
	sync.Mutex
	running   bool
	doneCh    chan bool
	scope     tally.Scope
	gauge     tally.Gauge
	name      string
	tagPrefix string
}

// NewStringListEmitter returns a StringListEmitter. The name and tagPrefix
// arguments are used by the Start() and UpdateStringList() function to set
// the name and tags on the gauge.
func NewStringListEmitter(scope tally.Scope, name, tagPrefix string) *StringListEmitter {
	return &StringListEmitter{
		running:   false,
		doneCh:    make(chan bool, 1),
		scope:     scope,
		gauge:     nil,
		name:      name,
		tagPrefix: tagPrefix,
	}
}

func (bge *StringListEmitter) newGauge(scope tally.Scope, sl []string) tally.Gauge {
	tags := make(map[string]string, len(sl))
	for i, v := range sl {
		tags[fmt.Sprintf("%s%d", bge.tagPrefix, i)] = v
	}
	g := scope.Tagged(tags).Gauge(bge.name)
	g.Update(1)

	return g
}

// Start starts a goroutine that continuously emits the value of the gauge.
func (bge *StringListEmitter) Start(sl []string) error {
	bge.Lock()
	defer bge.Unlock()

	if bge.running {
		return errStringListEmitterAlreadyRunning
	}

	bge.gauge = bge.newGauge(bge.scope, sl)

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
				time.Sleep(stringListEmitterWaitInterval)
			}
		}
	}()
	return nil
}

// UpdateStringList updates the gauge with new tags according to the passed
// list of strings. It will first set the old gauge to 0, then emit a
// new metric with the same name but different tags.
func (bge *StringListEmitter) UpdateStringList(sl []string) error {
	bge.Lock()
	defer bge.Unlock()

	if !bge.running {
		return errStringListEmitterNotStarted
	}

	if bge.gauge != nil {
		bge.gauge.Update(0)
	}

	bge.gauge = bge.newGauge(bge.scope, sl)

	return nil
}

// Close stops emitting the gauge.
func (bge *StringListEmitter) Close() error {
	bge.Lock()
	defer bge.Unlock()

	if !bge.running {
		return errStringListEmitterNotStarted
	}

	bge.running = false
	close(bge.doneCh)

	return nil
}
