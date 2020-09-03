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

package client

import (
	"errors"

	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errM3MsgOptionsNoProducer = errors.New("no producer set")

	// defaultM3MsgTimerOptions by defaults ensures to use
	// low overhead timers for M3Msg clients (can't do this
	// for legacy clients since people depend on those stats being
	// non-histograms).
	defaultM3MsgTimerOptions = instrument.TimerOptions{
		Type:             instrument.HistogramTimerType,
		HistogramBuckets: instrument.DefaultHistogramTimerHistogramBuckets(),
	}
)

// M3MsgOptions is a set of M3Msg client options.
type M3MsgOptions interface {
	// Validate validates the M3Msg client options.
	Validate() error

	// SetProducer sets the producer.
	SetProducer(value producer.Producer) M3MsgOptions

	// Producer gets the producer.
	Producer() producer.Producer

	// SetTimerOptions sets the instrument timer options.
	SetTimerOptions(value instrument.TimerOptions) M3MsgOptions

	// TimerOptions gets the instrument timer options.
	TimerOptions() instrument.TimerOptions
}

type m3msgOptions struct {
	producer     producer.Producer
	timerOptions instrument.TimerOptions
}

// NewM3MsgOptions returns a new set of M3Msg options.
func NewM3MsgOptions() M3MsgOptions {
	return &m3msgOptions{
		timerOptions: defaultM3MsgTimerOptions,
	}
}

func (o *m3msgOptions) Validate() error {
	if o.producer == nil {
		return errM3MsgOptionsNoProducer
	}
	return nil
}

func (o *m3msgOptions) SetProducer(value producer.Producer) M3MsgOptions {
	opts := *o
	opts.producer = value
	return &opts
}

func (o *m3msgOptions) Producer() producer.Producer {
	return o.producer
}

func (o *m3msgOptions) SetTimerOptions(value instrument.TimerOptions) M3MsgOptions {
	opts := *o
	opts.timerOptions = value
	return &opts
}

func (o *m3msgOptions) TimerOptions() instrument.TimerOptions {
	return o.timerOptions
}
