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

package executor

import (
	"time"

	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/instrument"
)

type engineOptions struct {
	instrumentOpts   instrument.Options
	store            storage.Storage
	parseOptions     promql.ParseOptions
	lookbackDuration time.Duration
}

// NewEngineOptions returns a new instance of options used to create an engine.
func NewEngineOptions() EngineOptions {
	return &engineOptions{
		parseOptions: promql.NewParseOptions(),
	}
}

func (o *engineOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *engineOptions) SetInstrumentOptions(v instrument.Options) EngineOptions {
	opts := *o
	opts.instrumentOpts = v
	return &opts
}

func (o *engineOptions) Store() storage.Storage {
	return o.store
}

func (o *engineOptions) SetStore(v storage.Storage) EngineOptions {
	opts := *o
	opts.store = v
	return &opts
}

func (o *engineOptions) LookbackDuration() time.Duration {
	return o.lookbackDuration
}

func (o *engineOptions) SetLookbackDuration(v time.Duration) EngineOptions {
	opts := *o
	opts.lookbackDuration = v
	return &opts
}

func (o *engineOptions) ParseOptions() promql.ParseOptions {
	return o.parseOptions
}

func (o *engineOptions) SetParseOptions(p promql.ParseOptions) EngineOptions {
	opts := *o
	opts.parseOptions = p
	return &opts
}
