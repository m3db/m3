// Copyright (c) 2021  Uber Technologies, Inc.
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

package inprocess

import (
	aggcfg "github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	dbnodecfg "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	coordcfg "github.com/m3db/m3/src/cmd/services/m3query/config"
)

// DBNodeStartFn is a custom function that can be used to start a DB node.
// Function must return a channel for interrupting the server and
// a channel for receiving notifications that the server has shut down.
type DBNodeStartFn func(cfg *dbnodecfg.Configuration) (chan<- error, <-chan struct{})

// CoordinatorStartFn is a custom function that can be used to start a coordinator.
// Function must return a channel for interrupting the server and
// a channel for receiving notifications that the server has shut down.
type CoordinatorStartFn func(cfg *coordcfg.Configuration) (chan<- error, <-chan struct{})

// AggregatorStartFn is a custom function that can be used to start an aggregator.
// Function must return a channel for interrupting the server and
// a channel for receiving notifications that the server has shut down.
type AggregatorStartFn func(cfg *aggcfg.Configuration) (chan<- error, <-chan struct{})
