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

package node

import (
	"fmt"
	"time"

	xclock "github.com/m3db/m3x/clock"
)

const (
	defaultEnabled           = false
	defaultHeartbeatInterval = 10 * time.Second
	defaultCheckInterval     = 2 * time.Second
	defaultHeartbeatTimeout  = 30 * time.Second
)

var (
	defaultNowFn = time.Now
)

type heartbeatOpts struct {
	enabled       bool
	nowFn         xclock.NowFn
	interval      time.Duration
	checkInterval time.Duration
	timeout       time.Duration
	router        HeartbeatRouter
}

// NewHeartbeatOptions returns the default HeartbeatOptions
func NewHeartbeatOptions() HeartbeatOptions {
	return &heartbeatOpts{
		enabled:       defaultEnabled,
		nowFn:         defaultNowFn,
		interval:      defaultHeartbeatInterval,
		checkInterval: defaultCheckInterval,
		timeout:       defaultHeartbeatTimeout,
	}
}

func (ho *heartbeatOpts) Validate() error {
	if ho.enabled && ho.router == nil {
		return fmt.Errorf("HeartbeatRouter not set")
	}
	return nil
}

func (ho *heartbeatOpts) SetEnabled(f bool) HeartbeatOptions {
	ho.enabled = f
	return ho
}

func (ho *heartbeatOpts) Enabled() bool {
	return ho.enabled
}

func (ho *heartbeatOpts) SetNowFn(fn xclock.NowFn) HeartbeatOptions {
	ho.nowFn = fn
	return ho
}

func (ho *heartbeatOpts) NowFn() xclock.NowFn {
	return ho.nowFn
}

func (ho *heartbeatOpts) SetInterval(d time.Duration) HeartbeatOptions {
	ho.interval = d
	return ho
}

func (ho *heartbeatOpts) Interval() time.Duration {
	return ho.interval
}

func (ho *heartbeatOpts) SetCheckInterval(td time.Duration) HeartbeatOptions {
	ho.checkInterval = td
	return ho
}

func (ho *heartbeatOpts) CheckInterval() time.Duration {
	return ho.checkInterval
}

func (ho *heartbeatOpts) SetTimeout(d time.Duration) HeartbeatOptions {
	ho.timeout = d
	return ho
}

func (ho *heartbeatOpts) Timeout() time.Duration {
	return ho.timeout
}

func (ho *heartbeatOpts) SetHeartbeatRouter(r HeartbeatRouter) HeartbeatOptions {
	ho.router = r
	return ho
}

func (ho *heartbeatOpts) HeartbeatRouter() HeartbeatRouter {
	return ho.router
}
