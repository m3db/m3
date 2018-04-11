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

package panicmon

import "os"

// ProcessStartEvent is passed to a ProcessStarted handler.
type ProcessStartEvent struct {
	Args []string
}

// ProcessFailedEvent is passed to a ProcessFailed handler.
type ProcessFailedEvent struct {
	Args []string
	Err  error
}

// ProcessExitedEvent is passed to a ProcessExited handler.
type ProcessExitedEvent struct {
	Args []string
	Code StatusCode
}

// SignalReceivedEvent is passed to a SignalReceived handler.
type SignalReceivedEvent struct {
	Signal   os.Signal
	ChildPid int
}

// SignalPassedEvent is passed to a SignalPassed handler.
type SignalPassedEvent struct {
	Signal   os.Signal
	ChildPid int
}

// SignalFailedEvent is passed to a SignalFailed handler.
type SignalFailedEvent struct {
	Signal   os.Signal
	ChildPid int
	Err      error
}
