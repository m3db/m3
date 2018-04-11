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

// Handler is composed of a ProcessHandler and SignalHandler.
type Handler struct {
	ProcessHandler
	SignalHandler
}

// ProcessHandler is implemented by clients that would like to subscribe to
// process-related events (start, exit, etc.).
type ProcessHandler interface {
	// ProcessStarted is called immediately prior to a process being started.
	ProcessStarted(ProcessStartEvent)

	// ProcessFailed is called when a process cannot successfully be started.
	// It receives the error returned by os/exec.
	ProcessFailed(ProcessFailedEvent)

	// ProcessExited is called when a process has been successfully started and
	// has ran to completion (regardless of exit status). It receives the status
	// code returned by the process.
	ProcessExited(ProcessExitedEvent)
}

// NoopProcessHandler implements ProcessHandler and does nothing.
type NoopProcessHandler struct{}

// ProcessStarted is a noop.
func (NoopProcessHandler) ProcessStarted(_ ProcessStartEvent) {}

// ProcessFailed is a noop.
func (NoopProcessHandler) ProcessFailed(_ ProcessFailedEvent) {}

// ProcessExited is a noop.
func (NoopProcessHandler) ProcessExited(_ ProcessExitedEvent) {}

// SignalHandler is implemented by clients that would like to subscribe to
// signal-related events, specifically when signals are received by panicmon
// and whether or not they are successfully passed to the child.
type SignalHandler interface {
	// SignalReceived is called immediately after a signal is intercepted by
	// panicmon.
	SignalReceived(SignalReceivedEvent)

	// SignalPassed is called when a signal has been successfully forwarded to a
	// child process.
	SignalPassed(SignalPassedEvent)

	// SignalFailed is called when panicmon encounters an error passing a signal
	// to a child.
	SignalFailed(SignalFailedEvent)
}

// NoopSignalHandler implements SignalHandler and does nothing.
type NoopSignalHandler struct{}

// SignalReceived is a noop.
func (NoopSignalHandler) SignalReceived(_ SignalReceivedEvent) {}

// SignalPassed is a noop.
func (NoopSignalHandler) SignalPassed(_ SignalPassedEvent) {}

// SignalFailed is a noop.
func (NoopSignalHandler) SignalFailed(_ SignalFailedEvent) {}
