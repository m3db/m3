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

package xos

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"
)

// InterruptOptions are options to use when waiting for an interrupt.
type InterruptOptions struct {
	// InterruptChannel is an existing interrupt channel, if none
	// specified one will be created.
	InterruptCh <-chan error

	// InterruptedChannel is a channel that will be closed once an
	// interrupt has been seen. Use this to pass to goroutines who
	// want to be notified about interruptions that you don't want
	// consuming from the main interrupt channel.
	InterruptedCh chan struct{}
}

// InterruptError is an error representing an interrupt.
type InterruptError struct {
	interrupt string
}

// ErrInterrupted is an error indicating that the interrupted channel was closed,
// meaning an interrupt was received on the main interrupt channel.
var ErrInterrupted = NewInterruptError("interrupted")

// NewInterruptOptions creates InterruptOptions with sane defaults.
func NewInterruptOptions() InterruptOptions {
	return InterruptOptions{
		InterruptCh:   NewInterruptChannel(1),
		InterruptedCh: make(chan struct{}),
	}
}

// NewInterruptError creates a new InterruptError.
func NewInterruptError(interrupt string) error {
	return &InterruptError{interrupt: interrupt}
}

func (i *InterruptError) Error() string {
	return i.interrupt
}

// WatchForInterrupt watches for interrupts in a non-blocking fashion and closes
// the interrupted channel when an interrupt is found. Use this method to
// watch for interrupts while the caller continues to execute (e.g. during
// server startup). To ensure child goroutines get properly closed, pass them
// the interrupted channel. If the interrupted channel is closed, then the
// goroutine knows to stop its work. This method returns a function that
// can be used to stop the watch.
func WatchForInterrupt(logger *zap.Logger, opts InterruptOptions) func() {
	interruptCh := opts.InterruptCh
	closed := make(chan struct{})
	go func() {
		select {
		case err := <-interruptCh:
			logger.Warn("interrupt", zap.Error(err))
			close(opts.InterruptedCh)
		case <-closed:
			logger.Info("interrupt watch stopped")
			return
		}
	}()

	var doOnce sync.Once
	return func() {
		doOnce.Do(func() {
			close(closed)
		})
	}
}

// WaitForInterrupt will wait for an interrupt to occur and return when done.
func WaitForInterrupt(logger *zap.Logger, opts InterruptOptions) {
	// Handle interrupts.
	interruptCh := opts.InterruptCh
	if interruptCh == nil {
		// Need to catch our own interrupts.
		interruptCh = NewInterruptChannel(1)
		logger.Info("registered new interrupt handler")
	} else {
		logger.Info("using registered interrupt handler")
	}

	logger.Warn("interrupt", zap.Error(<-interruptCh))

	if opts.InterruptedCh != nil {
		close(opts.InterruptedCh)
	}
}

// NewInterruptChannel will return an interrupt channel useful with multiple
// listeners.
func NewInterruptChannel(numListeners int) <-chan error {
	interruptCh := make(chan error, numListeners)
	go func() {
		err := NewInterruptError(fmt.Sprintf("%v", <-interrupt()))
		for i := 0; i < numListeners; i++ {
			interruptCh <- err
		}
	}()
	return interruptCh
}

func interrupt() <-chan os.Signal {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return c
}
