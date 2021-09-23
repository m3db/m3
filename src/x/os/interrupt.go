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
	"syscall"

	"go.uber.org/zap"
)

// InterruptOptions are options to use when waiting for an interrupt.
type InterruptOptions struct {
	// InterruptChannel is an existing interrupt channel, if none
	// specified one will be created.
	InterruptCh <-chan error
}

// InterruptError is an error representing an interrupt.
type InterruptError struct {
	interrupt string
}

// NewInterruptError creates a new InterruptError.
func NewInterruptError(interrupt string) error {
	return &InterruptError{interrupt: interrupt}
}

func (i *InterruptError) Error() string {
	return i.interrupt
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
