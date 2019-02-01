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

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStatusCodeString(t *testing.T) {
	assert.Equal(t, StatusCode(1).String(), "1")
}

func TestNewDefaultExecutorOpts(t *testing.T) {
	exp := ExecutorOptions{
		Handler: Handler{
			ProcessHandler: NoopProcessHandler{},
			SignalHandler:  NoopSignalHandler{},
		},
	}
	assert.Equal(t, exp, NewDefaultExecutorOpts())
}

func TestExecutorOptsValidate(t *testing.T) {
	exp := ExecutorOptions{
		Handler: Handler{
			ProcessHandler: NoopProcessHandler{},
			SignalHandler:  NoopSignalHandler{},
		},
	}

	for _, opts := range []ExecutorOptions{
		{},
		{Handler: Handler{SignalHandler: NoopSignalHandler{}}},
		{Handler: Handler{ProcessHandler: NoopProcessHandler{}}},
	} {
		opts.Validate()
		assert.Equal(t, exp, opts)
	}
}

func TestNewExecutor(t *testing.T) {
	ex := NewExecutor(NewDefaultExecutorOpts())
	assert.NotNil(t, ex)
}

func TestExecutorRunError(t *testing.T) {
	proc := &mockProcessHandler{}
	proc.On("ProcessStarted", ProcessStartEvent{Args: nil}).Return()
	proc.On("ProcessFailed", mock.AnythingOfType("panicmon.ProcessFailedEvent")).Return()

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{ProcessHandler: proc},
	})

	_, err := ex.Run(nil)
	proc.AssertExpectations(t)
	assert.Error(t, err)
}

func TestExecutorRun_Exited_Success(t *testing.T) {
	args := []string{"ls"}
	proc := &mockProcessHandler{}
	proc.On("ProcessStarted", ProcessStartEvent{Args: args}).Return()
	proc.On("ProcessExited", ProcessExitedEvent{
		Args: args,
		Code: StatusCode(0),
	}).Return()

	sig := &mockSignalHandler{}

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{
			ProcessHandler: proc,
			SignalHandler:  sig,
		},
	})

	code, err := ex.Run(args)
	assert.Equal(t, StatusCode(0), code)
	assert.NoError(t, err)

	proc.AssertExpectations(t)
	sig.AssertExpectations(t)
}

func TestExecutorRun_Exited_Success_Output(t *testing.T) {
	args := []string{"echo", "hello"}
	proc := &mockProcessHandler{}
	proc.On("ProcessStarted", ProcessStartEvent{Args: args}).Return()
	proc.On("ProcessExited", ProcessExitedEvent{
		Args: args,
		Code: StatusCode(0),
	}).Return()

	sig := &mockSignalHandler{}

	buf := &bytes.Buffer{}

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{
			ProcessHandler: proc,
			SignalHandler:  sig,
		},
		Stdout: buf,
	})

	code, err := ex.Run(args)
	assert.Equal(t, StatusCode(0), code)
	assert.NoError(t, err)

	assert.Equal(t, "hello\n", buf.String())

	proc.AssertExpectations(t)
	sig.AssertExpectations(t)
}

func TestExecutorRun_Exited_Success_Signals(t *testing.T) {
	args := []string{"sleep", "10"}

	proc := &mockProcessHandler{}
	proc.On("ProcessStarted", ProcessStartEvent{Args: args}).Return()
	proc.On("ProcessExited", mock.MatchedBy(func(e ProcessExitedEvent) bool {
		return reflect.DeepEqual(e.Args, args) && e.Code == StatusCode(-1)
	})).Return()

	sig := &mockSignalHandler{}

	sig.On("SignalReceived", mock.MatchedBy(func(e SignalReceivedEvent) bool {
		return e.Signal == syscall.SIGUSR1
	})).Return()

	sig.On("SignalPassed", mock.MatchedBy(func(e SignalPassedEvent) bool {
		return e.Signal == syscall.SIGUSR1
	})).Return()

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{
			ProcessHandler: proc,
			SignalHandler:  sig,
		},
		Stdout: ioutil.Discard,
		Stderr: ioutil.Discard,
	})

	procInfo, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)

	codeC := make(chan StatusCode)
	errC := make(chan error)

	go func() {
		code, err := ex.Run(args)
		codeC <- code
		errC <- err
	}()

	go func() {
		procInfo.Signal(syscall.SIGUSR1)
	}()

	assert.Equal(t, StatusCode(-1), <-codeC)
	assert.Error(t, <-errC)

	proc.Lock()
	proc.AssertExpectations(t)
	proc.Unlock()

	sig.Lock()
	sig.AssertExpectations(t)
	sig.Unlock()

	_, ok := <-ex.(*executor).closeC
	assert.False(t, ok, "executor closeC should be closed")
}

func TestExecutorRun_Start_Error(t *testing.T) {
	args := []string{"non_existent_command_"}

	proc := &mockProcessHandler{}
	proc.On("ProcessStarted", ProcessStartEvent{Args: args}).Return()
	proc.On("ProcessFailed", mock.MatchedBy(func(e ProcessFailedEvent) bool {
		return reflect.DeepEqual(args, e.Args) && e.Err != nil
	}))

	sig := &mockSignalHandler{}

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{
			ProcessHandler: proc,
			SignalHandler:  sig,
		},
		Stdout: ioutil.Discard,
		Stderr: ioutil.Discard,
	})

	code, err := ex.Run(args)
	assert.Equal(t, StatusCode(0), code)
	assert.Error(t, err)

	proc.AssertExpectations(t)
	sig.AssertExpectations(t)
}

func TestExecutorRun_Exited_Error(t *testing.T) {
	args := []string{"ls", "/non/existent/path"}

	proc := &mockProcessHandler{}
	proc.On("ProcessStarted", ProcessStartEvent{Args: args}).Return()
	proc.On("ProcessExited", mock.MatchedBy(func(e ProcessExitedEvent) bool {
		return reflect.DeepEqual(args, e.Args) && e.Code != 0
	})).Return()

	sig := &mockSignalHandler{}

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{
			ProcessHandler: proc,
			SignalHandler:  sig,
		},
		Stdout: ioutil.Discard,
		Stderr: ioutil.Discard,
	})

	code, err := ex.Run(args)
	assert.NotEqual(t, StatusCode(0), code)
	assert.Error(t, err)

	proc.AssertExpectations(t)
	sig.AssertExpectations(t)
}

func TestExecutorPassSignals(t *testing.T) {
	sig := &mockSignalHandler{}

	ex := NewExecutor(ExecutorOptions{
		Handler: Handler{
			SignalHandler: sig,
		},
	}).(*executor)

	child := exec.Command("sleep", "10")
	err := child.Start()
	require.NoError(t, err)

	go ex.passSignals(child.Process)

	sig.On("SignalReceived", SignalReceivedEvent{
		Signal:   syscall.SIGUSR1,
		ChildPid: child.Process.Pid,
	})

	sig.On("SignalPassed", SignalPassedEvent{
		Signal:   syscall.SIGUSR1,
		ChildPid: child.Process.Pid,
	})

	procInfo, err := os.FindProcess(os.Getpid())
	assert.NoError(t, err)
	procInfo.Signal(syscall.SIGUSR1)

	child.Wait()
	ex.closeAndWait()

	sig.AssertExpectations(t)
}
