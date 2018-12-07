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
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
)

// StatusCode represents the exit code of a process.
type StatusCode int

// String returns the string representation of the status code.
func (r StatusCode) String() string {
	return strconv.Itoa(int(r))
}

// An Executor is responsible for executing a command and forwarding signals
// received by the parent to the spawned process. It reports process/signal
// events via the ProcessHandler/SignalHandler interfaces it is created with.
type Executor interface {
	// Run executes a command as defined by args.
	Run(args []string) (StatusCode, error)
}

// executor is the underlying implementation of Executor.
type executor struct {
	// channel on which os will send received signals
	sigC chan os.Signal
	// channel to shut down signal handler
	closeC chan struct{}

	handler Handler
	stdout  io.Writer
	stderr  io.Writer
	env     []string
}

// ExecutorOptions specifies options for creating a new executor.
type ExecutorOptions struct {
	// Handler for signal and process events.
	Handler Handler

	// If ListenSigs is non-empty than only the signals specified will be
	// listened to / forwarded. Default (empty) will cause all signals to be
	// listened to, and all signals except SIGCHLD will be forwarded to the
	// child.
	Signals []os.Signal

	// By default panicmon directs the child's stdout/stderr to its own in order
	// to seamlessly wrap the process. This behavior can be overridden by
	// specifying an io.Writer to send the child's stdout/stderr to. If either
	// is nil the parent's will be used. Output can be silenced by setting either
	// to ioutil.Discard.
	Stdout io.Writer
	Stderr io.Writer

	// By default panicmon uses the existing processes environment variables
	// when launching child processes. Set Env if you'd like to specify you're
	// own environment variables to spawn with, on how to use this option see
	// os/exec.Cmd.
	Env []string
}

// Validate ensures that an ExecutorOpts type is valid. Specifically, it will
// set ProcessHandler and SignalHandler to noop handlers if either of them are
// nil.
func (opts *ExecutorOptions) Validate() {
	if opts.Handler.ProcessHandler == nil {
		opts.Handler.ProcessHandler = NoopProcessHandler{}
	}

	if opts.Handler.SignalHandler == nil {
		opts.Handler.SignalHandler = NoopSignalHandler{}
	}
}

// NewDefaultExecutorOpts returns an ExecutorOpts that will listen to / forward
// all signals (except SIGCHLD) but will use noop handlers for signal/process
// events.
func NewDefaultExecutorOpts() ExecutorOptions {
	return ExecutorOptions{
		Handler: Handler{
			ProcessHandler: NoopProcessHandler{},
			SignalHandler:  NoopSignalHandler{},
		},
	}
}

// NewExecutor returns an executor configured according to opts (after calling
// opts.Validate()).
func NewExecutor(opts ExecutorOptions) Executor {
	opts.Validate()

	ex := &executor{
		sigC:    make(chan os.Signal, 2),
		closeC:  make(chan struct{}),
		handler: opts.Handler,
		stdout:  opts.Stdout,
		stderr:  opts.Stderr,
		env:     opts.Env,
	}

	signal.Notify(ex.sigC, opts.Signals...)

	return ex
}

func (ex *executor) Run(args []string) (StatusCode, error) {
	ex.handler.ProcessStarted(ProcessStartEvent{Args: args})

	result, err := ex.execCmd(args)
	if result.failedAtStartup && err != nil {
		ex.handler.ProcessFailed(ProcessFailedEvent{
			Args: args,
			Err:  err,
		})
		return result.statusCode, err
	}

	ex.handler.ProcessExited(ProcessExitedEvent{
		Args: args,
		Code: result.statusCode,
		Err:  err,
	})
	return result.statusCode, err
}

type execResult struct {
	failedAtStartup bool
	statusCode      StatusCode
}

// execCmd spawns a command according to args and passes any signals received
// by the parent process to the spawned process. It returns a bool indicating
// whether the process failed at startup or after starting, the status code of
// the failed process, and an error capturing further information.
func (ex *executor) execCmd(args []string) (execResult, error) {
	result := execResult{true, StatusCode(0)}
	if len(args) == 0 {
		return result, errors.New("args cannot be empty")
	}

	cmd := exec.Command(args[0], args[1:]...)

	if ex.stdout == nil {
		cmd.Stdout = os.Stdout
	} else {
		cmd.Stdout = ex.stdout
	}

	if ex.stderr == nil {
		cmd.Stderr = os.Stderr
	} else {
		cmd.Stderr = ex.stderr
	}

	if ex.env != nil {
		cmd.Env = ex.env
	}

	cmd.SysProcAttr = execSyscallAttr

	if err := cmd.Start(); err != nil {
		return result, err
	}

	// i.e. indicate we made it past process startup
	result.failedAtStartup = false

	go ex.passSignals(cmd.Process)
	defer ex.close()

	// if cmd.Wait returns a nil error it means the process exited with 0
	err := cmd.Wait()
	if err == nil {
		return result, nil
	}

	// if exited uncleanly, capture status code to report
	status := cmd.ProcessState.Sys().(syscall.WaitStatus)
	result.statusCode = StatusCode(status.ExitStatus())
	return result, err
}

func (ex *executor) close() {
	close(ex.closeC)
}

// passSignals forwards all signals (except SIGCHLD) received on sigC to the
// process running at proc.
func (ex *executor) passSignals(proc *os.Process) {
	for {
		select {
		case sig := <-ex.sigC:
			if sig.(syscall.Signal) == syscall.SIGCHLD {
				continue
			}

			ex.handler.SignalReceived(SignalReceivedEvent{
				Signal:   sig,
				ChildPid: proc.Pid,
			})

			if err := proc.Signal(sig); err != nil {
				ex.handler.SignalFailed(SignalFailedEvent{
					Signal:   sig,
					ChildPid: proc.Pid,
					Err:      err,
				})
				continue
			}
			ex.handler.SignalPassed(SignalPassedEvent{
				Signal:   sig,
				ChildPid: proc.Pid,
			})

		case <-ex.closeC:
			return
		}
	}
}
