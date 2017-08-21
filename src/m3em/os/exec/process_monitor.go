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

package exec

import (
	"context"
	"fmt"
	"os"
	oexec "os/exec"
	"path"
	"path/filepath"
	"sync"

	xerrors "github.com/m3db/m3x/errors"
)

var (
	defaultStdoutSuffix = "out"
	defaultStderrSuffix = "err"
)

var (
	errUnableToStartClosed  = fmt.Errorf("unable to start: process monitor Closed()")
	errUnableToStartRunning = fmt.Errorf("unable to start: process already running")
	errUnableToStopStoped   = fmt.Errorf("unable to stop: process not running")
)

func (m EnvMap) toSlice() []string {
	if m == nil || len(m) == 0 {
		return nil
	}
	envVars := os.Environ()
	for k, v := range m {
		envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
	}
	return envVars
}

type processListener struct {
	completeFn func()
	errFn      func(error)
}

// NewProcessListener returns a new ProcessListener
func NewProcessListener(
	completeFn func(),
	errFn func(error),
) ProcessListener {
	return &processListener{
		completeFn: completeFn,
		errFn:      errFn,
	}
}

func (pl *processListener) OnComplete() {
	if fn := pl.completeFn; fn != nil {
		fn()
	}
}

func (pl *processListener) OnError(err error) {
	if fn := pl.errFn; fn != nil {
		fn(err)
	}
}

type startFn func()

type processMonitor struct {
	sync.Mutex
	cmd        *oexec.Cmd
	cancelFunc context.CancelFunc
	stdoutPath string
	stderrPath string
	stdoutFd   *os.File
	stderrFd   *os.File
	startFn    startFn
	listener   ProcessListener
	err        error
	running    bool
	done       bool
}

// NewProcessMonitor creates a new ProcessMonitor
func NewProcessMonitor(cmd Cmd, pl ProcessListener) (ProcessMonitor, error) {
	if err := cmd.Validate(); err != nil {
		return nil, err
	}

	fileInfo, err := os.Stat(cmd.OutputDir)
	if err != nil {
		return nil, fmt.Errorf("specified directory does not exist: %v", err)
	}

	if !fileInfo.IsDir() {
		return nil, fmt.Errorf("specified path is not a directory: %v", cmd.OutputDir)
	}

	base := filepath.Base(cmd.Path)
	stdoutPath := outputPath(cmd.OutputDir, base, defaultStdoutSuffix)
	stdoutWriter, err := newWriter(stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("unable to open stdout writer: %v", err)
	}

	stderrPath := outputPath(cmd.OutputDir, base, defaultStderrSuffix)
	stderrWriter, err := newWriter(stderrPath)
	if err != nil {
		stdoutWriter.Close()
		return nil, fmt.Errorf("unable to open stderr writer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ocmd := oexec.CommandContext(ctx, cmd.Path)
	ocmd.Args = cmd.Args
	ocmd.Dir = cmd.OutputDir
	ocmd.Stderr = stderrWriter
	ocmd.Stdout = stdoutWriter
	ocmd.Env = cmd.Env.toSlice()

	pm := &processMonitor{
		listener:   pl,
		stdoutFd:   stdoutWriter,
		stderrFd:   stderrWriter,
		stdoutPath: stdoutPath,
		stderrPath: stderrPath,
		cancelFunc: cancel,
		cmd:        ocmd,
	}
	pm.startFn = pm.startAsync

	return pm, nil
}

func (pm *processMonitor) notifyListener(err error) {
	if pm.listener == nil {
		return
	}
	if err == nil {
		pm.listener.OnComplete()
	} else {
		pm.listener.OnError(err)
	}
}

func (pm *processMonitor) Start() error {
	pm.Lock()
	if pm.done {
		pm.Unlock()
		return errUnableToStartClosed
	}

	if pm.err != nil {
		pm.Unlock()
		return pm.err
	}

	if pm.running {
		pm.Unlock()
		return errUnableToStartRunning
	}
	pm.Unlock()

	pm.startFn()
	return nil
}

func (pm *processMonitor) startAsync() {
	go pm.startSync()
}

func (pm *processMonitor) startSync() {
	pm.Lock()
	pm.running = true
	pm.Unlock()

	err := pm.cmd.Run()
	pm.Lock()
	defer pm.Unlock()

	// only notify when Stop() has not been called explicitly
	if done := pm.done; !done {
		pm.notifyListener(err)
		pm.err = err
	}
	pm.running = false
}

func (pm *processMonitor) Stop() error {
	pm.Lock()
	defer pm.Unlock()
	pm.done = true
	return pm.stopWithLock()
}

func (pm *processMonitor) stopWithLock() error {
	if pm.err != nil {
		return pm.err
	}

	if !pm.running {
		return errUnableToStopStoped
	}

	pm.cancelFunc()
	return nil
}

func (pm *processMonitor) Running() bool {
	pm.Lock()
	defer pm.Unlock()
	return pm.running
}

func (pm *processMonitor) Err() error {
	pm.Lock()
	defer pm.Unlock()
	return pm.err
}

func (pm *processMonitor) Close() error {
	pm.Lock()
	defer pm.Unlock()
	if pm.done {
		return nil
	}

	var multiErr xerrors.MultiError
	if pm.running {
		multiErr = multiErr.Add(pm.stopWithLock())
	}

	if pm.stderrFd != nil {
		multiErr = multiErr.Add(pm.stderrFd.Close())
		pm.stderrFd = nil
	}
	if pm.stdoutFd != nil {
		multiErr = multiErr.Add(pm.stdoutFd.Close())
		pm.stdoutFd = nil
	}
	pm.done = true
	return multiErr.FinalError()
}

func (pm *processMonitor) StdoutPath() string {
	return pm.stdoutPath
}

func (pm *processMonitor) StderrPath() string {
	return pm.stderrPath
}

func outputPath(outputDir string, prefix string, suffix string) string {
	return path.Join(outputDir, fmt.Sprintf("%s.%s", prefix, suffix))
}

func newWriter(outputPath string) (*os.File, error) {
	fd, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}
	return fd, nil
}
