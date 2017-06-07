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

package agent

import (
	"fmt"
	"os"
	"time"

	"github.com/m3db/m3em/os/exec"

	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	// defaultHeartbeatTimeout is the default heartbeat timeout
	defaultHeartbeatTimeout = 2 * time.Minute

	// defaultNewFileMode is the file mode used for new files by default
	defaultNewFileMode = os.FileMode(0666)

	// defaultNewDirectoryMode is the file mode used for new directories by default
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)
)

var (
	defaultNoErrorFn = func() error {
		return nil
	}
)

type opts struct {
	iopts            instrument.Options
	workingDir       string
	execGenFn        ExecGenFn
	initFn           HostResourcesFn
	releaseFn        HostResourcesFn
	envMap           exec.EnvMap
	heartbeatTimeout time.Duration
	nowFn            xclock.NowFn
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
}

// NewOptions constructs new options
func NewOptions(io instrument.Options) Options {
	return &opts{
		iopts:            io,
		initFn:           defaultNoErrorFn,
		releaseFn:        defaultNoErrorFn,
		heartbeatTimeout: defaultHeartbeatTimeout,
		nowFn:            time.Now,
		newFileMode:      defaultNewFileMode,
		newDirectoryMode: defaultNewDirectoryMode,
	}
}

func (o *opts) Validate() error {
	if o.execGenFn == nil {
		return fmt.Errorf("ExecGenFn is not set")
	}

	if o.workingDir == "" {
		return fmt.Errorf("WorkingDirectory is not set")
	}

	return nil
}

func (o *opts) SetInstrumentOptions(io instrument.Options) Options {
	o.iopts = io
	return o
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o *opts) SetWorkingDirectory(wd string) Options {
	o.workingDir = wd
	return o
}

func (o *opts) WorkingDirectory() string {
	return o.workingDir
}

func (o *opts) SetExecGenFn(fn ExecGenFn) Options {
	o.execGenFn = fn
	return o
}

func (o *opts) ExecGenFn() ExecGenFn {
	return o.execGenFn
}

func (o *opts) SetInitHostResourcesFn(fn HostResourcesFn) Options {
	o.initFn = fn
	return o
}

func (o *opts) InitHostResourcesFn() HostResourcesFn {
	return o.initFn
}

func (o *opts) SetReleaseHostResourcesFn(fn HostResourcesFn) Options {
	o.releaseFn = fn
	return o
}

func (o *opts) ReleaseHostResourcesFn() HostResourcesFn {
	return o.releaseFn
}

func (o *opts) SetEnvMap(em exec.EnvMap) Options {
	o.envMap = em
	return o
}

func (o *opts) EnvMap() exec.EnvMap {
	return o.envMap
}

func (o *opts) SetHeartbeatTimeout(t time.Duration) Options {
	o.heartbeatTimeout = t
	return o
}

func (o *opts) HeartbeatTimeout() time.Duration {
	return o.heartbeatTimeout
}

func (o *opts) SetNowFn(fn xclock.NowFn) Options {
	o.nowFn = fn
	return o
}

func (o *opts) NowFn() xclock.NowFn {
	return o.nowFn
}

func (o *opts) SetNewFileMode(value os.FileMode) Options {
	o.newFileMode = value
	return o
}

func (o *opts) NewFileMode() os.FileMode {
	return o.newFileMode
}

func (o *opts) SetNewDirectoryMode(value os.FileMode) Options {
	o.newDirectoryMode = value
	return o
}

func (o *opts) NewDirectoryMode() os.FileMode {
	return o.newDirectoryMode
}
