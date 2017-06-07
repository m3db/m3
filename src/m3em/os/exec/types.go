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
	"fmt"
)

var (
	// ErrPathNotSet indicates a Cmd object which doesn't have a path value
	ErrPathNotSet = fmt.Errorf("cmd: no path specified")

	// ErrArgsRequiresPathAsFirstArg indicates a Cmd object which doesn't have
	// args[0] == path
	ErrArgsRequiresPathAsFirstArg = fmt.Errorf("cmd: args[0] un-equal to path")
)

// EnvMap is a map of Key-Value pairs representing the environment variables.
// NB(prateek): these are a set of 'delta' vars, i.e. they are appended to the
// vars already present in `os.Environ()`.
type EnvMap map[string]string

// Cmd represents an executable command
// NB(prateek): golang's os/exec package makes an implicit requirement that
// Args[0] to be set to Path, iff Args is set at all. This package follows the
// convention but makes that requirement explicit. ProcessMonitor reports
// error when being constructed.
type Cmd struct {
	Path      string
	Args      []string
	OutputDir string
	Env       EnvMap
}

// Validate ensures the provide Cmd object conforms to the expected structure.
func (c Cmd) Validate() error {
	if c.Path == "" {
		return ErrPathNotSet
	}
	if len(c.Args) > 0 && c.Path != c.Args[0] {
		return ErrArgsRequiresPathAsFirstArg
	}
	return nil
}

// ProcessListener permits users of the API to be notified of process termination
type ProcessListener interface {
	// OnComplete is executed in the event of process termination without error
	OnComplete()

	// OnError is executed in the event of process termination with error
	OnError(error)
}

// ProcessMonitor provides necessary controls to supervise a process
type ProcessMonitor interface {
	// Start starts the provided process
	Start() error

	// Stop stops any running process
	// NB(prateek): the golang runtime reports error inconsistently upon calling
	// `process.Kill()`. Consequently, it is not guaranteed which notification method
	// of the ProcessListener (OnComplete/OnError) is invoked upon process termination.
	// Refer https://github.com/golang/go/blob/master/src/os/exec_plan9.go#L51-L63
	Stop() error

	// Running returns a flag indicating if the process is running
	Running() bool

	// Err returns any error encountered
	Err() error

	// Close releases any held resources
	Close() error
}
