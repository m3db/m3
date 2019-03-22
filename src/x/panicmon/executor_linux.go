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

// Package panicmon provides a method of spawning processes and monitoring
// its behavior via generic pluggable handlers.
//
// On linux we want to signal to the child process that when its parent dies
// it should die as well via receiving SIGKILL. This is because panicmon is
// set up to forward every signal, so if it dies we assume that means it was
// because it was sent SIGTERM, which it propagated to the child, which the
// child then did not process and shut down in time to prevent the supervisor
// (supervisord / systemd) from sending the parent a SIGKILL, which it cannot
// ignore and thus not pass to child, meaning that the child as well should
// exit as if it was passed SIGKILL.
package panicmon

import "syscall"

var (
	execSyscallAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
)
