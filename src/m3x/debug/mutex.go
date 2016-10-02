// Copyright (c) 2016 Uber Technologies, Inc.
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

package xdebug

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"
)

var (
	rwMutexStackBufferLength uint64 = 8192
)

// RWMutexStackBufferLength returns the length of the RWMutex stack buffer
func RWMutexStackBufferLength() int {
	return int(atomic.LoadUint64(&rwMutexStackBufferLength))
}

// SetRWMutexStackBufferLength sets the length of the RWMutex stack buffer
func SetRWMutexStackBufferLength(value int) {
	atomic.StoreUint64(&rwMutexStackBufferLength, uint64(value))
}

// RWMutex is a debug wrapper for sync.RWMutex
type RWMutex struct {
	Name   string
	Log    xlog.Logger
	Writer io.Writer

	mutex          sync.RWMutex
	pendingWriters int64
	writers        int64
	readers        int64
	stateMutex     sync.RWMutex
	stackBuf       []byte
	lastLockStack  []byte
}

// Lock the mutex for writing
func (m *RWMutex) Lock() {
	atomic.AddInt64(&m.pendingWriters, 1)
	m.mutex.Lock()
	atomic.AddInt64(&m.writers, 1)
	atomic.AddInt64(&m.pendingWriters, -1)

	stackBufLen := RWMutexStackBufferLength()

	m.stateMutex.Lock()
	if len(m.stackBuf) < stackBufLen {
		m.stackBuf = make([]byte, stackBufLen)
	}
	n := runtime.Stack(m.stackBuf, false)
	m.lastLockStack = m.stackBuf[:n]
	m.stateMutex.Unlock()
}

// Unlock the mutex for writing
func (m *RWMutex) Unlock() {
	m.mutex.Unlock()
	atomic.AddInt64(&m.writers, -1)
}

// RLock the mutex for reading
func (m *RWMutex) RLock() {
	atomic.AddInt64(&m.readers, 1)
	m.mutex.RLock()
}

// RUnlock the mutex for reading
func (m *RWMutex) RUnlock() {
	m.mutex.RUnlock()
	atomic.AddInt64(&m.readers, -1)
}

// RLocker returns a Locker interface that implements the Lock and Unlock methods by calling rw.RLock and rw.RUnlock.
func (m *RWMutex) RLocker() sync.Locker {
	return &rlocker{RWMutex: m}
}

// Report reports the state of the RWMutex
func (m *RWMutex) Report() {
	writers := atomic.LoadInt64(&m.writers)
	str := fmt.Sprintf("writers %d (pending %d) readers %d", writers, atomic.LoadInt64(&m.pendingWriters), atomic.LoadInt64(&m.readers))
	if writers > 0 {
		m.stateMutex.RLock()
		str += fmt.Sprintf(", stack: %s", string(m.lastLockStack))
		m.stateMutex.RUnlock()
	}
	m.println(str)
}

// ReportEvery will report the state of the RWMutex at a regular interval
func (m *RWMutex) ReportEvery(interval time.Duration) xclose.SimpleCloser {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			m.Report()
		}
	}()
	return &tickerCloser{Ticker: ticker}
}

func (m *RWMutex) println(str string) {
	name := m.Name
	if name == "" {
		name = "unnamed"
	}
	contents := fmt.Sprintf("debug.RWMutex[%s]: %s", name, str)
	if m.Log != nil {
		m.Log.Info(contents)
		return
	}
	if m.Writer != nil {
		m.Writer.Write([]byte(contents + "\n"))
		return
	}
	os.Stderr.WriteString(contents + "\n")
}

type rlocker struct {
	*RWMutex
}

func (m *rlocker) Lock() {
	m.RLock()
}

func (m *rlocker) Unlock() {
	m.RUnlock()
}

type tickerCloser struct {
	*time.Ticker
}

func (t *tickerCloser) Close() {
	t.Stop()
}
