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

package storage

import (
	"fmt"
	"sync"
)

type bootstrapState int

const (
	bootstrapNotStarted bootstrapState = iota
	bootstrapping
	bootstrapped
)

func analyze(bs bootstrapState, entity string) (bool, error) {
	switch bs {
	case bootstrapped:
		return false, nil
	case bootstrapping:
		return false, fmt.Errorf("%s is being bootstrapped", entity)
	default:
		return true, nil
	}
}

// tryBootstrap attempts to start the bootstrap process. It returns
// 1. (true, nil) if the attempt succeeds;
// 2. (false, nil) if the target has already bootstrapped;
// 3. (false, error) if the target is currently being bootstrapped.
func tryBootstrap(l *sync.RWMutex, s *bootstrapState, entity string) (bool, error) {
	l.RLock()
	bs := *s
	l.RUnlock()
	if success, err := analyze(bs, entity); !success {
		return success, err
	}

	l.Lock()
	// bootstrap state changed during RLock -> WLock promotion
	if success, err := analyze(bs, entity); !success {
		l.Unlock()
		return success, err
	}
	*s = bootstrapping
	l.Unlock()

	return true, nil
}
