// Copyright (c) 2021 Uber Technologies, Inc.
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

package permits

import "github.com/m3db/m3/src/x/context"

type noOpPermits struct {
}

var _ Manager = (*noOpPermits)(nil)

var _ Permits = (*noOpPermits)(nil)

// NewNoOpPermitsManager builds a new no-op permits manager.
func NewNoOpPermitsManager() Manager {
	return &noOpPermits{}
}

// NewNoOpPermits builds a new no-op permits.
func NewNoOpPermits() Permits {
	return &noOpPermits{}
}

func (p noOpPermits) NewPermits(context.Context) (Permits, error) {
	return p, nil
}

func (p noOpPermits) Acquire(context.Context) (*Permit, error) {
	return nil, nil
}

func (p noOpPermits) TryAcquire(context.Context) (*Permit, error) {
	return nil, nil
}

func (p noOpPermits) Release(_ *Permit) {
}
