// Copyright (c) 2020 Uber Technologies, Inc.
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

package checked

import (
	"errors"

	"go.uber.org/atomic"
)

var (
	errAlreadyFinalized = errors.New("double finalize")
	errNotFinalized     = errors.New("unfinalizing without prior finalize")
)

// FinalizeableOnce implements the logics needed to enforce a single finalize on pool.ObjectPool.
type FinalizeableOnce struct {
	finalized atomic.Bool
}

// SetFinalized marks the object as finalized and returns an error in case of double finalize.
func (c *FinalizeableOnce) SetFinalized() error {
	if oldValue := c.finalized.Swap(true); oldValue == true {
		return errAlreadyFinalized
	}
	return nil
}

// SetUnfinalized marks the object as not finalized and returns an error in case it was not finalize previously.
func (c *FinalizeableOnce) SetUnfinalized() error {
	if oldValue := c.finalized.Swap(false); oldValue == false {
		return errNotFinalized
	}
	return nil
}
