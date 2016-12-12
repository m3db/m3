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

package client

import (
	"testing"

	_ "github.com/golang/mock/gomock"
)

func TestWriteToAvailableShard(t *testing.T) {

}

func TestWriteToInitializingShard(t *testing.T) {

}

func TestWriteToLeavingShard(t *testing.T) {

}

func TestWriteToInitializingAndLeavingShards(t *testing.T) {

}

func initShardTest(t *testing.T) {
	// ctrl := gomock.NewController(t)
	// defer ctrl.Finish()

	// session := newDefaultTestSession(t).(*session)

	// var completionFn completionFn
	// enqueueWg := mockHostQueues(ctrl, session, sessionTestReplicas, []testEnqueueFn{func(idx int, op op) {
	// 	completionFn = op.CompletionFn()
	// 	write, ok := op.(*writeOp)
	// }})
}
