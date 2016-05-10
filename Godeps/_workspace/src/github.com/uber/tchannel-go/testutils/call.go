// Copyright (c) 2015 Uber Technologies, Inc.

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

package testutils

import "github.com/uber/tchannel-go"

// This file contains test setup logic, and is named with a _test.go suffix to
// ensure it's only compiled with tests.

// FakeIncomingCall implements IncomingCall interface.
// Note: the F suffix for the fields is to clash with the method name.
type FakeIncomingCall struct {
	// CallerNameF is the calling service's name.
	CallerNameF string

	// ShardKeyF is the intended destination for this call.
	ShardKeyF string

	// RemotePeerF is the calling service's peer info.
	RemotePeerF tchannel.PeerInfo

	// RoutingDelegateF is the routing delegate.
	RoutingDelegateF string
}

// CallerName returns the caller name as specified in the fake call.
func (f *FakeIncomingCall) CallerName() string {
	return f.CallerNameF
}

// ShardKey returns the shard key as specified in the fake call.
func (f *FakeIncomingCall) ShardKey() string {
	return f.ShardKeyF
}

// RoutingDelegate returns the routing delegate as specified in the fake call.
func (f *FakeIncomingCall) RoutingDelegate() string {
	return f.RoutingDelegateF
}

// RemotePeer returns the caller's peer info.
func (f *FakeIncomingCall) RemotePeer() tchannel.PeerInfo {
	return f.RemotePeerF
}

// NewIncomingCall creates an incoming call for tests.
func NewIncomingCall(callerName string) tchannel.IncomingCall {
	return &FakeIncomingCall{CallerNameF: callerName}
}
