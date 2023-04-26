// Copyright (c) 2022 Uber Technologies, Inc.
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

package dockerexternal

// EtcdClusterOption configure an etcd cluster.
type EtcdClusterOption interface {
	apply(opts *etcdClusterOptions)
}

type etcdClusterOptions struct {
	useBridge bool
	port      int
}

// EtcdClusterUseBridge configures an EtcdNode to insert a networking "bridge" between the etcd container and the
// calling processes. The bridge intercepts network traffic, and forwards it, unless told not to via e.g. Blackhole().
// See the bridge package.
// As noted in that package, this implementation is lifted directly from the etcd/integration package; all credit goes
// to etcd authors for the approach.
func EtcdClusterUseBridge(shouldUseBridge bool) EtcdClusterOption {
	return useBridge(shouldUseBridge)
}

type useBridge bool

func (u useBridge) apply(opts *etcdClusterOptions) {
	opts.useBridge = bool(u)
}

// EtcdClusterPort sets a specific port for etcd to listen on. Default is to listen on :0 (any free port).
func EtcdClusterPort(port int) EtcdClusterOption {
	return withPort(port)
}

type withPort int

func (w withPort) apply(opts *etcdClusterOptions) {
	opts.port = int(w)
}
