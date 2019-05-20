// Copyright (c) 2018 Uber Technologies, Inc.
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

package remote

import (
	"google.golang.org/grpc/naming"
)

type staticResolver struct {
	updates []*naming.Update
}

func newStaticResolver(addresses []string) naming.Resolver {
	var updates []*naming.Update
	for _, address := range addresses {
		updates = append(updates, &naming.Update{
			Op:       naming.Add,
			Addr:     address,
			Metadata: nil,
		})
	}
	return &staticResolver{
		updates: updates,
	}
}

// Resolve creates a Watcher for target.
func (r *staticResolver) Resolve(target string) (naming.Watcher, error) {
	ch := make(chan []*naming.Update, 1)
	ch <- r.updates
	return &staticWatcher{
		updates: ch,
	}, nil
}

type staticWatcher struct {
	updates chan []*naming.Update
}

// Next returns the static address set
func (w *staticWatcher) Next() ([]*naming.Update, error) {
	return <-w.updates, nil
}

// Close closes the watcher
func (w *staticWatcher) Close() {
	close(w.updates)
}
