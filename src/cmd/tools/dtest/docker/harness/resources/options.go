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

package resources

type dockerImage struct {
	name string
	tag  string
}

type setupOptions struct {
	dbNodeImage      dockerImage
	coordinatorImage dockerImage
}

// SetupOptions is a setup option.
type SetupOptions func(*setupOptions)

// WithDBNodeImage sets an option to use an image name and tag for the DB node.
func WithDBNodeImage(name, tag string) SetupOptions {
	return func(o *setupOptions) {
		o.dbNodeImage = dockerImage{name: name, tag: tag}
	}
}

// WithCoordinatorImage sets an option to use an image name and tag for the coordinator.
func WithCoordinatorImage(name, tag string) SetupOptions {
	return func(o *setupOptions) {
		o.coordinatorImage = dockerImage{name: name, tag: tag}
	}
}
