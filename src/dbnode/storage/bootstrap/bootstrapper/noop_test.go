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

package bootstrapper

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"

	"github.com/stretchr/testify/require"
)

func TestNoOpNoneBootstrapperBootstrapProvider(t *testing.T) {
	testNoOpNoneBootstrapperBootstrapProvider(t, false)
}

func TestNoOpNoneBootstrapperBootstrapProviderWithIndex(t *testing.T) {
	testNoOpNoneBootstrapperBootstrapProvider(t, true)
}

func testNoOpNoneBootstrapperBootstrapProvider(t *testing.T, indexEnabled bool) {
	bs := NewNoOpNoneBootstrapperProvider()
	ranges := testShardTimeRanges()
	bootstrapper, err := bs.Provide()
	require.NoError(t, err)
	mds := namespace.MustBuildMetadatas(indexEnabled, "foo", "bar")
	opts := bootstrap.NewRunOptions()
	ns := bootstrap.BuildNamespacesTester(t, opts, ranges, mds...)
	defer ns.Finish()
	ns.TestBootstrapWith(bootstrapper)
	for _, md := range mds {
		ns.TestUnfulfilledForNamespace(md, ranges, ranges)
	}
}

func TestNoOpAllBootstrapperBootstrapProvider(t *testing.T) {
	testNoOpAllBootstrapperBootstrapProvider(t, false)
}

func TestNoOpAllBootstrapperBootstrapProviderWithIndex(t *testing.T) {
	testNoOpAllBootstrapperBootstrapProvider(t, true)
}

func testNoOpAllBootstrapperBootstrapProvider(t *testing.T, indexEnabled bool) {
	bs := NewNoOpAllBootstrapperProvider()
	ranges := testShardTimeRanges()
	bootstrapper, err := bs.Provide()
	require.NoError(t, err)
	mds := namespace.MustBuildMetadatas(indexEnabled, "foo", "bar")
	opts := bootstrap.NewRunOptions()
	ns := bootstrap.BuildNamespacesTester(t, opts, ranges, mds...)
	defer ns.Finish()
	ns.TestBootstrapWith(bootstrapper)
	for _, md := range mds {
		ns.TestUnfulfilledForNamespaceIsEmpty(md)
	}
}
