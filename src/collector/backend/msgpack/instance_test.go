// Copyright (c) 2017 Uber Technologies, Inc.
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

package msgpack

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstance(t *testing.T) {
	require.Equal(t, "testInstanceID", testPlacementInstance.ID())
	require.Equal(t, "testInstanceAddress", testPlacementInstance.Address())
	require.Equal(t, "Instance<id=testInstanceID, address=testInstanceAddress>", testPlacementInstance.String())
}

func TestInstancesByIDAsc(t *testing.T) {
	instances := []instance{
		newInstance("foo", "foo_addr"),
		newInstance("baz", "baz_addr"),
		newInstance("bar", "bar_addr"),
	}
	expected := []instance{instances[2], instances[1], instances[0]}
	sort.Sort(instancesByIDAsc(instances))
	require.Equal(t, expected, instances)
}
