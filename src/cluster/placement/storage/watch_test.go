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

package storage

import (
	"testing"

	"github.com/m3db/m3cluster/generated/proto/kvtest"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"

	"github.com/stretchr/testify/require"
)

func TestPlacementWatch(t *testing.T) {
	m := mem.NewStore()
	ps := newTestPlacementStorage(m, placement.NewOptions()).(*storage)
	w, err := ps.Watch()
	require.NoError(t, err)
	require.Equal(t, 0, len(w.C()))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{}).
		SetShards([]uint32{}).
		SetReplicaFactor(0)
	err = ps.Set(p)
	require.NoError(t, err)
	<-w.C()
	p, err = w.Get()
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(1), p)

	err = ps.Set(p)
	require.NoError(t, err)
	<-w.C()
	p, err = w.Get()
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(2), p)

	err = ps.Delete()
	require.NoError(t, err)
	<-w.C()
	_, err = w.Get()
	require.Error(t, err)

	err = ps.SetIfNotExist(p)
	require.NoError(t, err)
	<-w.C()
	p, err = w.Get()
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(1), p)

	m.Set(ps.key, &kvtest.Foo{Msg: "foo"})
	require.NoError(t, err)
	<-w.C()
	_, err = w.Get()
	require.Error(t, err)
}
