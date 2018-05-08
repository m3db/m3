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

package commitlog

import (
	"testing"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestBootstrapIndex(t *testing.T) {
	opts := testOptions()
	src := newCommitLogSource(opts, fs.Inspection{})

	md := testNsMetadata(t)
	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-2 * blockSize)

	foo := commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo")}
	bar := commitlog.Series{Namespace: testNamespaceID, Shard: 1, ID: ident.StringID("bar")}
	baz := commitlog.Series{Namespace: testNamespaceID, Shard: 2, ID: ident.StringID("baz")}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
	}

	_, err := src.ReadIndex(md, result.ShardTimeRanges{}, testDefaultRunOpts)
	require.NoError(t, err)
}
