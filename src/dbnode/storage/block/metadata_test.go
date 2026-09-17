// Copyright (c) 2024 Uber Technologies, Inc.
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

package block

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("NewMetadata", func(t *testing.T) {
		// Create test data
		id := ident.StringID("test")
		tags := ident.NewTags(
			ident.StringTag("tag1", "value1"),
			ident.StringTag("tag2", "value2"),
		)
		start := xtime.Now()
		size := int64(1024)
		checksum := uint32(12345)
		lastRead := xtime.Now().Add(time.Hour)

		// Test with all fields
		md := NewMetadata(id, tags, start, size, &checksum, lastRead)
		require.Equal(t, id, md.ID)
		require.Equal(t, tags, md.Tags)
		require.Equal(t, start, md.Start)
		require.Equal(t, size, md.Size)
		require.Equal(t, &checksum, md.Checksum)
		require.Equal(t, lastRead, md.LastRead)

		// Test with nil checksum
		md = NewMetadata(id, tags, start, size, nil, lastRead)
		require.Equal(t, id, md.ID)
		require.Equal(t, tags, md.Tags)
		require.Equal(t, start, md.Start)
		require.Equal(t, size, md.Size)
		require.Nil(t, md.Checksum)
		require.Equal(t, lastRead, md.LastRead)

		// Test with zero values
		md = NewMetadata(nil, ident.Tags{}, 0, 0, nil, 0)
		require.Nil(t, md.ID)
		require.Equal(t, ident.Tags{}, md.Tags)
		require.Equal(t, xtime.UnixNano(0), md.Start)
		require.Equal(t, int64(0), md.Size)
		require.Nil(t, md.Checksum)
		require.Equal(t, xtime.UnixNano(0), md.LastRead)
	})

	t.Run("Finalize", func(t *testing.T) {
		// Create test data
		id := ident.StringID("test")
		tags := ident.NewTags(
			ident.StringTag("tag1", "value1"),
			ident.StringTag("tag2", "value2"),
		)
		start := xtime.Now()
		size := int64(1024)
		checksum := uint32(12345)
		lastRead := xtime.Now().Add(time.Hour)

		// Test with all fields
		md := NewMetadata(id, tags, start, size, &checksum, lastRead)
		md.Finalize()
		require.Nil(t, md.ID)
		require.Equal(t, ident.Tags{}, md.Tags)
		require.Equal(t, start, md.Start)
		require.Equal(t, size, md.Size)
		require.Equal(t, &checksum, md.Checksum)
		require.Equal(t, lastRead, md.LastRead)

		// Test with nil ID
		md = NewMetadata(nil, tags, start, size, &checksum, lastRead)
		md.Finalize()
		require.Nil(t, md.ID)
		require.Equal(t, ident.Tags{}, md.Tags)
		require.Equal(t, start, md.Start)
		require.Equal(t, size, md.Size)
		require.Equal(t, &checksum, md.Checksum)
		require.Equal(t, lastRead, md.LastRead)

		// Test with empty tags
		md = NewMetadata(id, ident.Tags{}, start, size, &checksum, lastRead)
		md.Finalize()
		require.Nil(t, md.ID)
		require.Equal(t, ident.Tags{}, md.Tags)
		require.Equal(t, start, md.Start)
		require.Equal(t, size, md.Size)
		require.Equal(t, &checksum, md.Checksum)
		require.Equal(t, lastRead, md.LastRead)

		// Test with all nil/empty values
		md = NewMetadata(nil, ident.Tags{}, 0, 0, nil, 0)
		md.Finalize()
		require.Nil(t, md.ID)
		require.Equal(t, ident.Tags{}, md.Tags)
		require.Equal(t, xtime.UnixNano(0), md.Start)
		require.Equal(t, int64(0), md.Size)
		require.Nil(t, md.Checksum)
		require.Equal(t, xtime.UnixNano(0), md.LastRead)
	})
}
